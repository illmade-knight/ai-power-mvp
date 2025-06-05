//go:build integration

package endtoend // A new package for end-to-end tests

import (
	"context"
	"errors"
	"fmt"
	messenger2 "load_test/apps/devicegen/messenger"
	loadtestverifier2 "load_test/apps/verifier/loadtestverifier"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	// Emulators and Cloud Clients
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	// Testcontainers
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	// --- Your Packages (IMPORTANT: Adjust these import paths!) ---
	// Assuming 'connectors' is the package containing IngestionService, EnrichMQTTData, message_types.go content
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/connectors"
)

const (
	// Test Run Config
	e2eTestRunID = "e2e-load-test-run-001"

	// Mosquitto
	e2eMosquittoImage   = "eclipse-mosquitto:2.0"
	e2eMqttBrokerPort   = "1883/tcp"
	e2eMqttInputTopic   = "e2e/devices/+/up"  // Ingestion service subscribes to this
	e2eMqttPublishTopic = "e2e/devices/%s/up" // Messenger publishes to this (DEVICE_EUI placeholder)

	// Pub/Sub Emulator
	e2ePubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	e2ePubSubEmulatorPort  = "8085/tcp"
	e2ePubSubProjectID     = "e2e-test-project"
	// Topics for Ingestion Service Output / Verifier Input
	e2ePubSubTopicIDEnriched       = "e2e-enriched-device-messages"
	e2ePubSubTopicIDUnidentified   = "e2e-unidentified-device-messages"
	e2ePubSubSubPrefixEnriched     = "e2e-sub-enriched-"     // Verifier will add UUID
	e2ePubSubSubPrefixUnidentified = "e2e-sub-unidentified-" // Verifier will add UUID

	// Firestore Emulator
	e2eFirestoreEmulatorImage     = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	e2eFirestoreEmulatorPort      = "8080/tcp"
	e2eFirestoreCollectionDevices = "e2e-devices-metadata"
)

// --- Testcontainer Setup Helpers (Adapted) ---

func setupMosquittoContainerE2E(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	confContent := []byte("allow_anonymous true\nlistener 1883\npersistence false\n")
	tempDir := t.TempDir()
	hostConfPath := filepath.Join(tempDir, "mosquitto.conf")
	err := os.WriteFile(hostConfPath, confContent, 0644)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        e2eMosquittoImage,
		ExposedPorts: []string{e2eMqttBrokerPort},
		WaitingFor:   wait.ForListeningPort(e2eMqttBrokerPort).WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: hostConfPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
		Cmd:          []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, e2eMqttBrokerPort)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("E2E Mosquitto container started: %s", brokerURL)
	return brokerURL, func() {
		require.NoError(t, container.Terminate(ctx))
		t.Log("E2E Mosquitto container terminated.")
	}
}

func setupPubSubEmulatorE2E(t *testing.T, ctx context.Context, projectID, enrichedTopicID, unidentifiedTopicID string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        e2ePubSubEmulatorImage,
		ExposedPorts: []string{e2ePubSubEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(e2ePubSubEmulatorPort, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, e2ePubSubEmulatorPort)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("E2E Pub/Sub emulator started: %s", emulatorHost)

	// Set env var for Pub/Sub client auto-detection if needed by any component
	originalPubSubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	adminClient, err := pubsub.NewClient(ctx, projectID, option.WithEndpoint(emulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer adminClient.Close()

	topicsToCreate := []string{enrichedTopicID, unidentifiedTopicID}
	for _, topicID := range topicsToCreate {
		topic := adminClient.Topic(topicID)
		exists, _ := topic.Exists(ctx)
		if !exists {
			_, err = adminClient.CreateTopic(ctx, topicID)
			require.NoError(t, err, "Failed to create topic %s", topicID)
			t.Logf("E2E Created Pub/Sub topic '%s'", topicID)
		} else {
			t.Logf("E2E Pub/Sub topic '%s' already exists", topicID)
		}
	}
	return emulatorHost, func() {
		if originalPubSubEmulatorHost == "" {
			os.Unsetenv("PUBSUB_EMULATOR_HOST")
		} else {
			os.Setenv("PUBSUB_EMULATOR_HOST", originalPubSubEmulatorHost)
		}
		require.NoError(t, container.Terminate(ctx))
		t.Log("E2E Pub/Sub emulator terminated.")
	}
}

func setupFirestoreEmulatorE2E(t *testing.T, ctx context.Context, projectID string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        e2eFirestoreEmulatorImage,
		ExposedPorts: []string{e2eFirestoreEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "firestore", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(e2eFirestoreEmulatorPort, "/")[0])},
		WaitingFor:   wait.ForLog("Dev App Server is now running").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, e2eFirestoreEmulatorPort)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("E2E Firestore emulator started: %s", emulatorHost)
	return emulatorHost, func() {
		require.NoError(t, container.Terminate(ctx))
		t.Log("E2E Firestore emulator terminated.")
	}
}

// --- Main E2E Test Function ---
func TestEndToEndLoadScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute) // Overall E2E test timeout
	defer cancel()

	baseLogger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// --- 1. Setup Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerE2E(t, ctx)
	defer mosquittoCleanup()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorE2E(t, ctx,
		e2ePubSubProjectID, e2ePubSubTopicIDEnriched, e2ePubSubTopicIDUnidentified,
	)
	defer pubsubEmulatorCleanup()

	firestoreEmulatorHost, firestoreEmulatorCleanup := setupFirestoreEmulatorE2E(t, ctx, e2ePubSubProjectID) // Use same project MessageID for simplicity
	defer firestoreEmulatorCleanup()

	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost)
	t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "")

	// --- 2. Configure and Run Messenger (Load Generator) ---
	loadGenLogger := baseLogger.With().Str("component", "Messenger").Logger()
	numKnownDevices := 2
	numUnknownDevicesTotal := 1 // Messenger will generate this many, but their EUIs won't be in Firestore
	totalMessengerDevices := numKnownDevices + numUnknownDevicesTotal
	msgRate := 1.0                     // messages per second per device
	loadGenDuration := 5 * time.Second // How long the messenger publishes for

	// Firestore client for messenger's DeviceGenerator
	fsClientMessenger, err := firestore.NewClient(ctx, e2ePubSubProjectID, // Project MessageID for Firestore
		option.WithEndpoint(firestoreEmulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCConnectionPool(1),
	)
	require.NoError(t, err, "Failed to create Firestore client for Messenger")
	defer fsClientMessenger.Close()

	// DeviceGenerator setup
	messengerDeviceGenCfg := &messenger2.DeviceGeneratorConfig{
		NumDevices:            totalMessengerDevices, // Will generate this many devices
		MsgRatePerDevice:      msgRate,
		FirestoreEmulatorHost: firestoreEmulatorHost, // For seeding
		FirestoreProjectID:    e2ePubSubProjectID,
		FirestoreCollection:   e2eFirestoreCollectionDevices,
	}
	messengerDeviceGen, err := messenger2.NewDeviceGenerator(messengerDeviceGenCfg, fsClientMessenger, loadGenLogger)
	require.NoError(t, err)
	messengerDeviceGen.CreateDevices() // Creates devices in memory

	// Manually adjust EUIs for known vs unknown for clarity in this test
	// The first `numKnownDevices` will be "known", the rest "unknown" by not seeding them
	// or by ensuring their metadata fetch fails in the ingestion service.
	// For this test, we'll seed only the "known" ones.
	var knownDeviceEUIs []string
	var allMessengerGeneratedDevices []*messenger2.Device // Keep track of all devices messenger will try to publish for

	for i := 0; i < totalMessengerDevices; i++ {
		// Use the EUI generated by messenger's CreateDevices
		eui := messengerDeviceGen.Devices[i].GetEUI() // Assuming Device has a GetEUI() method
		allMessengerGeneratedDevices = append(allMessengerGeneratedDevices, messengerDeviceGen.Devices[i])
		if i < numKnownDevices {
			knownDeviceEUIs = append(knownDeviceEUIs, eui)
			// Seed this known device into Firestore (simplified seeding for test)
			_, err = fsClientMessenger.Collection(e2eFirestoreCollectionDevices).Doc(eui).Set(ctx, map[string]string{
				"clientID":       fmt.Sprintf("E2EClient-%s", eui),
				"locationID":     "E2ELoc-01",
				"deviceCategory": "E2ESensor",
			})
			require.NoError(t, err, "Failed to seed known device %s", eui)
			t.Logf("E2E Seeded KNOWN device in Firestore: %s", eui)
		} else {
			t.Logf("E2E UNKNOWN device (will not be seeded, EUI for messenger to publish): %s", eui)
		}
	}
	// Note: messenger.DeviceGenerator.SeedDevices() would seed all its generated devices.
	// Here we are doing more targeted seeding for the E2E test logic.

	// Publisher setup
	messengerPublisher := messenger2.NewPublisher(
		fmt.Sprintf(e2eMqttPublishTopic, "%s"), // Placeholder for EUI
		mqttBrokerURL,
		"e2e-messenger-publisher",
		0, // QOS
		loadGenLogger,
		&messenger2.PahoMQTTClientFactory{},
	)

	// --- 3. Configure and Run Ingestion Microservice (In-Process) ---
	ingestionLogger := baseLogger.With().Str("component", "IngestionService").Logger()
	// Set environment variables or create a config struct for the ingestion service
	// This mimics how the ingestion service would get its configuration.
	// The example integration_test.go used os.Setenv.
	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", e2eMqttInputTopic) // Wildcard topic ingestion service listens to
	t.Setenv("MQTT_CLIENT_ID_PREFIX", "e2e-ingestion-")
	t.Setenv("GCP_PROJECT_ID", e2ePubSubProjectID)
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES", e2ePubSubTopicIDEnriched)
	t.Setenv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES", e2ePubSubTopicIDUnidentified)
	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost)
	t.Setenv("FIRESTORE_COLLECTION_DEVICES", e2eFirestoreCollectionDevices)
	// Ensure credentials files are empty if using emulators
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")
	t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "")

	// Initialize and run the ingestion service (adapted from your example integration_test.go)
	// This assumes your 'connectors' package has an entry point like InitializeAndGetCoreService
	// and the service has Start()/Stop() methods.
	ingestionServiceDeps, err := connectors.InitializeAndGetCoreService(ctx, ingestionLogger) // Pass E2E context
	require.NoError(t, err, "Failed to initialize ingestion service dependencies")
	require.NotNil(t, ingestionServiceDeps.Service, "Ingestion service instance is nil")

	ingestionService := ingestionServiceDeps.Service
	ingestionErrChan := make(chan error, 1)
	go func() {
		ingestionLogger.Info().Msg("Ingestion service starting...")
		ingestionErrChan <- ingestionService.Start()
		ingestionLogger.Info().Msg("Ingestion service Start() returned.")
	}()
	defer ingestionService.Stop() // Ensure graceful shutdown

	// Wait for ingestion service to be "ready" (e.g., connected to MQTT)
	// For a real service, you'd have a health check or a more robust readiness signal.
	// For this test, a short delay might suffice after Start() is called,
	// assuming Start() blocks until basic connections are up or returns quickly and connects async.
	time.Sleep(5 * time.Second) // Adjust as needed, or implement a better readiness check.
	select {
	case errStart := <-ingestionErrChan: // Check if Start() errored immediately
		if errStart != nil {
			t.Fatalf("Ingestion service failed to start: %v", errStart)
		}
	default:
		ingestionLogger.Info().Msg("Ingestion service assumed to be running.")
	}

	// Start messenger publisher in a goroutine
	messengerCtx, messengerCancel := context.WithTimeout(ctx, loadGenDuration+2*time.Second) // Context for messenger
	defer messengerCancel()
	var messengerWg sync.WaitGroup
	messengerWg.Add(1)
	go func() {
		defer messengerWg.Done()
		loadGenLogger.Info().Msg("Messenger publisher starting...")
		// Publish for all devices it generated (known and unknown EUIs)
		errPub := messengerPublisher.Publish(allMessengerGeneratedDevices, loadGenDuration, messengerCtx)
		if errPub != nil && !errors.Is(errPub, context.Canceled) && !errors.Is(errPub, context.DeadlineExceeded) {
			loadGenLogger.Error().Err(errPub).Msg("Messenger publisher error")
		}
		loadGenLogger.Info().Msg("Messenger publisher finished.")
	}()

	// --- 4. Configure and Run Verifier ---
	verifierLogger := baseLogger.With().Str("component", "Verifier").Logger()
	verifierConfig := loadtestverifier2.VerifierConfig{
		ProjectID:           e2ePubSubProjectID,
		EnrichedTopicID:     e2ePubSubTopicIDEnriched,
		UnidentifiedTopicID: e2ePubSubTopicIDUnidentified,
		TestDuration:        loadGenDuration + 20*time.Second, // Run verifier longer than load generator
		OutputFile:          filepath.Join(t.TempDir(), "e2e_verifier_results.json"),
		TestRunID:           e2eTestRunID,
		EmulatorHost:        pubsubEmulatorHost,
		LogLevel:            "debug",
	}
	verifier, err := loadtestverifier2.NewVerifier(verifierConfig, verifierLogger)
	require.NoError(t, err)

	var verifierResults *loadtestverifier2.TestRunResults
	var verifierErr error
	var verifierWg sync.WaitGroup
	verifierWg.Add(1)
	go func() {
		defer verifierWg.Done()
		verifierLogger.Info().Msg("Verifier starting...")
		verifierResults, verifierErr = verifier.Run(ctx) // Use the main E2E context
		verifierLogger.Info().Msg("Verifier finished.")
	}()

	// --- 5. Wait for Load Generation and Processing ---
	loadGenLogger.Info().Msg("Waiting for messenger publisher to complete...")
	messengerWg.Wait() // Wait for messenger to finish its publishing duration
	loadGenLogger.Info().Msg("Messenger publisher completed its run.")

	// Wait a bit longer for ingestion and verifier to process messages
	ingestionLogger.Info().Msg("Allowing extra time for ingestion and verification processing...")
	time.Sleep(8 * time.Second) // Grace period

	// --- 6. Stop Verifier (by canceling its context if not already done by duration) and Ingestion ---
	// Verifier will stop when its TestDuration is up or its context (overall E2E ctx) is cancelled.
	// We can explicitly wait for the verifier goroutine.
	verifierLogger.Info().Msg("Waiting for verifier to complete...")
	verifierWg.Wait()
	require.NoError(t, verifierErr, "Verifier.Run reported an error")
	require.NotNil(t, verifierResults, "Verifier did not produce results")

	ingestionLogger.Info().Msg("Stopping ingestion service...")
	ingestionService.Stop() // Call Stop on the ingestion service
	// Check if the ingestion service goroutine exited cleanly
	select {
	case errStop := <-ingestionErrChan:
		if errStop != nil && !errors.Is(errStop, context.Canceled) { // context.Canceled is expected on graceful stop
			t.Logf("Ingestion service Start() goroutine exited with error: %v", errStop)
		}
	case <-time.After(5 * time.Second):
		t.Logf("Timeout waiting for ingestion service Start() goroutine to exit after Stop()")
	}
	ingestionLogger.Info().Msg("Ingestion service stopped.")

	// --- 7. Assert Results ---
	require.NotNil(t, verifierResults, "Verifier results are nil")
	t.Logf("Verifier Results: Total=%d, Enriched=%d, Unidentified=%d",
		verifierResults.TotalMessagesVerified,
		verifierResults.EnrichedMessages,
		verifierResults.UnidentifiedMessages)

	// Expected messages: numDevices * msgRate * loadGenDuration
	// This is approximate.
	expectedMessagesPerKnownDeviceMin := int(msgRate*loadGenDuration.Seconds()) - 1 // Allow for one missed tick
	if expectedMessagesPerKnownDeviceMin < 0 {
		expectedMessagesPerKnownDeviceMin = 0
	}
	expectedMessagesPerUnknownDeviceMin := int(msgRate*loadGenDuration.Seconds()) - 1
	if expectedMessagesPerUnknownDeviceMin < 0 {
		expectedMessagesPerUnknownDeviceMin = 0
	}

	assert.GreaterOrEqual(t, verifierResults.EnrichedMessages, numKnownDevices*expectedMessagesPerKnownDeviceMin, "Not enough enriched messages")
	assert.GreaterOrEqual(t, verifierResults.UnidentifiedMessages, numUnknownDevicesTotal*expectedMessagesPerUnknownDeviceMin, "Not enough unidentified messages")

	// Further detailed checks on specific EUIs in results:
	foundKnownEUIs := make(map[string]int)
	foundUnknownEUIs := make(map[string]int)

	for _, record := range verifierResults.Records {
		if record.TopicSource == "enriched" {
			foundKnownEUIs[record.DeviceEUI]++
		} else if record.TopicSource == "unidentified" {
			foundUnknownEUIs[record.DeviceEUI]++
		}
	}

	for _, eui := range knownDeviceEUIs {
		assert.GreaterOrEqual(t, foundKnownEUIs[eui], expectedMessagesPerKnownDeviceMin, "Known EUI %s missing sufficient enriched messages", eui)
	}

	// Check that unknown devices (those published by messenger but not seeded) ended up in unidentified
	for i := numKnownDevices; i < totalMessengerDevices; i++ {
		unknownEUI := allMessengerGeneratedDevices[i].GetEUI()
		assert.GreaterOrEqual(t, foundUnknownEUIs[unknownEUI], expectedMessagesPerUnknownDeviceMin, "Unknown EUI %s missing sufficient unidentified messages", unknownEUI)
		// Also ensure known EUIs are NOT in the unidentified list
		assert.Zero(t, foundUnknownEUIs[knownDeviceEUIs[0]], "Known EUI %s should not be in unidentified messages", knownDeviceEUIs[0])
	}

	t.Log("E2E load test scenario completed.")
}

// Helper method for messenger.Device if it doesn't exist
// (This is an assumption based on the test logic needing the EUI)
// If your messenger.Device already has a GetEUI() or similar, you don't need this.
// Add this to your messenger package or adapt the test if EUI is accessed differently.
/*
func (d *messenger.Device) GetEUI() string {
	// This is a placeholder. Access the EUI field of your messenger.Device struct.
	// For example, if it's an unexported field `eui`, you might need a getter.
	// If d.eui is exported, then just use d.eui directly in the test.
	// This depends on your actual messenger.Device struct definition.
	// return d.eui // If 'eui' is an exported field or you add a getter
	panic("Device.GetEUI() not implemented or 'eui' field not accessible for test")
}
*/
