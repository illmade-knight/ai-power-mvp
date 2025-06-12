//go:build integration

package endtoendcloud_test

import (
	"context"
	"errors"
	"fmt"
	"load_test/apps/devicegen/messenger"
	"load_test/apps/verifier/loadtestverifier"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	// GCP Clients
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Testcontainers
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	// --- Your Packages (IMPORTANT: Adjust these import paths!) ---
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/connectors"
)

const (
	// --- Cloud Test Configuration ---
	// !!! REPLACE THESE WITH YOUR ACTUAL GCP PROJECT AND DESIRED NAMES !!!
	cloudTestGCPProjectID = "gemini-power-test" // ****** MUST BE SET ******
	cloudTestRunPrefix    = "e2ecloud"          // Prefix for temporary resources

	// MQTT Configuration
	cloudTestMosquittoImage = "eclipse-mosquitto:2.0" // Using testcontainers for MQTT
	cloudTestMqttBrokerPort = "1883/tcp"
	cloudMqttInputTopic     = "e2ecloud/devices/+/up"
	cloudMqttPublishPattern = "e2ecloud/devices/%s/up"

	// Pub/Sub Topic Names (will be created and deleted if possible)
	cloudPubSubTopicIDEnriched     = cloudTestRunPrefix + "-enriched-msgs"
	cloudPubSubTopicIDUnidentified = cloudTestRunPrefix + "-unidentified-msgs"

	// Firestore Collection Name
	cloudFirestoreCollectionDevices = cloudTestRunPrefix + "-devices"
)

// Helper to get environment variables or use defaults (useful for CI/configurable tests)
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// --- Testcontainer Setup Helper for Mosquitto (similar to non-cloud E2E) ---
func setupMosquittoContainerCloudE2E(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	confContent := []byte("allow_anonymous true\nlistener 1883\npersistence false\n")
	tempDir := t.TempDir()
	hostConfPath := filepath.Join(tempDir, "mosquitto.conf")
	err := os.WriteFile(hostConfPath, confContent, 0644)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        cloudTestMosquittoImage,
		ExposedPorts: []string{cloudTestMqttBrokerPort},
		WaitingFor:   wait.ForListeningPort(cloudTestMqttBrokerPort).WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: hostConfPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
		Cmd:          []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Mosquitto container for cloud E2E")
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, cloudTestMqttBrokerPort)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("CLOUD E2E: Mosquitto container started: %s", brokerURL)
	return brokerURL, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("CLOUD E2E: Warning - Failed to terminate Mosquitto container: %v", err)
		} else {
			t.Log("CLOUD E2E: Mosquitto container terminated.")
		}
	}
}

// --- Cloud Resource Setup and Teardown Helpers ---

// setupRealPubSubTopics creates Pub/Sub topics in your GCP project.
func setupRealPubSubTopics(t *testing.T, ctx context.Context, projectID, enrichedTopicID, unidentifiedTopicID string) (cleanupFunc func()) {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID) // Uses ADC or GOOGLE_APPLICATION_CREDENTIALS
	require.NoError(t, err, "Failed to create real Pub/Sub client")

	topicsToManage := map[string]**pubsub.Topic{
		enrichedTopicID:     new(*pubsub.Topic),
		unidentifiedTopicID: new(*pubsub.Topic),
	}

	for id, topicPtr := range topicsToManage {
		topic := client.Topic(id)
		existsCtx, cancelExists := context.WithTimeout(ctx, 30*time.Second)
		exists, errExists := topic.Exists(existsCtx)
		cancelExists()
		require.NoError(t, errExists, "Failed to check existence of topic %s", id)

		if !exists {
			createCtx, cancelCreate := context.WithTimeout(ctx, 30*time.Second)
			*topicPtr, err = client.CreateTopic(createCtx, id)
			cancelCreate()
			require.NoError(t, err, "Failed to create Pub/Sub topic %s", id)
			t.Logf("CLOUD E2E: Created Pub/Sub topic '%s'", id)
		} else {
			*topicPtr = topic
			t.Logf("CLOUD E2E: Pub/Sub topic '%s' already exists, using it.", id)
		}
	}

	return func() {
		t.Logf("CLOUD E2E: Starting Pub/Sub topic cleanup...")
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Longer timeout for cleanup
		defer cancel()
		for id, topicPtr := range topicsToManage {
			if *topicPtr != nil {
				deleteCtx, deleteCancel := context.WithTimeout(cleanupCtx, 30*time.Second)
				errDel := (*topicPtr).Delete(deleteCtx)
				deleteCancel()
				if errDel != nil {
					// Log as warning because topic might be cleaned up by other means or have active subs
					t.Logf("CLOUD E2E: Warning - failed to delete Pub/Sub topic '%s': %v", id, errDel)
				} else {
					t.Logf("CLOUD E2E: Deleted Pub/Sub topic '%s'", id)
				}
			}
		}
		client.Close()
		t.Logf("CLOUD E2E: Pub/Sub client closed.")
	}
}

// --- Main Cloud E2E Test Function ---
func TestEndToEndCloudLoadScenario(t *testing.T) {
	if cloudTestGCPProjectID == "your-gcp-project-id-for-testing" || cloudTestGCPProjectID == "" {
		t.Skip("Skipping cloud integration test: GCP Project MessageID is not configured (cloudTestGCPProjectID).")
		return
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		// Attempt to create a client. If this fails, ADC is likely not configured.
		_, errAdc := firestore.NewClient(adcCheckCtx, cloudTestGCPProjectID)
		if errAdc != nil {
			t.Logf("ADC Firestore client creation failed: %v", errAdc)
			t.Skip("Skipping cloud integration test: GOOGLE_APPLICATION_CREDENTIALS not set and ADC might not be available.")
			return
		}
		t.Log("ADC seems available, proceeding with cloud test.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // Longer overall timeout for cloud tests
	defer cancel()

	baseLogger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.InfoLevel).With().Timestamp().Logger()

	// --- 1. Setup Emulated MQTT Broker and Real Cloud Resources (Pub/Sub Topics) ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerCloudE2E(t, ctx) // Using Mosquitto container
	defer mosquittoCleanup()

	pubsubCleanup := setupRealPubSubTopics(t, ctx, cloudTestGCPProjectID, cloudPubSubTopicIDEnriched, cloudPubSubTopicIDUnidentified)
	defer pubsubCleanup() // Firestore client is created and closed within device seeding

	// --- Test Parameters ---
	numKnownDevicesCloud := 20
	numUnknownDevicesCloud := 5
	totalDevicesToGenerate := numKnownDevicesCloud + numUnknownDevicesCloud
	msgRateCloud := .6
	loadGenDurationCloud := 30 * time.Second // Duration for messenger to publish

	// --- 2. Device Generation and Firestore Seeding (using messenger.DeviceGenerator into REAL Firestore) ---
	messengerSetupLogger := baseLogger.With().Str("component", "MessengerSetup").Logger()

	// Firestore client for DeviceGenerator to seed known devices into REAL Firestore
	firestoreClientForSeeding, err := firestore.NewClient(ctx, cloudTestGCPProjectID)
	require.NoError(t, err, "Failed to create real Firestore client for seeding")
	defer firestoreClientForSeeding.Close()

	allDevicesConfig := &messenger.DeviceGeneratorConfig{
		NumDevices:          totalDevicesToGenerate,
		MsgRatePerDevice:    msgRateCloud,
		FirestoreProjectID:  cloudTestGCPProjectID,
		FirestoreCollection: cloudFirestoreCollectionDevices,
		// FirestoreEmulatorHost is NOT set, so it uses the real client passed to NewDeviceGenerator
	}
	allDeviceGenerator, err := messenger.NewDeviceGenerator(allDevicesConfig, firestoreClientForSeeding, messengerSetupLogger)
	require.NoError(t, err)
	allDeviceGenerator.CreateDevices()

	var knownDevicesToSeedAndPublish []*messenger.Device
	var unknownDevicesToPublish []*messenger.Device
	var allEUIsForCleanup []string

	for i, device := range allDeviceGenerator.Devices {
		// ASSUMPTION: messenger.Device has a GetEUI() method or an exported Eui field.
		eui := fmt.Sprintf("LOADTEST-%06d", i) // Assuming this pattern from messenger.NewDevice
		// If messenger.Device.eui is exported: eui = device.eui
		// Or if a getter exists: eui = device.GetEUI()
		allEUIsForCleanup = append(allEUIsForCleanup, eui)

		if i < numKnownDevicesCloud {
			knownDevicesToSeedAndPublish = append(knownDevicesToSeedAndPublish, device)
		} else {
			unknownDevicesToPublish = append(unknownDevicesToPublish, device)
		}
	}

	if len(knownDevicesToSeedAndPublish) > 0 {
		messengerSetupLogger.Info().Int("count", len(knownDevicesToSeedAndPublish)).Msg("Seeding KNOWN devices into REAL Firestore...")
		tempKnownDeviceGeneratorCfg := &messenger.DeviceGeneratorConfig{
			NumDevices:          len(knownDevicesToSeedAndPublish),
			FirestoreProjectID:  cloudTestGCPProjectID,
			FirestoreCollection: cloudFirestoreCollectionDevices,
		}
		tempKnownDeviceGenerator, err := messenger.NewDeviceGenerator(tempKnownDeviceGeneratorCfg, firestoreClientForSeeding, messengerSetupLogger)
		require.NoError(t, err)
		tempKnownDeviceGenerator.Devices = knownDevicesToSeedAndPublish // Crucial: use the subset of devices

		seedCtx, seedCancel := context.WithTimeout(ctx, time.Duration(numKnownDevicesCloud*3)*time.Second+15*time.Second) // Generous timeout for cloud seeding
		defer seedCancel()
		err = tempKnownDeviceGenerator.SeedDevices(seedCtx)
		require.NoError(t, err, "Failed to seed KNOWN devices into REAL Firestore")
		messengerSetupLogger.Info().Msg("KNOWN devices seeded successfully into REAL Firestore.")
	}

	// temp pause after device seeding - remove if unnecessary
	time.Sleep(time.Second * 2)

	allDevicesToPublish := append(knownDevicesToSeedAndPublish, unknownDevicesToPublish...)

	defer func() {
		t.Logf("CLOUD E2E: Starting Firestore document cleanup for collection '%s'...", cloudFirestoreCollectionDevices)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute) // Longer for cloud
		defer cancel()
		fsCleanupClient, err := firestore.NewClient(cleanupCtx, cloudTestGCPProjectID)
		if err != nil {
			t.Logf("CLOUD E2E: Warning - failed to create Firestore client for cleanup: %v", err)
			return
		}
		defer fsCleanupClient.Close()
		var deleteWg sync.WaitGroup
		deletedCount := 0
		errorCount := 0
		for _, eui := range allEUIsForCleanup {
			deleteWg.Add(1)
			go func(deviceEUI string) {
				defer deleteWg.Done()
				docRef := fsCleanupClient.Collection(cloudFirestoreCollectionDevices).Doc(deviceEUI)
				delCtx, delCancel := context.WithTimeout(cleanupCtx, 20*time.Second)
				_, errDel := docRef.Delete(delCtx)
				delCancel()
				if errDel != nil {
					if strings.Contains(errDel.Error(), "NotFound") {
						baseLogger.Debug().Str("eui", deviceEUI).Msg("Device not found during cleanup.")
					} else {
						t.Logf("CLOUD E2E: Warning - failed to delete Firestore document '%s': %v", deviceEUI, errDel)
						errorCount++
					}
				} else {
					baseLogger.Info().Str("eui", deviceEUI).Msg("CLOUD E2E: Deleted Firestore document")
					deletedCount++
				}
			}(eui)
		}
		deleteWg.Wait()
		t.Logf("CLOUD E2E: Firestore document cleanup finished. Deleted: %d, Errors: %d", deletedCount, errorCount)
	}()

	// --- 3. Configure and START Ingestion Microservice (In-Process) ---
	ingestionLogger := baseLogger.With().Str("component", "IngestionService").Logger()
	// Ensure EMULATOR HOSTS are NOT set for ingestion service to use real cloud services
	os.Unsetenv("FIRESTORE_EMULATOR_HOST")
	os.Unsetenv("PUBSUB_EMULATOR_HOST")

	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL) // Use the Mosquitto container URL
	t.Setenv("MQTT_TOPIC", cloudMqttInputTopic)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", cloudTestRunPrefix+"-ingestion-")
	t.Setenv("GCP_PROJECT_ID", cloudTestGCPProjectID) // Real Project MessageID
	t.Setenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES", cloudPubSubTopicIDEnriched)
	t.Setenv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES", cloudPubSubTopicIDUnidentified)
	t.Setenv("FIRESTORE_COLLECTION_DEVICES", cloudFirestoreCollectionDevices)
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")
		t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "")
	}

	ingestionServiceDeps, err := connectors.InitializeAndGetCoreService(ctx, ingestionLogger)
	require.NoError(t, err, "Failed to initialize ingestion service dependencies for cloud test")
	require.NotNil(t, ingestionServiceDeps.Service, "Ingestion service instance is nil for cloud test")

	ingestionService := ingestionServiceDeps.Service
	ingestionErrChan := make(chan error, 1)
	var ingestionServiceWg sync.WaitGroup
	ingestionServiceWg.Add(1)
	go func() {
		defer ingestionServiceWg.Done()
		ingestionLogger.Info().Msg("CLOUD E2E: Ingestion service starting...")
		errStart := ingestionService.Start()
		if errStart != nil && !errors.Is(errStart, context.Canceled) {
			ingestionLogger.Error().Err(errStart).Msg("CLOUD E2E: Ingestion service Start() returned an error")
			select {
			case ingestionErrChan <- errStart:
			default:
			}
		} else if errStart != nil {
			ingestionLogger.Info().Err(errStart).Msg("CLOUD E2E: Ingestion service Start() returned (likely context canceled).")
		} else {
			ingestionLogger.Info().Msg("CLOUD E2E: Ingestion service Start() returned (nil error).")
		}
		close(ingestionErrChan)
	}()

	ingestionLogger.Info().Msg("CLOUD E2E: Waiting for ingestion service to become ready (connect to MQTT, Firestore, Pub/Sub)...")
	time.Sleep(25 * time.Second) // Longer readiness for cloud services + MQTT container
	select {
	case errStart, ok := <-ingestionErrChan:
		if ok && errStart != nil {
			t.Fatalf("CLOUD E2E: Ingestion service failed to start promptly: %v", errStart)
		}
	default:
		ingestionLogger.Info().Msg("CLOUD E2E: Ingestion service assumed to be ready.")
	}

	// --- 4. Configure and Run Messenger Publisher ---
	messengerLogger := baseLogger.With().Str("component", "Messenger").Logger()
	publisher := messenger.NewPublisher(
		cloudMqttPublishPattern, // To format with EUI
		mqttBrokerURL,           // Use the Mosquitto container URL
		cloudTestRunPrefix+"-messenger-pub",
		0, // QOS
		messengerLogger,
		&messenger.PahoMQTTClientFactory{},
	)

	messengerCtx, messengerCancel := context.WithTimeout(ctx, loadGenDurationCloud+10*time.Second)
	defer messengerCancel()
	var messengerWg sync.WaitGroup
	messengerWg.Add(1)
	go func() {
		defer messengerWg.Done()
		messengerLogger.Info().Dur("duration", loadGenDurationCloud).Msg("CLOUD E2E: Messenger publisher starting messages...")
		errPub := publisher.Publish(allDevicesToPublish, loadGenDurationCloud, messengerCtx)
		if errPub != nil && !errors.Is(errPub, context.Canceled) && !errors.Is(errPub, context.DeadlineExceeded) {
			messengerLogger.Error().Err(errPub).Msg("CLOUD E2E: Messenger publisher error")
		}
		messengerLogger.Info().Msg("CLOUD E2E: Messenger publisher finished sending messages.")
	}()

	// --- 5. Configure and Run Verifier ---
	verifierLogger := baseLogger.With().Str("component", "Verifier").Logger()
	verifierConfig := loadtestverifier.VerifierConfig{
		ProjectID:           cloudTestGCPProjectID, // Real Project MessageID
		EnrichedTopicID:     cloudPubSubTopicIDEnriched,
		UnidentifiedTopicID: cloudPubSubTopicIDUnidentified,
		TestDuration:        loadGenDurationCloud + 60*time.Second, // Verifier runs significantly longer for cloud
		OutputFile:          filepath.Join(t.TempDir(), fmt.Sprintf("e2e_cloud_results_%s.json", uuid.NewString())),
		TestRunID:           cloudTestRunPrefix + "-" + uuid.NewString(),
		LogLevel:            "info",
		// NO EmulatorHost for verifier, it uses real Pub/Sub
	}
	verifier, err := loadtestverifier.NewVerifier(verifierConfig, verifierLogger)
	require.NoError(t, err)

	var verifierResults *loadtestverifier.TestRunResults
	var verifierErr error
	var verifierWg sync.WaitGroup
	verifierWg.Add(1)
	go func() {
		defer verifierWg.Done()
		verifierLogger.Info().Msg("CLOUD E2E: Verifier starting...")
		verifierResults, verifierErr = verifier.Run(ctx)
		verifierLogger.Info().Msg("CLOUD E2E: Verifier finished.")
	}()

	// --- 6. Wait for Load Generation ---
	messengerLogger.Info().Msg("CLOUD E2E: Waiting for messenger publisher to complete...")
	messengerWg.Wait()
	messengerLogger.Info().Msg("CLOUD E2E: Messenger publisher completed its run.")

	// --- 7. Wait for Verifier ---
	verifierLogger.Info().Msg("CLOUD E2E: Waiting for verifier to complete...")
	verifierWg.Wait()
	if verifierErr != nil {
		t.Errorf("CLOUD E2E: Verifier.Run reported an error: %v", verifierErr)
	}

	// --- 8. Stop Ingestion Service ---
	ingestionLogger.Info().Msg("CLOUD E2E: Signaling ingestion service to stop...")
	ingestionService.Stop()
	ingestionLogger.Info().Msg("CLOUD E2E: Waiting for ingestion service to fully stop...")
	ingestionServiceWg.Wait()
	if errIngestExit, isOpen := <-ingestionErrChan; isOpen && errIngestExit != nil {
		t.Errorf("CLOUD E2E: Ingestion service exited with error: %v", errIngestExit)
	} else if !isOpen && errIngestExit != nil {
		t.Errorf("CLOUD E2E: Ingestion service exited with error (channel closed): %v", errIngestExit)
	}
	ingestionLogger.Info().Msg("CLOUD E2E: Ingestion service stopped.")

	// --- 9. Assert Results ---
	require.NotNil(t, verifierResults, "CLOUD E2E: Verifier results are nil. This implies a failure in the verifier's run.")
	t.Logf("CLOUD E2E: Final Verifier Results: Total=%d, Enriched=%d, Unidentified=%d. Output: %s",
		verifierResults.TotalMessagesVerified,
		verifierResults.EnrichedMessages,
		verifierResults.UnidentifiedMessages,
		verifierConfig.OutputFile)

	messagesPerDeviceExpected := int(msgRateCloud * loadGenDurationCloud.Seconds())
	// For cloud tests, latency can be higher, allow for more message variance/loss.
	minMessagesPerDevice := int(float64(messagesPerDeviceExpected) * 0.6) // Allow 40% loss/timing variance
	if messagesPerDeviceExpected > 0 && minMessagesPerDevice == 0 {       // Ensure at least 1 if expected > 0
		minMessagesPerDevice = 1
	}

	expectedEnrichedMin := numKnownDevicesCloud * minMessagesPerDevice
	expectedUnidentifiedMin := numUnknownDevicesCloud * minMessagesPerDevice

	assert.GreaterOrEqualf(t, verifierResults.EnrichedMessages, expectedEnrichedMin,
		"CLOUD E2E: Not enough enriched messages. Expected at least ~%d (target %d), got %d",
		expectedEnrichedMin, numKnownDevicesCloud*messagesPerDeviceExpected, verifierResults.EnrichedMessages)
	assert.GreaterOrEqualf(t, verifierResults.UnidentifiedMessages, expectedUnidentifiedMin,
		"CLOUD E2E: Not enough unidentified messages. Expected at least ~%d (target %d), got %d",
		expectedUnidentifiedMin, numUnknownDevicesCloud*messagesPerDeviceExpected, verifierResults.UnidentifiedMessages)

	if verifierResults.EnrichedMessages >= expectedEnrichedMin && verifierResults.UnidentifiedMessages >= expectedUnidentifiedMin {
		assert.Equal(t, verifierResults.EnrichedMessages+verifierResults.UnidentifiedMessages, verifierResults.TotalMessagesVerified,
			"CLOUD E2E: Total verified should be sum of enriched and unidentified")
	}

	t.Log("CLOUD E2E: Test scenario completed.")
}
