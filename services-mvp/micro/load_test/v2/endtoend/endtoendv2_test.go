//go:build integration

package endtoend_test

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/enrich/connectors"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/mqtttopubsub/converter"
	// Messenger and Verifier - assuming these are from a shared test library
	"load_test/apps/devicegen/messenger"
	"load_test/apps/verifier/loadtestverifier"
)

// --- Test Constants ---
const (
	// Test Run Config
	e2eTestRunID = "e2e-two-service-run-001"

	// --- Service 1: MQTT to Pub/Sub Converter ---
	// Mosquitto (Input to Service 1)
	e2eMosquittoImage   = "eclipse-mosquitto:2.0"
	e2eMqttBrokerPort   = "1883/tcp"
	e2eMqttInputTopic   = "e2e/devices/+/up"  // Converter service subscribes to this
	e2eMqttPublishTopic = "e2e/devices/%s/up" // Messenger publishes to this

	// Pub/Sub (Output of Service 1 / Input to Service 2)
	e2ePubSubTopicIDRaw        = "e2e-raw-mqtt-messages"
	e2ePubSubSubscriptionIDRaw = "e2e-sub-raw-for-enrichment"

	// --- Service 2: Enrichment Service ---
	// Firestore (Data source for Service 2)
	e2eFirestoreEmulatorImage     = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	e2eFirestoreEmulatorPort      = "8080/tcp"
	e2eFirestoreCollectionDevices = "e2e-devices-metadata"

	// Pub/Sub (Output of Service 2 / Input for Verifier)
	e2ePubSubTopicIDEnriched     = "e2e-enriched-device-messages"
	e2ePubSubTopicIDUnidentified = "e2e-unidentified-device-messages"

	// --- General ---
	e2ePubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	e2ePubSubEmulatorPort  = "8085/tcp"
	e2ePubSubProjectID     = "e2e-test-project"
)

// --- Testcontainer Setup Helpers ---

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

func setupPubSubEmulatorE2E(t *testing.T, ctx context.Context, projectID string, topicsToCreate map[string]string) (emulatorHost string, cleanupFunc func()) {
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

	originalPubSubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	adminClient, err := pubsub.NewClient(ctx, projectID, option.WithEndpoint(emulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer adminClient.Close()

	for topicID, subID := range topicsToCreate {
		topic := adminClient.Topic(topicID)
		exists, _ := topic.Exists(ctx)
		if !exists {
			_, err = adminClient.CreateTopic(ctx, topicID)
			require.NoError(t, err, "Failed to create topic %s", topicID)
			t.Logf("E2E Created Pub/Sub topic '%s'", topicID)
		}
		if subID != "" {
			sub := adminClient.Subscription(subID)
			exists, _ := sub.Exists(ctx)
			if !exists {
				_, err = adminClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic, AckDeadline: 20 * time.Second})
				require.NoError(t, err, "Failed to create subscription %s", subID)
				t.Logf("E2E Created Pub/Sub subscription '%s'", subID)
			}
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
func TestEndToEndTwoServiceScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute) // Overall E2E test timeout
	defer cancel()

	baseLogger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	// --- 1. Setup Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerE2E(t, ctx)
	defer mosquittoCleanup()

	pubsubTopicsToCreate := map[string]string{
		e2ePubSubTopicIDRaw:          e2ePubSubSubscriptionIDRaw,
		e2ePubSubTopicIDEnriched:     "", // Verifier will create its own sub
		e2ePubSubTopicIDUnidentified: "", // Verifier will create its own sub
	}
	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorE2E(t, ctx, e2ePubSubProjectID, pubsubTopicsToCreate)
	defer pubsubEmulatorCleanup()

	firestoreEmulatorHost, firestoreEmulatorCleanup := setupFirestoreEmulatorE2E(t, ctx, e2ePubSubProjectID)
	defer firestoreEmulatorCleanup()

	// Set env vars for components that auto-discover them
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost)

	// --- 2. Configure and Run Service 1: MQTT->PubSub Converter ---
	converterLogger := baseLogger.With().Str("component", "ConverterService").Logger()
	converterPubCfg := &converter.GooglePubSubPublisherConfig{
		ProjectID: e2ePubSubProjectID,
		TopicID:   e2ePubSubTopicIDRaw,
	}
	converterPublisher, err := converter.NewGooglePubSubPublisher(ctx, converterPubCfg, converterLogger)
	require.NoError(t, err)
	defer converterPublisher.Stop()

	// Create and start the converter service
	mqttCfg := &converter.MQTTClientConfig{
		BrokerURL:        mqttBrokerURL,
		Topic:            e2eMqttInputTopic,
		ClientIDPrefix:   "e2e-converter-",
		KeepAlive:        60 * time.Second,
		ConnectTimeout:   10 * time.Second,
		ReconnectWaitMax: 1 * time.Minute,
	}
	converterService := converter.NewIngestionService(converterPublisher, converterLogger, converter.DefaultIngestionServiceConfig(), mqttCfg)

	converterErrChan := make(chan error, 1)
	go func() {
		converterLogger.Info().Msg("Converter service starting...")
		if err := converterService.Start(); err != nil {
			converterErrChan <- err
		}
	}()
	defer converterService.Stop()

	// --- 3. Configure and Run Service 2: Enrichment Service ---
	enrichmentLogger := baseLogger.With().Str("component", "EnrichmentService").Logger()

	// Manually set up EnrichmentService dependencies
	enrichedPubCfg := &connectors.GooglePubSubPublisherConfig{ProjectID: e2ePubSubProjectID, TopicID: e2ePubSubTopicIDEnriched}
	enrichedPublisher, err := connectors.NewGooglePubSubPublisher(ctx, enrichedPubCfg, enrichmentLogger.With().Str("publisher", "enriched").Logger())
	require.NoError(t, err)
	defer enrichedPublisher.Stop()

	unidentifiedPubCfg := &connectors.GooglePubSubPublisherConfig{ProjectID: e2ePubSubProjectID, TopicID: e2ePubSubTopicIDUnidentified}
	unidentifiedPublisher, err := connectors.NewGooglePubSubPublisher(ctx, unidentifiedPubCfg, enrichmentLogger.With().Str("publisher", "unidentified").Logger())
	require.NoError(t, err)
	defer unidentifiedPublisher.Stop()

	firestoreFetcherCfg := &connectors.FirestoreFetcherConfig{ProjectID: e2ePubSubProjectID, CollectionName: e2eFirestoreCollectionDevices}
	metadataFetcher, err := connectors.NewGoogleDeviceMetadataFetcher(ctx, firestoreFetcherCfg, enrichmentLogger.With().Str("fetcher", "firestore").Logger())
	require.NoError(t, err)
	defer metadataFetcher.Close()

	enrichmentServiceConfig := connectors.DefaultEnrichmentServiceConfig()
	inputSubCfg := connectors.PubSubSubscriptionConfig{ProjectID: e2ePubSubProjectID, SubscriptionID: e2ePubSubSubscriptionIDRaw}
	enrichmentService, err := connectors.NewEnrichmentService(ctx, enrichmentServiceConfig, inputSubCfg, metadataFetcher.Fetch, enrichedPublisher, unidentifiedPublisher, enrichmentLogger)
	require.NoError(t, err)

	enrichmentErrChan := make(chan error, 1)
	go func() {
		enrichmentLogger.Info().Msg("Enrichment service starting...")
		if err := enrichmentService.Start(); err != nil {
			enrichmentErrChan <- err
		}
	}()
	defer enrichmentService.Stop()

	// Wait for services to be ready
	t.Log("Waiting for services to start up...")
	time.Sleep(10 * time.Second) // Give services time to connect
	select {
	case err := <-converterErrChan:
		t.Fatalf("Converter service failed to start: %v", err)
	case err := <-enrichmentErrChan:
		t.Fatalf("Enrichment service failed to start: %v", err)
	default:
		t.Log("Services assumed to be running.")
	}

	// --- 4. Seed Firestore with Known Device Data ---
	fsClientForSeeding, err := firestore.NewClient(ctx, e2ePubSubProjectID, option.WithEndpoint(firestoreEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer fsClientForSeeding.Close()

	// --- 5. Configure and Run Messenger (Load Generator) ---
	messengerLogger := baseLogger.With().Str("component", "Messenger").Logger()
	numKnownDevices := 3
	numUnknownDevicesTotal := 2
	totalMessengerDevices := numKnownDevices + numUnknownDevicesTotal
	msgRate := 2.0 // messages per second per device
	loadGenDuration := 8 * time.Second

	messengerDeviceGenCfg := &messenger.DeviceGeneratorConfig{
		NumDevices:            totalMessengerDevices,
		MsgRatePerDevice:      msgRate,
		FirestoreEmulatorHost: firestoreEmulatorHost,
		FirestoreProjectID:    e2ePubSubProjectID,
		FirestoreCollection:   e2eFirestoreCollectionDevices,
	}
	fsClientForMessenger, err := firestore.NewClient(ctx, e2ePubSubProjectID, option.WithEndpoint(firestoreEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer fsClientForMessenger.Close()

	messengerDeviceGen, err := messenger.NewDeviceGenerator(messengerDeviceGenCfg, fsClientForMessenger, messengerLogger)
	require.NoError(t, err)
	messengerDeviceGen.CreateDevices()

	var knownDeviceEUIs []string
	var allDeviceEUIs []string
	for i, dev := range messengerDeviceGen.Devices {
		eui := dev.GetEUI()
		allDeviceEUIs = append(allDeviceEUIs, eui)
		if i < numKnownDevices {
			knownDeviceEUIs = append(knownDeviceEUIs, eui)
			_, err = fsClientForSeeding.Collection(e2eFirestoreCollectionDevices).Doc(eui).Set(ctx, map[string]string{
				"clientID":       fmt.Sprintf("E2EClient-%s", eui),
				"locationID":     "E2ELoc-XYZ",
				"deviceCategory": "E2E-Temp-Sensor",
			})
			require.NoError(t, err, "Failed to seed known device %s", eui)
		}
	}
	t.Logf("Seeded %d known devices in Firestore.", len(knownDeviceEUIs))

	pahoFactory := &messenger.PahoMQTTClientFactory{}
	messengerPublisher := messenger.NewPublisher(
		e2eMqttPublishTopic,
		mqttBrokerURL,
		"e2e-messenger-publisher",
		1, // QOS
		messengerLogger,
		pahoFactory,
	)

	messengerCtx, messengerCancel := context.WithTimeout(ctx, loadGenDuration+5*time.Second)
	defer messengerCancel()
	var messengerWg sync.WaitGroup
	messengerWg.Add(1)
	go func() {
		defer messengerWg.Done()
		messengerLogger.Info().Msg("Messenger starting...")
		errPub := messengerPublisher.Publish(messengerDeviceGen.Devices, loadGenDuration, messengerCtx)
		if errPub != nil && !errors.Is(errPub, context.Canceled) && !errors.Is(errPub, context.DeadlineExceeded) {
			messengerLogger.Error().Err(errPub).Msg("Messenger publisher error")
		}
		messengerLogger.Info().Msg("Messenger finished.")
	}()

	// --- 6. Configure and Run Verifier ---
	verifierLogger := baseLogger.With().Str("component", "Verifier").Logger()
	verifierConfig := loadtestverifier.VerifierConfig{
		ProjectID:           e2ePubSubProjectID,
		EnrichedTopicID:     e2ePubSubTopicIDEnriched,
		UnidentifiedTopicID: e2ePubSubTopicIDUnidentified,
		TestDuration:        loadGenDuration + 25*time.Second,
		OutputFile:          filepath.Join(t.TempDir(), "e2e_verifier_results.json"),
		TestRunID:           e2eTestRunID,
		EmulatorHost:        pubsubEmulatorHost,
	}
	verifier, err := loadtestverifier.NewVerifier(verifierConfig, verifierLogger)
	require.NoError(t, err)

	var verifierResults *loadtestverifier.TestRunResults
	var verifierErr error
	var verifierWg sync.WaitGroup
	verifierWg.Add(1)
	go func() {
		defer verifierWg.Done()
		verifierLogger.Info().Msg("Verifier starting...")
		verifierResults, verifierErr = verifier.Run(ctx)
		verifierLogger.Info().Msg("Verifier finished.")
	}()

	// --- 7. Wait and Verify ---
	t.Log("Waiting for messenger to complete...")
	messengerWg.Wait()
	t.Log("Messenger completed. Waiting for verifier to complete...")
	verifierWg.Wait()
	require.NoError(t, verifierErr, "Verifier.Run reported an error")
	require.NotNil(t, verifierResults, "Verifier did not produce results")

	t.Logf("Verifier Results: Total=%d, Enriched=%d, Unidentified=%d",
		verifierResults.TotalMessagesVerified,
		verifierResults.EnrichedMessages,
		verifierResults.UnidentifiedMessages)

	// --- 8. Assert Results ---
	expectedMessagesPerDeviceMin := int(msgRate*loadGenDuration.Seconds()) - 2 // Allow for timing tolerance
	if expectedMessagesPerDeviceMin < 1 {
		expectedMessagesPerDeviceMin = 1
	}

	expectedEnrichedMin := numKnownDevices * expectedMessagesPerDeviceMin
	expectedUnidentifiedMin := numUnknownDevicesTotal * expectedMessagesPerDeviceMin

	assert.GreaterOrEqual(t, verifierResults.EnrichedMessages, expectedEnrichedMin, "Not enough enriched messages")
	assert.GreaterOrEqual(t, verifierResults.UnidentifiedMessages, expectedUnidentifiedMin, "Not enough unidentified messages")

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
		assert.GreaterOrEqual(t, foundKnownEUIs[eui], expectedMessagesPerDeviceMin, "Known EUI %s missing sufficient enriched messages", eui)
		assert.Zero(t, foundUnknownEUIs[eui], "Known EUI %s should not appear in unidentified messages", eui)
	}

	unknownCount := 0
	for _, eui := range allDeviceEUIs {
		isKnown := false
		for _, k := range knownDeviceEUIs {
			if eui == k {
				isKnown = true
				break
			}
		}
		if !isKnown {
			unknownCount++
			assert.GreaterOrEqual(t, foundUnknownEUIs[eui], expectedMessagesPerDeviceMin, "Unknown EUI %s missing sufficient unidentified messages", eui)
			assert.Zero(t, foundKnownEUIs[eui], "Unknown EUI %s should not appear in enriched messages", eui)
		}
	}
	assert.Equal(t, numUnknownDevicesTotal, unknownCount, "Mismatch in number of unknown devices checked")

	t.Log("E2E two-service test scenario completed successfully.")
}

// NOTE: This test assumes that your `messenger.Device` struct has an exported `GetEUI()` method
// or an exported `EUI` field to retrieve the device identifier, e.g.:
//
// func (d *Device) GetEUI() string { return d.EUI }
//
// This is necessary for the test to correctly seed data and perform assertions.
