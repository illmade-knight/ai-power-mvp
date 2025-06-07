//go:build integration

package endtoend_test

import (
	"context"
	"errors"
	"fmt"
	endtoend "load_test/v2/endtoendcloud"
	"os"
	"path/filepath"
	//"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	// Emulators and Cloud Clients
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	//"google.golang.org/api/option"

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

// --- Cloud Test Constants ---
const (
	// --- Cloud Test Configuration ---
	cloudTestGCPProjectID = "gemini-power-test" // ****** MUST BE SET ******
	cloudTestRunPrefix    = "e2e-cloud"         // Prefix for all temporary cloud resources

	// MQTT Configuration (using a local container)
	cloudTestMosquittoImage = "eclipse-mosquitto:2.0"
	cloudTestMqttBrokerPort = "1883/tcp"
	cloudMqttInputTopic     = "e2ecloud/devices/+/up"
	cloudMqttPublishPattern = "e2ecloud/devices/%s/up"
)

// --- Testcontainer Setup Helper for Mosquitto ---
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
	require.NoError(t, err, "Failed to start Mosquitto container")
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, cloudTestMqttBrokerPort)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("CLOUD E2E: Mosquitto container started: %s", brokerURL)
	return brokerURL, func() {
		require.NoError(t, container.Terminate(ctx))
		t.Log("CLOUD E2E: Mosquitto container terminated.")
	}
}

// --- Real Cloud Resource Setup/Teardown ---
func setupRealPubSubResources(t *testing.T, ctx context.Context, projectID string, topicIDs []string, subConfig map[string]string) (cleanupFunc func()) {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real Pub/Sub client")

	// Create topics
	for _, topicID := range topicIDs {
		topic := client.Topic(topicID)
		exists, errExists := topic.Exists(ctx)
		require.NoError(t, errExists, "Failed to check existence of topic %s", topicID)
		if !exists {
			_, err = client.CreateTopic(ctx, topicID)
			require.NoError(t, err, "Failed to create Pub/Sub topic %s", topicID)
			t.Logf("CLOUD E2E: Created Pub/Sub topic '%s'", topicID)
		} else {
			t.Logf("CLOUD E2E: Pub/Sub topic '%s' already exists, using it.", topicID)
		}
	}

	// Create subscriptions
	for subID, topicID := range subConfig {
		sub := client.Subscription(subID)
		exists, errExists := sub.Exists(ctx)
		require.NoError(t, errExists, "Failed to check existence of subscription %s", subID)
		if !exists {
			_, err = client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
				Topic:       client.Topic(topicID),
				AckDeadline: 20 * time.Second,
			})
			require.NoError(t, err, "Failed to create subscription %s for topic %s", subID, topicID)
			t.Logf("CLOUD E2E: Created Pub/Sub subscription '%s'", subID)
		} else {
			t.Logf("CLOUD E2E: Pub/Sub subscription '%s' already exists, using it.", subID)
		}
	}

	// Return a cleanup function
	return func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		t.Logf("CLOUD E2E: Starting Pub/Sub resource cleanup...")
		// Delete subscriptions first
		for subID := range subConfig {
			sub := client.Subscription(subID)
			if err := sub.Delete(cleanupCtx); err != nil {
				t.Logf("CLOUD E2E: Warning - failed to delete subscription '%s': %v", subID, err)
			} else {
				t.Logf("CLOUD E2E: Deleted subscription '%s'", subID)
			}
		}
		// Then delete topics
		for _, topicID := range topicIDs {
			topic := client.Topic(topicID)
			if err := topic.Delete(cleanupCtx); err != nil {
				t.Logf("CLOUD E2E: Warning - failed to delete topic '%s': %v", topicID, err)
			} else {
				t.Logf("CLOUD E2E: Deleted topic '%s'", topicID)
			}
		}
		client.Close()
	}
}

func cleanupFirestoreCollection(t *testing.T, ctx context.Context, projectID, collectionID string) {
	t.Helper()
	t.Logf("CLOUD E2E: Starting Firestore document cleanup for collection '%s'...", collectionID)
	fsClient, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		t.Logf("CLOUD E2E: Warning - failed to create Firestore client for cleanup: %v", err)
		return
	}
	defer fsClient.Close()

	colRef := fsClient.Collection(collectionID)
	batchSize := 100
	for {
		iter := colRef.Limit(batchSize).Documents(ctx)
		numDeleted := 0
		batch := fsClient.Batch()
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			require.NoError(t, err, "Failed to iterate documents for deletion")
			batch.Delete(doc.Ref)
			numDeleted++
		}
		if numDeleted == 0 {
			break
		}
		_, err = batch.Commit(ctx)
		require.NoError(t, err, "Failed to commit batch delete")
		t.Logf("CLOUD E2E: Deleted %d documents from collection '%s'", numDeleted, collectionID)
	}
	t.Logf("CLOUD E2E: Firestore document cleanup finished for collection '%s'.", collectionID)
}

// --- Main Cloud E2E Test Function ---
func TestEndToEndTwoServiceCloudScenario(t *testing.T) {
	t.Setenv("GCP_PROJECT_ID", cloudTestGCPProjectID) // Real Project MessageID
	gcpProjectID := os.Getenv("GCP_PROJECT_ID")
	if gcpProjectID == "" {
		t.Skip("Skipping cloud integration test: GCP_PROJECT_ID environment variable is not set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Log("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		_, errAdc := firestore.NewClient(adcCheckCtx, gcpProjectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud integration test: ADC check failed with: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
	}

	cfg := endtoend.DefaultCloudTestConfig()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.OverallTestTimeout)
	defer cancel()

	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	baseLogger := zerolog.New(consoleWriter).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	runID := uuid.NewString()
	testPrefix := fmt.Sprintf("%s-%s", cloudTestRunPrefix, runID[:8])

	// --- 1. Define Dynamic Resource Names ---
	topicRaw := testPrefix + "-raw-mqtt"
	subRaw := testPrefix + "-sub-enrich"
	topicEnriched := testPrefix + "-enriched"
	topicUnidentified := testPrefix + "-unidentified"
	collectionDevices := testPrefix + "-devices"

	// --- 2. Setup External Dependencies (MQTT local, GCP real) ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerCloudE2E(t, ctx)
	defer mosquittoCleanup()

	pubsubTopicsToCreate := []string{topicRaw, topicEnriched, topicUnidentified}
	pubsubSubsToCreate := map[string]string{subRaw: topicRaw}
	pubsubCleanup := setupRealPubSubResources(t, ctx, gcpProjectID, pubsubTopicsToCreate, pubsubSubsToCreate)
	defer pubsubCleanup()

	defer cleanupFirestoreCollection(t, context.Background(), gcpProjectID, collectionDevices)

	// --- 3. Configure and Run Services ---
	t.Setenv("PUBSUB_EMULATOR_HOST", "")
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")

	// Service 1: Converter
	converterLogger := baseLogger.With().Str("component", "ConverterService").Logger()
	converterPubCfg := &converter.GooglePubSubPublisherConfig{
		ProjectID: gcpProjectID,
		TopicID:   topicRaw,
	}
	converterPublisher, err := converter.NewGooglePubSubPublisher(ctx, converterPubCfg, converterLogger)
	require.NoError(t, err)
	defer converterPublisher.Stop()

	// Create and start the converter service
	mqttCfg := &converter.MQTTClientConfig{
		BrokerURL:        mqttBrokerURL,
		Topic:            cloudMqttInputTopic,
		ClientIDPrefix:   "e2e-converter-",
		KeepAlive:        60 * time.Second,
		ConnectTimeout:   10 * time.Second,
		ReconnectWaitMax: 1 * time.Minute,
	}
	converterService := converter.NewIngestionService(converterPublisher, converterLogger,
		cfg.ConverterServiceConfig, mqttCfg)
	var converterWg sync.WaitGroup
	converterWg.Add(1)
	go func() {
		defer converterWg.Done()
		if err := converterService.Start(); err != nil && !errors.Is(err, context.Canceled) {
			converterLogger.Error().Err(err).Msg("Converter service returned a fatal error")
		}
	}()

	// Service 2: Enrichment
	enrichmentLogger := baseLogger.With().Str("component", "EnrichmentService").Logger()
	enrichedPubCfg := &connectors.GooglePubSubPublisherConfig{ProjectID: gcpProjectID, TopicID: topicEnriched}
	enrichedPublisher, err := connectors.NewGooglePubSubPublisher(ctx, enrichedPubCfg, enrichmentLogger)
	require.NoError(t, err)

	unidentifiedPubCfg := &connectors.GooglePubSubPublisherConfig{ProjectID: gcpProjectID, TopicID: topicUnidentified}
	unidentifiedPublisher, err := connectors.NewGooglePubSubPublisher(ctx, unidentifiedPubCfg, enrichmentLogger)
	require.NoError(t, err)

	firestoreFetcherCfg := &connectors.FirestoreFetcherConfig{ProjectID: gcpProjectID, CollectionName: collectionDevices}
	metadataFetcher, err := connectors.NewGoogleDeviceMetadataFetcher(ctx, firestoreFetcherCfg, enrichmentLogger)
	require.NoError(t, err)

	inputSubCfg := connectors.PubSubSubscriptionConfig{ProjectID: gcpProjectID, SubscriptionID: subRaw}
	enrichmentService, err := connectors.NewEnrichmentService(ctx, cfg.EnrichmentServiceConfig, inputSubCfg,
		metadataFetcher.Fetch, enrichedPublisher, unidentifiedPublisher, enrichmentLogger)
	require.NoError(t, err)

	var enrichmentWg sync.WaitGroup
	enrichmentWg.Add(1)
	go func() {
		defer enrichmentWg.Done()
		if err := enrichmentService.Start(); err != nil && !errors.Is(err, context.Canceled) {
			enrichmentLogger.Error().Err(err).Msg("Enrichment service returned a fatal error")
		}
	}()

	t.Log("Waiting for services to start up and connect to cloud resources...")
	time.Sleep(cfg.CloudServiceStartupDelay)
	t.Log("...Startup delay finished. Proceeding with test.")

	// --- 4. Seed Firestore ---
	fsClientForSeeding, err := firestore.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)
	defer fsClientForSeeding.Close()

	// --- 5. Configure and Run Messenger (Load Generator) ---
	messengerLogger := baseLogger.With().Str("component", "Messenger").Logger()
	totalDevices := cfg.NumKnownDevices + cfg.NumUnknownDevices

	messengerDeviceGenCfg := &messenger.DeviceGeneratorConfig{
		NumDevices:          totalDevices,
		MsgRatePerDevice:    cfg.MsgRatePerDevice,
		FirestoreProjectID:  gcpProjectID,
		FirestoreCollection: collectionDevices,
	}
	fsClientForMessenger, err := firestore.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)
	defer fsClientForMessenger.Close()
	messengerDeviceGen, err := messenger.NewDeviceGenerator(messengerDeviceGenCfg, fsClientForMessenger, messengerLogger)
	require.NoError(t, err)
	messengerDeviceGen.CreateDevices()

	var knownDeviceEUIs []string
	for i, dev := range messengerDeviceGen.Devices {
		if i < cfg.NumKnownDevices {
			eui := dev.GetEUI()
			knownDeviceEUIs = append(knownDeviceEUIs, eui)
			_, err = fsClientForSeeding.Collection(collectionDevices).Doc(eui).Set(ctx, map[string]string{
				"clientID":       fmt.Sprintf("CloudClient-%s", eui),
				"locationID":     "CloudLoc-ABC",
				"deviceCategory": "Cloud-Sensor",
			})
			require.NoError(t, err)
		}
	}
	t.Logf("CLOUD E2E: Seeded %d known devices into REAL Firestore.", len(knownDeviceEUIs))

	pahoFactory := &messenger.PahoMQTTClientFactory{}
	messengerPublisher := messenger.NewPublisher(
		cloudMqttPublishPattern,
		mqttBrokerURL,
		"e2e-messenger-publisher",
		1, // QOS
		messengerLogger,
		pahoFactory,
	)

	messengerContextDuration := cfg.LoadGenDuration + 25*time.Second
	messengerCtx, messengerCancel := context.WithTimeout(ctx, messengerContextDuration)
	defer messengerCancel()
	var messengerWg sync.WaitGroup
	messengerWg.Add(1)
	go func() {
		defer messengerWg.Done()
		messengerLogger.Info().Msg("Messenger starting...")
		errPub := messengerPublisher.Publish(messengerDeviceGen.Devices, cfg.LoadGenDuration, messengerCtx)
		if errPub != nil && !errors.Is(errPub, context.Canceled) && !errors.Is(errPub, context.DeadlineExceeded) {
			messengerLogger.Error().Err(errPub).Msg("Messenger publisher error")
		}
		messengerLogger.Info().Msg("Messenger finished.")
	}()

	// --- 6. Configure and Run Verifier ---
	verifierLogger := baseLogger.With().Str("component", "Verifier").Logger()
	verifierCtx, verifierCancel := context.WithCancel(ctx)

	verifierConfig := loadtestverifier.VerifierConfig{
		ProjectID:           gcpProjectID,
		EnrichedTopicID:     topicEnriched,
		UnidentifiedTopicID: topicUnidentified,
		TestDuration:        cfg.LoadGenDuration + 30*time.Second,
		OutputFile:          filepath.Join(t.TempDir(), "cloud_verifier_results.json"),
		TestRunID:           runID,
	}
	verifier, err := loadtestverifier.NewVerifier(verifierConfig, verifierLogger)
	require.NoError(t, err)

	var verifierResults *loadtestverifier.TestRunResults
	var verifierErr error
	var verifierWg sync.WaitGroup
	verifierWg.Add(1)
	go func() {
		defer verifierWg.Done()
		verifierResults, verifierErr = verifier.Run(verifierCtx)
	}()

	// --- 7. Wait and Verify ---
	t.Log("Waiting for messenger to complete...")
	messengerWg.Wait()
	t.Log("Messenger completed. Allowing grace period for message processing...")
	time.Sleep(cfg.GracePeriodForMessageCompletion)

	t.Log("Signaling verifier to stop and waiting for it to complete...")
	verifierCancel()
	verifierWg.Wait()
	if verifierErr != nil && !errors.Is(verifierErr, context.Canceled) {
		t.Fatalf("Verifier returned an unexpected error: %v", verifierErr)
	}
	require.NotNil(t, verifierResults, "Verifier results are nil. This implies a failure in the verifier's run.")

	t.Logf("CLOUD E2E: Final Verifier Results: Total=%d, Enriched=%d, Unidentified=%d", verifierResults.TotalMessagesVerified, verifierResults.EnrichedMessages, verifierResults.UnidentifiedMessages)

	// --- 8. Explicitly Stop Services and Wait ---
	t.Log("CLOUD E2E: Shutting down services...")
	converterService.Stop()
	enrichmentService.Stop()

	t.Log("CLOUD E2E: Waiting for services to fully stop...")
	converterWg.Wait()
	enrichmentWg.Wait()
	t.Log("CLOUD E2E: Services confirmed stopped.")

	// --- 9. Assert Results ---
	minMessagesMultiplier := 0.7
	minEnriched := int(float64(cfg.NumKnownDevices) * cfg.MsgRatePerDevice * cfg.LoadGenDuration.Seconds() * minMessagesMultiplier)
	minUnidentified := int(float64(cfg.NumUnknownDevices) * cfg.MsgRatePerDevice * cfg.LoadGenDuration.Seconds() * minMessagesMultiplier)

	assert.GreaterOrEqualf(t, verifierResults.EnrichedMessages, minEnriched, "Not enough enriched messages. Expected at least ~%d, got %d", minEnriched, verifierResults.EnrichedMessages)
	assert.GreaterOrEqualf(t, verifierResults.UnidentifiedMessages, minUnidentified, "Not enough unidentified messages. Expected at least ~%d, got %d", minUnidentified, verifierResults.UnidentifiedMessages)

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
		assert.GreaterOrEqualf(t, foundKnownEUIs[eui], int(cfg.MsgRatePerDevice*cfg.LoadGenDuration.Seconds()*minMessagesMultiplier), "Known EUI %s missing sufficient enriched messages", eui)
		assert.Zero(t, foundUnknownEUIs[eui], "Known EUI %s should not appear in unidentified messages", eui)
	}

	t.Log("CLOUD E2E: Test scenario completed successfully.")
}
