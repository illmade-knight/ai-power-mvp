//go:build integration

package main_test

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators" // Now expected to contain all emulator helper functions
	"github.com/illmade-knight/go-iot/pkg/icestore"
	"google.golang.org/api/iterator"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import initializers for both services
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/icestore/icinit"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/ingestion/mqinit"

	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/illmade-knight/go-iot/pkg/types"
)

// --- E2E Cloud Test Constants ---
const (
	cloudTestGCPProjectID = "gemini-power-test"
	// ProjectID will be read from GOOGLE_CLOUD_PROJECT environment variable
	// testProjectID is intentionally not set here; it's read from env.

	// MQTT -> Ingestion Service (This part still uses a local Mosquitto emulator, but connects to real Pub/Sub)
	testMosquittoImage     = "eclipse-mosquitto:2.0"
	testMqttBrokerPort     = "1883"
	testMqttTopicPattern   = "devices/+/data"
	testMqttDeviceUID      = "GARDEN_MONITOR_CLOUD_001" // Default device UID for cloud tests
	testMqttClientIDPrefix = "ingestion-service-cloud-e2e"

	// Pub/Sub -> Link between services (Now real Google Cloud Pub/Sub)
	cloudPubsubTopicID        = "garden-monitor-processed-cloud"     // Ingestion publishes here
	cloudPubsubSubscriptionID = "garden-monitor-processed-sub-cloud" // Processing subscribes here

	// GCS (Now real Google Cloud Storage)
	cloudGCSBucketPrefix = "e2e-garden-monitor-test-" // Prefix for unique bucket names
	maxBucketNameLength  = 63
)

// e2eTestCase defines the structure for a single end-to-end test case.
type e2eTestCase struct {
	name               string
	messagesToPublish  []types.GardenMonitorReadings
	expectedGCSObjects int
	// expectedBigQueryRows int // Not currently verified in the provided e2e test, kept for completeness.
}

// TestE2E_MqttToIceStoreFlowCloud runs a suite of end-to-end tests using real Google Cloud resources.
func TestE2E_MqttToIceStoreFlowCloud(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		// Perform a quick check to see if ADC is likely to work.
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		_, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
	}

	// --- One-time Setup for Mosquitto Emulator ---
	// Create a context for the entire test suite, including Mosquitto setup
	suiteCtx, suiteCancel := context.WithCancel(context.Background())
	defer suiteCancel() // Ensure this is called when the suite finishes

	log.Info().Msg("E2E Cloud: Setting up Mosquitto emulator (once for all tests)...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, suiteCtx, emulators.GetDefaultMqttImageContainer())
	defer mosquittoCleanup() // This will clean up after all subtests complete

	pubsubCleanup, err := setupRealPubSub(t, suiteCtx, projectID, cloudPubsubTopicID, cloudPubsubSubscriptionID)
	defer pubsubCleanup()
	if err != nil {
		t.Fatal(err)
	}

	// Define test cases
	testCases := []e2eTestCase{
		{
			name: "Single message flow to cloud",
			messagesToPublish: []types.GardenMonitorReadings{
				{DE: testMqttDeviceUID, Sequence: 123, Battery: 98},
			},
			expectedGCSObjects: 1,
		},
		{
			name: "Multiple messages from same device to cloud",
			messagesToPublish: []types.GardenMonitorReadings{
				{DE: testMqttDeviceUID, Sequence: 1, Battery: 90},
				{DE: testMqttDeviceUID, Sequence: 2, Battery: 85},
				{DE: testMqttDeviceUID, Sequence: 3, Battery: 80},
			},
			// Expecting 1 GCS object if the batcher combines them based on configuration
			expectedGCSObjects: 1,
		},
		{
			name:               "No messages published to cloud",
			messagesToPublish:  []types.GardenMonitorReadings{},
			expectedGCSObjects: 0,
		},
		{
			name: "Messages from different devices to cloud",
			messagesToPublish: []types.GardenMonitorReadings{
				{DE: "DEVICE_CLOUD_001", Sequence: 10, Battery: 70},
				{DE: "DEVICE_CLOUD_002", Sequence: 20, Battery: 65},
				{DE: "DEVICE_CLOUD_003", Sequence: 30, Battery: 60},
				{DE: "DEVICE_CLOUD_001", Sequence: 11, Battery: 68},
			},
			expectedGCSObjects: 1,
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runCloudE2ETestCase(t, projectID, mqttBrokerURL, tc)
		})
	}
}

// runCloudE2ETestCase encapsulates the full E2E setup, execution, and verification for a single cloud test case.
func runCloudE2ETestCase(t *testing.T, projectID, mqttBrokerURL string, tc e2eTestCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Increased timeout for cloud ops
	defer cancel()

	// Ensure logger is configured for each test case run
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 2. Configure and Start MQTT Ingestion Service (connecting to real Pub/Sub) ---
	log.Info().Msg("E2E Cloud: Configuring MQTT Ingestion Service to connect to real Pub/Sub...")
	mqttCfg := &mqinit.Config{
		LogLevel:  "debug",
		ProjectID: projectID, // Use real project ID
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			TopicID: cloudPubsubTopicID, // Use real Pub/Sub topic ID
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicPattern,
			ClientIDPrefix: "ingestion-test-cloud-client-",
			KeepAlive:      10 * time.Second,
			ConnectTimeout: 5 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
	}

	mqttLogger := log.With().Str("service", "mqtt-ingestion").Logger()
	// NewGooglePubsubPublisher will now connect to real Pub/Sub based on projectID and ADC/credentials
	mqttPublisher, err := mqttconverter.NewGooglePubsubPublisher(ctx, mqttconverter.GooglePubsubPublisherConfig{
		ProjectID: mqttCfg.ProjectID,
		TopicID:   mqttCfg.Publisher.TopicID,
	}, mqttLogger)
	require.NoError(t, err)

	// Ensure Pub/Sub topic and subscription exist for the test
	pubsubClient, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer pubsubClient.Close()

	topic := pubsubClient.Topic(cloudPubsubTopicID)
	exists, err := topic.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		topic, err = pubsubClient.CreateTopic(ctx, cloudPubsubTopicID)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		// Clean up the topic and subscription after the test
		// Use a dedicated cleanup context for Pub/Sub deletion as well,
		// though the GCS one is more prone to issues.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		log.Info().Msgf("E2E Cloud: Deleting Pub/Sub topic %s and subscription %s", cloudPubsubTopicID, cloudPubsubSubscriptionID)
		sub := pubsubClient.Subscription(cloudPubsubSubscriptionID)
		// Delete subscription first, as topic cannot be deleted if it has subscriptions
		// We'll ignore errors on best effort cleanup if they're already gone or not found.
		if subExists, err := sub.Exists(cleanupCtx); err == nil && subExists {
			sub.Delete(cleanupCtx)
		}
		if topicExists, err := topic.Exists(cleanupCtx); err == nil && topicExists {
			topic.Delete(cleanupCtx)
		}
	})

	ingestionService := mqttconverter.NewIngestionService(mqttPublisher, nil, mqttLogger, mqttCfg.Service, mqttCfg.MQTT)
	mqttServer := mqinit.NewServer(mqttCfg, ingestionService, mqttLogger)
	go func() {
		if err := mqttServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("MQTT Ingestion Server failed during test execution")
		}
	}()
	defer mqttServer.Shutdown()
	time.Sleep(2 * time.Second) // Give the server and MQTT client time to connect

	// --- 3. Configure and Start IceStore Processing Service (connecting to real GCS) ---
	log.Info().Msg("E2E Cloud: Configuring IceStore Processing Service to connect to real GCS...")

	// Generate a unique UUID for the bucket name
	id := uuid.New().String()
	// GCS bucket names must be lowercase and contain only lowercase letters, numbers, hyphens, and dots.
	// They cannot start or end with a hyphen or contain underscores.
	// Removing hyphens from UUID for simplicity and direct compliance.
	uniqueID := strings.ReplaceAll(id, "-", "") // Remove hyphens from UUID

	// Construct the unique bucket name using the prefix and the unique ID
	// Example: e2e-garden-monitor-test-abcdef1234567890abcdef1234567890
	uniqueBucketName := fmt.Sprintf("%s-%s", cloudGCSBucketPrefix, uniqueID)

	// Ensure the bucket name is lowercase (UUIDs are usually lowercase, but good practice)
	uniqueBucketName = strings.ToLower(uniqueBucketName)

	icestoreCfg := &icinit.IceServiceConfig{
		LogLevel:  "debug",
		ProjectID: projectID, // Use real project ID
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: cloudPubsubSubscriptionID}, // Use real Pub/Sub subscription ID
		IceStore: struct {
			CredentialsFile string `mapstructure:"credentials_file"`
			BucketName      string `mapstructure:"bucket_name"`
		}{
			BucketName: uniqueBucketName, // Use the unique bucket name
		},
		BatchProcessing: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{
			NumWorkers:   5,
			BatchSize:    4,
			FlushTimeout: time.Second * 10, // Increased timeout for cloud operations
		},
	}

	gcsLogger := log.With().Str("service", "gcs-processor").Logger()

	// Initialize real GCS client
	gcsClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	defer gcsClient.Close()

	// Create the unique bucket
	bucketHandle := gcsClient.Bucket(uniqueBucketName)
	// For storage v1.55.0+, the Create method takes only context and bucket attributes.
	// The projectID is implicitly taken from the client's configuration (ADC/credentials).
	err = bucketHandle.Create(ctx, projectID, nil)
	if err != nil { // Ignore if bucket already exists (unlikely with unique name)
		require.NoError(t, err, "Failed to create GCS bucket")
	}

	// Ensure the bucket is deleted after the test
	t.Cleanup(func() {
		log.Info().Msgf("E2E Cloud: Clearing and deleting GCS bucket: %s", uniqueBucketName)
		if err := clearBucket(t, ctx, bucketHandle); err != nil {
			log.Error().Err(err).Msg("Failed to clear bucket during cleanup")
		}
		// The bucket must be empty before it can be deleted.
		// `clearBucket` should ensure this, but we add a check for ErrBucketNotEmpty
		// just in case of race conditions or if `clearBucket` fails partially.
		if err := bucketHandle.Delete(ctx); err != nil {
			log.Error().Err(err).Msgf("Failed to delete GCS bucket %s during cleanup", uniqueBucketName)
		}
	})

	// Create Pub/Sub subscription for IceStore consumer
	sub := pubsubClient.Subscription(cloudPubsubSubscriptionID)
	exists, err = sub.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		_, err = pubsubClient.CreateSubscription(ctx, cloudPubsubSubscriptionID, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 10 * time.Second,
		})
		require.NoError(t, err)
	}

	gcsConsumer, err := consumers.NewGooglePubsubConsumer(ctx, &consumers.GooglePubsubConsumerConfig{
		ProjectID:      projectID,
		SubscriptionID: cloudPubsubSubscriptionID,
	}, nil, gcsLogger) // nil for pubsub.ClientOption, relies on ADC/credentials
	require.NoError(t, err)

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{BatchSize: icestoreCfg.BatchProcessing.BatchSize, FlushTimeout: icestoreCfg.BatchProcessing.FlushTimeout},
		icestore.GCSBatchUploaderConfig{BucketName: uniqueBucketName, ObjectPrefix: "archived-data"},
		gcsLogger,
	)
	require.NoError(t, err)

	gcsService, err := icestore.NewIceStorageService(2, gcsConsumer, batcher, icestore.ArchivalTransformer, gcsLogger)
	require.NoError(t, err)

	gcsServer := icinit.NewServer(icestoreCfg, gcsService, gcsLogger)
	go func() {
		if err := gcsServer.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("IceStore Processing server failed: %v", err)
		}
	}()
	defer gcsServer.Shutdown()

	time.Sleep(5 * time.Second) // Give services more time to start and connect to cloud resources

	// --- 4. Publish Test Messages to MQTT ---
	if len(tc.messagesToPublish) > 0 {
		// Use a simple test publisher for MQTT, assuming Mosquitto is still local.
		mqttTestPublisher, err := emulators.CreateTestMqttPublisher(mqttBrokerURL, "e2e-test-cloud-publisher") // Now calls function from emulators package
		require.NoError(t, err)
		defer mqttTestPublisher.Disconnect(250)

		for _, msg := range tc.messagesToPublish {
			msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &msg})
			require.NoError(t, err)
			publishTopic := strings.Replace(testMqttTopicPattern, "+", msg.DE, 1) // Use device ID from message
			token := mqttTestPublisher.Publish(publishTopic, 1, false, msgBytes)
			token.Wait()
			require.NoError(t, token.Error())
		}
		log.Info().Int("count", len(tc.messagesToPublish)).Msg("E2E Cloud: Published test messages to MQTT.")
	} else {
		log.Info().Msg("E2E Cloud: No messages to publish for this test case.")
	}

	// Give ample time for messages to be processed by Pub/Sub and flushed by IceStore service
	processingWaitTime := icestoreCfg.BatchProcessing.FlushTimeout + 15*time.Second // Increased buffer for cloud
	log.Info().Dur("wait", processingWaitTime).Msg("E2E Cloud: Waiting for messages to be processed by IceStore service.")
	time.Sleep(processingWaitTime)

	// --- 5. Verify Data in GCS ---
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), time.Second*30) // Increased timeout
	defer cancelVerify()

	log.Info().Msg("E2E Cloud: Verifying GCS contents...")
	require.Eventually(t, func() bool {
		objects, err := listGCSObjectAttrs(t, verifyCtx, bucketHandle)
		if err != nil {
			t.Logf("Verification failed to list objects, will retry: %v", err)
			return false
		}

		if assert.Len(t, objects, tc.expectedGCSObjects, "Incorrect number of files created in GCS") {
			if tc.expectedGCSObjects > 0 {
				log.Info().Int("actual", len(objects)).Int("expected", tc.expectedGCSObjects).Msg("GCS object count matched.")
			}
			return true
		}
		return false
	}, 30*time.Second, 1*time.Second, "GCS verification failed after publishing messages")

	// --- 6. BigQueryConfig verification (currently commented out/simplified) ---
	// var receivedRows []types.GardenMonitorReadings
	// require.Len(t, receivedRows, tc.expectedBigQueryRows, "Incorrect number of rows in BigQuery")
	// If you uncomment BigQuery verification, ensure the `receivedRows` is populated.

	log.Info().Msgf("E2E Cloud: Test case '%s' completed successfully!", tc.name)
}

// listGCSObjectAttrs is a helper function to list objects in a GCS bucket.
func listGCSObjectAttrs(t *testing.T, ctx context.Context, bucket *storage.BucketHandle) ([]*storage.ObjectAttrs, error) {
	t.Helper() // Added t.Helper()
	var attrs []*storage.ObjectAttrs
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		attrs = append(attrs, objAttrs)
	}
	return attrs, nil
}

// clearBucket is a helper function to delete all objects in a GCS bucket.
func clearBucket(t *testing.T, ctx context.Context, bucket *storage.BucketHandle) error {
	t.Helper() // Added t.Helper()
	it := bucket.Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects for deletion: %w", err)
		}
		if err := bucket.Object(attrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", attrs.Name, err)
		}
	}
	return nil
}

// setupRealPubSub creates a topic and subscription on GCP for the test run.
// It returns a cleanup function to delete them.
func setupRealPubSub(t *testing.T, ctx context.Context, projectID, topicID, subID string) (func(), error) {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real Pub/Sub client")

	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		topic, err = client.CreateTopic(ctx, topicID)
		require.NoError(t, err, "Failed to create real Pub/Sub topic")
		t.Logf("Created Cloud Pub/Sub Topic: %s", topic.ID())
	} else {
		t.Logf("Cloud Pub/Sub Topic: %s", topic.ID())
	}

	sub := client.Subscription(subID)
	exists, err = sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		sub, err = client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 20 * time.Second,
		})
		require.NoError(t, err, "Failed to create real Pub/Sub subscription")
		t.Logf("Created Cloud Pub/Sub Subscription: %s", sub.ID())
	} else {
		t.Logf("Real Pub/Sub Subscription Exists: %s", sub.ID())
	}

	return func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		t.Logf("Tearing down Cloud Pub/Sub resources...")

		subRef := client.Subscription(subID)
		if err := subRef.Delete(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete subscription '%s': %v", subID, err)
		} else {
			t.Logf("Deleted subscription '%s'", subID)
		}

		topicRef := client.Topic(topicID)
		if err := topicRef.Delete(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete topic '%s': %v", topicID, err)
		} else {
			t.Logf("Deleted topic '%s'", topicID)
		}
		client.Close()
	}, nil
}
