//go:build integration

package main_test

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
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

// --- E2E Test Constants ---
const (
	testProjectID = "e2e-garden-project"

	// MQTT -> Ingestion Service
	testMosquittoImage     = "eclipse-mosquitto:2.0"
	testMqttBrokerPort     = "1883"
	testMqttTopicPattern   = "devices/+/data"
	testMqttDeviceUID      = "GARDEN_MONITOR_E2E_001" // Default device UID for tests
	testMqttClientIDPrefix = "ingestion-service-e2e"

	// Pub/Sub -> Link between services
	testPubsubEmulatorImage  = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubsubEmulatorPort   = "8085"
	testPubsubTopicID        = "garden-monitor-processed"     // Ingestion publishes here
	testPubsubSubscriptionID = "garden-monitor-processed-sub" // Processing subscribes here

	// GCS
	testGCSBucket = "test-bucket"
)

// e2eTestCase defines the structure for a single end-to-end test case.
type e2eTestCase struct {
	name               string
	messagesToPublish  []types.GardenMonitorReadings
	expectedGCSObjects int
	// expectedBigQueryRows int // Not currently verified in the provided e2e test, kept for completeness if future BigQuery verification is added.
}

// TestE2E_MqttToIceStoreFlow runs a suite of end-to-end tests for the MQTT to IceStore data flow.
func TestE2E_MqttToIceStoreFlow(t *testing.T) {
	// Define test cases
	testCases := []e2eTestCase{
		{
			name: "Single message flow",
			messagesToPublish: []types.GardenMonitorReadings{
				{DE: testMqttDeviceUID, Sequence: 123, Battery: 98},
			},
			expectedGCSObjects: 1,
		},
		{
			name: "Multiple messages from same device",
			messagesToPublish: []types.GardenMonitorReadings{
				{DE: testMqttDeviceUID, Sequence: 1, Battery: 90},
				{DE: testMqttDeviceUID, Sequence: 2, Battery: 85},
				{DE: testMqttDeviceUID, Sequence: 3, Battery: 80},
			},
			// Expecting 1 GCS object if the batcher combines them based on configuration
			// If batchSize is 4 and flushTimeout is 3s (from icinit), then 3 messages will result in 1 object.
			expectedGCSObjects: 1,
		},
		{
			name:               "No messages published",
			messagesToPublish:  []types.GardenMonitorReadings{},
			expectedGCSObjects: 0,
		},
		{
			name: "Messages from different devices (may result in more GCS objects based on batching strategy)",
			messagesToPublish: []types.GardenMonitorReadings{
				{DE: "DEVICE_001", Sequence: 10, Battery: 70},
				{DE: "DEVICE_002", Sequence: 20, Battery: 65},
				{DE: "DEVICE_003", Sequence: 30, Battery: 60},
				{DE: "DEVICE_001", Sequence: 11, Battery: 68}, // Second message for DEVICE_001
			},
			// The current batcher config (batchSize: 4, flushTimeout: 3s) might combine these.
			// It will depend on how the ArchivalTransformer groups the data by location/time.
			// For simplicity, assuming they might group into one object if flushed by timeout.
			// A more robust test would define expected files per path.
			expectedGCSObjects: 1,
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runE2ETestCase(t, tc)
		})
	}
}

// runE2ETestCase encapsulates the full E2E setup, execution, and verification for a single test case.
func runE2ETestCase(t *testing.T, tc e2eTestCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	log.Info().Msg("E2E: Setting up Mosquitto emulator...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	defer mosquittoCleanup()

	log.Info().Msg("E2E: Setting up Pub/Sub emulator...")
	pubsubOptions, pubsubCleanup := emulators.SetupPubSubEmulator(t, ctx, emulators.PubsubConfig{
		GCImageContainer: emulators.GCImageContainer{
			ImageContainer: emulators.ImageContainer{
				EmulatorImage:    testPubsubEmulatorImage,
				EmulatorHTTPPort: testPubsubEmulatorPort,
			},
			ProjectID: testProjectID,
		},
		TopicSubs: map[string]string{testPubsubTopicID: testPubsubSubscriptionID},
	})
	defer pubsubCleanup()

	consumerLogger := log.With().Str("service", "pubsub-consumer").Logger()
	gcsConsumer, err := consumers.NewGooglePubsubConsumer(ctx, &consumers.GooglePubsubConsumerConfig{
		ProjectID:      testProjectID,
		SubscriptionID: testPubsubSubscriptionID,
	}, pubsubOptions, consumerLogger)
	require.NoError(t, err)

	log.Info().Msg("Pausing before ingestion setup")
	time.Sleep(3 * time.Second) // Give emulators time to spin up fully

	// --- 2. Configure and Start MQTT Ingestion Service ---
	mqttCfg := &mqinit.Config{
		LogLevel:  "debug",
		ProjectID: testProjectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			TopicID: testPubsubTopicID,
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicPattern,
			ClientIDPrefix: "ingestion-test-client-",
			KeepAlive:      10 * time.Second,
			ConnectTimeout: 5 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
	}

	mqttLogger := log.With().Str("service", "mqtt-ingestion").Logger()
	mqttPublisher, err := mqttconverter.NewGooglePubsubPublisher(ctx, mqttconverter.GooglePubsubPublisherConfig{
		ProjectID: mqttCfg.ProjectID,
		TopicID:   mqttCfg.Publisher.TopicID,
	}, mqttLogger)
	require.NoError(t, err)

	ingestionService := mqttconverter.NewIngestionService(mqttPublisher, nil, mqttLogger, mqttCfg.Service, mqttCfg.MQTT)

	mqttServer := mqinit.NewServer(mqttCfg, ingestionService, mqttLogger)

	go func() {
		if err := mqttServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Server failed during test execution")
		}
	}()
	defer mqttServer.Shutdown()
	time.Sleep(2 * time.Second) // Give the server and MQTT client time to connect

	// --- 3. Configure and Start IceStore Processing Service ---
	icestoreCfg := &icinit.IceServiceConfig{
		LogLevel:  "debug",
		ProjectID: testProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: testPubsubSubscriptionID},
		IceStore: struct {
			CredentialsFile string `mapstructure:"credentials_file"`
			BucketName      string `mapstructure:"bucket_name"`
		}{
			BucketName: testGCSBucket,
		},
		BatchProcessing: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{
			NumWorkers:   5,
			BatchSize:    4,               // Matches the default batcher config in main.go/icinit
			FlushTimeout: time.Second * 3, // Matches the default batcher config in main.go/icinit
		},
	}

	gcsLogger := log.With().Str("service", "gcs-processor").Logger()
	gcsClient, gcsCleanUp := emulators.SetupGCSEmulator(t, ctx, emulators.GetDefaultGCSConfig(testProjectID, testGCSBucket))
	defer gcsCleanUp()

	// Clear the GCS bucket before each test case run to ensure isolation
	require.NoError(t, clearBucket(ctx, gcsClient.Bucket(testGCSBucket)), "Failed to clear GCS bucket")

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{BatchSize: icestoreCfg.BatchProcessing.BatchSize, FlushTimeout: icestoreCfg.BatchProcessing.FlushTimeout},
		icestore.GCSBatchUploaderConfig{BucketName: testGCSBucket, ObjectPrefix: "archived-data"},
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

	time.Sleep(3 * time.Second) // Give services time to start and connect

	// --- 4. Publish Test Messages to MQTT ---
	if len(tc.messagesToPublish) > 0 {
		mqttTestPublisher, err := emulators.CreateTestMqttPublisher(mqttBrokerURL, "e2e-test-publisher")
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
		log.Info().Int("count", len(tc.messagesToPublish)).Msg("E2E: Published test messages to MQTT.")
	} else {
		log.Info().Msg("E2E: No messages to publish for this test case.")
	}

	// Give some time for messages to be processed and flushed by the IceStore service
	// This duration should be at least the FlushTimeout of the batcher + some buffer.
	processingWaitTime := icestoreCfg.BatchProcessing.FlushTimeout + 5*time.Second // Add extra buffer
	log.Info().Dur("wait", processingWaitTime).Msg("E2E: Waiting for messages to be processed by IceStore service.")
	time.Sleep(processingWaitTime)

	// --- 5. Verify Data in GCS ---
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), time.Second*20)
	defer cancelVerify()

	log.Info().Msg("E2E: Verifying GCS contents...")
	require.Eventually(t, func() bool {
		bucket := gcsClient.Bucket(testGCSBucket)
		objects, err := listGCSObjectAttrs(verifyCtx, bucket)
		if err != nil {
			t.Logf("Verification failed to list objects, will retry: %v", err)
			return false
		}

		// Verify the number of objects created in GCS
		if assert.Len(t, objects, tc.expectedGCSObjects, "Incorrect number of files created in GCS") {
			if tc.expectedGCSObjects > 0 {
				// Optional: Further validation of GCS object content can be added here,
				// similar to what's in integration_test.go's decompressAndScan.
				// For now, just checking count.
				log.Info().Int("actual", len(objects)).Int("expected", tc.expectedGCSObjects).Msg("GCS object count matched.")
			}
			return true
		}
		return false
	}, 20*time.Second, 500*time.Millisecond, "GCS verification failed after publishing messages")

	// --- 6. BigQueryConfig verification (currently commented out/simplified as per original file) ---
	// var receivedRows []types.GardenMonitorReadings
	// require.Len(t, receivedRows, tc.expectedBigQueryRows, "Incorrect number of rows in BigQuery")
	// If you uncomment BigQuery verification, ensure the `receivedRows` is populated.
	// assert.Equal(t, testPayload.DE, receivedRows[0].DE)
	// assert.Equal(t, testPayload.Sequence, receivedRows[0].Sequence)
	// assert.Equal(t, testPayload.Battery, receivedRows[0].Battery)

	log.Info().Msgf("E2E: Test case '%s' completed successfully!", tc.name)
}

// listGCSObjectAttrs is a helper function to list objects in a GCS bucket.
func listGCSObjectAttrs(ctx context.Context, bucket *storage.BucketHandle) ([]*storage.ObjectAttrs, error) {
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
func clearBucket(ctx context.Context, bucket *storage.BucketHandle) error {
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
