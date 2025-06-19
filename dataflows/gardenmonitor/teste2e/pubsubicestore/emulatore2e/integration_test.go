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
	testMqttBrokerPort     = "1883/tcp"
	testMqttTopicPattern   = "devices/+/data"
	testMqttDeviceUID      = "GARDEN_MONITOR_E2E_001"
	testMqttClientIDPrefix = "ingestion-service-e2e"

	// Pub/Sub -> Link between services
	// Pub/Sub (The link between Ingestion and Processing services)
	testPubsubEmulatorImage  = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubsubEmulatorPort   = "8085"
	testPubsubTopicID        = "garden-monitor-processed"     // Ingestion publishes here
	testPubsubSubscriptionID = "garden-monitor-processed-sub" // Processing subscribes here

	// GCS
	testGCSBucket = "test-bucket"
	testGCSImage  = "fsouza/fake-gcs-server:latest"
	testGCSPORT   = "4443"

	// Service Ports
	e2eMqttHTTPPort = ":9090"
	e2eBqHTTPPort   = ":9091"
)

// --- Full End-to-End Test ---

func TestE2E_MqttToIceStoreFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 1. Start All Emulators ---
	log.Info().Msg("E2E: Setting up Mosquitto emulator...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, ctx, emulators.ImageContainer{
		EmulatorImage:    testMosquittoImage,
		EmulatorHTTPPort: testMqttBrokerPort,
	})
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
	gcsConsumer, err := consumers.NewGooglePubSubConsumer(ctx, &consumers.GooglePubSubConsumerConfig{
		ProjectID:      testProjectID,
		SubscriptionID: testPubsubSubscriptionID,
	}, pubsubOptions, consumerLogger)
	require.NoError(t, err)

	//ensure service start
	log.Info().Msg("Pausing before ingestion setup")
	time.Sleep(3 * time.Second)

	// --- 2. Configure and Start MQTT Ingestion Service ---
	mqttCfg := &mqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  e2eMqttHTTPPort,
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
	bqCfg := &icinit.IceServiceConfig{
		LogLevel:  "debug",
		HTTPPort:  e2eBqHTTPPort,
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
			5, 4, time.Second * 10,
		},
	}

	gcsLogger := log.With().Str("service", "gcs-processor").Logger()
	gcsClient, gcsCleanUp := emulators.SetupGCSEmulator(t, ctx, emulators.GCSConfig{
		GCImageContainer: emulators.GCImageContainer{
			ImageContainer: emulators.ImageContainer{
				EmulatorImage:    testGCSImage,
				EmulatorHTTPPort: testGCSPORT,
			},
			ProjectID: testProjectID,
		},
		BaseBucket:  "test-bucket",
		BaseStorage: "/storage/v1/b",
	})
	defer gcsCleanUp()

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{BatchSize: 4, FlushTimeout: time.Second * 3},
		icestore.GCSBatchUploaderConfig{BucketName: testGCSBucket, ObjectPrefix: "archived-data"},
		gcsLogger,
	)

	gcsService, err := icestore.NewIceStorageService(2, gcsConsumer, batcher, icestore.ArchivalTransformer, gcsLogger)
	require.NoError(t, err)

	gcsServer := icinit.NewServer(bqCfg, gcsService, gcsLogger)

	go func() {
		if err := gcsServer.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("BigQueryConfig Processing server failed: %v", err)
		}
	}()
	defer gcsServer.Shutdown()

	time.Sleep(3 * time.Second) // Give services time to start and connect

	// --- 4. Publish a Test Message to MQTT ---
	mqttTestPublisher, err := emulators.CreateTestMqttPublisher(mqttBrokerURL, "e2e-test-publisher")
	require.NoError(t, err)
	defer mqttTestPublisher.Disconnect(250)

	const messageCount = 1
	testPayload := types.GardenMonitorReadings{
		DE:       testMqttDeviceUID,
		Sequence: 123,
		Battery:  98,
	}
	msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &testPayload})
	require.NoError(t, err)
	publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceUID, 1)
	token := mqttTestPublisher.Publish(publishTopic, 1, false, msgBytes)
	token.Wait()
	require.NoError(t, token.Error())
	log.Info().Msg("E2E: Published test message to MQTT.")

	// --- 5. Verify Data in BigQueryConfig using the correct polling method ---
	var receivedRows []types.GardenMonitorReadings

	verifyCtx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	// Verification logic
	require.Eventually(t, func() bool {
		bucket := gcsClient.Bucket(testGCSBucket)
		objects, err := listGCSObjectAttrs(verifyCtx, bucket)
		if err != nil {
			t.Logf("Verification failed to list objects, will retry: %v", err)
			return false
		}

		// SIMPLIFIED: Only check the number of objects, not their content.
		return assert.Len(t, objects, 1, "Incorrect number of files created")

	}, 15*time.Second, 500*time.Millisecond, "GCS verification failed")

	// --- 6. Final Assertions ---
	require.Len(t, receivedRows, messageCount)
	assert.Equal(t, testPayload.DE, receivedRows[0].DE)
	assert.Equal(t, testPayload.Sequence, receivedRows[0].Sequence)
	assert.Equal(t, testPayload.Battery, receivedRows[0].Battery)
	log.Info().Msg("E2E: Verification successful!")
}

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
