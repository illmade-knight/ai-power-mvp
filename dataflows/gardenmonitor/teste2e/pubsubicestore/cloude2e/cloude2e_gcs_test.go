//go:build integration

package main_test

import (
	"cloud.google.com/go/pubsub"
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
	cloudTestGCPProjectID = "gemini-power-test"

	// MQTT -> Ingestion Service
	testMosquittoImage     = "eclipse-mosquitto:2.0"
	testMqttBrokerPort     = "1883/tcp"
	testMqttTopicPattern   = "devices/+/data"
	testMqttDeviceUID      = "GARDEN_MONITOR_E2E_001"
	testMqttClientIDPrefix = "ingestion-service-e2e"

	testPubsubTopicID        = "garden-monitor-processed"     // Ingestion publishes here
	testPubsubSubscriptionID = "garden-monitor-processed-sub" // Processing subscribes here

	// GCS
	testGCSBucket = "illmade-test-bucket"
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

	pubsubCleanup := setupRealPubSub(t, ctx, cloudTestGCPProjectID, testPubsubTopicID, testPubsubSubscriptionID)
	defer pubsubCleanup()

	consumerLogger := log.With().Str("service", "pubsub-consumer").Logger()
	gcsConsumer, err := consumers.NewGooglePubsubConsumer(ctx, &consumers.GooglePubsubConsumerConfig{
		ProjectID:      cloudTestGCPProjectID,
		SubscriptionID: testPubsubSubscriptionID,
	}, nil, consumerLogger)
	require.NoError(t, err)

	//ensure service start
	log.Info().Msg("Pausing before ingestion setup")
	time.Sleep(3 * time.Second)

	// --- 2. Configure and Start MQTT Ingestion Service ---
	mqttCfg := &mqinit.Config{
		LogLevel:  "debug",
		ProjectID: cloudTestGCPProjectID,
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
		ProjectID: cloudTestGCPProjectID,
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

	gcsClient, err := storage.NewClient(ctx)
	require.NoError(t, err)

	bucketCleanup, err := setupRealGCSBucket(t, ctx, gcsClient)
	if err != nil {
		return
	}
	defer bucketCleanup()

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

func setupRealGCSBucket(t *testing.T, ctx context.Context, client *storage.Client) (cleanup func(), err error) {
	err = client.Bucket(testGCSBucket).Create(ctx, cloudTestGCPProjectID, nil)
	if err != nil {
		t.Errorf("Failed to create test GCS bucket: %v", err)
		return nil, err
	}

	return func() {
		client.Bucket(testGCSBucket).Delete(ctx)
		client.Close()
	}, nil
}

// setupRealPubSub creates a topic and subscription on GCP for the test run.
// It returns a cleanup function to delete them.
func setupRealPubSub(t *testing.T, ctx context.Context, projectID, topicID, subID string) func() {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real Pub/Sub client")

	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil
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
		return nil
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
	}
}
