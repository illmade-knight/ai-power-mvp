//go:build integration

package converter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/mqtttopubsub/converter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Test Constants ---
const (
	// Mosquitto
	testMosquittoImage      = "eclipse-mosquitto:2.0"
	testMqttBrokerPort      = "1883/tcp"
	testMqttTopicPattern    = "test/devices/ingestion_svc/+/up" // Topic pattern for the service to subscribe to
	testMqttDeviceEUI       = "INTTESTDEVICE01"
	testMqttClientIDPrefix  = "int-test-ingest-"
	testMqttPublisherPrefix = "int-test-mqtt-pub-"

	// Pub/Sub Emulator
	testPubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort  = "8085/tcp"
	testPubSubProjectID     = "integration-test-project"
	// Single topic for processed messages
	testPubSubTopicIDProcessed        = "processed-device-messages"
	testPubSubSubscriptionIDProcessed = "test-sub-processed"
	envVarPubSubTopicProcessed        = "PROCESSED_MESSAGES_TOPIC_ID" // Env var for the service
)

var testLogger zerolog.Logger

func init() {
	// Initialize a logger for tests. To see full logs, change Nop() to New().
	testLogger = zerolog.Nop()
}

// --- Test Setup Helpers ---

// createTestMqttPublisherClient creates an MQTT client for publishing test messages.
func createTestMqttPublisherClient(brokerURL string, clientID string, logger zerolog.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			logger.Error().Err(err).Str("client_id", clientID).Msg("Test MQTT publisher connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			logger.Info().Str("client_id", clientID).Msg("Test MQTT publisher connected")
		})
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(15*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test mqtt publisher Connect(): %w", token.Error())
	}
	return client, nil
}

// setupMosquittoContainer starts a Mosquitto container.
func setupMosquittoContainer(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	mosquittoConfContent := `
persistence false
listener 1883
allow_anonymous true
`
	tempDir := t.TempDir()
	confPath := filepath.Join(tempDir, "mosquitto.conf")
	err := os.WriteFile(confPath, []byte(mosquittoConfContent), 0644)
	require.NoError(t, err, "Failed to write temporary mosquitto.conf")

	req := testcontainers.ContainerRequest{
		Image:        testMosquittoImage,
		ExposedPorts: []string{testMqttBrokerPort},
		WaitingFor:   wait.ForListeningPort(testMqttBrokerPort).WithStartupTimeout(60 * time.Second),
		Files: []testcontainers.ContainerFile{
			{HostFilePath: confPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf", FileMode: 0o644},
		},
		Cmd: []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Mosquitto container")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testMqttBrokerPort)
	require.NoError(t, err)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container started, broker URL: %s", brokerURL)

	return brokerURL, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Mosquitto container: %v", err)
		}
	}
}

// setupPubSubEmulator starts a Pub/Sub emulator and creates topics/subscriptions.
func setupPubSubEmulator(t *testing.T, ctx context.Context, projectID, topicID, subID string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImage,
		ExposedPorts: []string{testPubSubEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPort, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Pub/Sub emulator")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testPubSubEmulatorPort)
	require.NoError(t, err)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator started, host: %s", emulatorHost)

	originalEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	defer os.Setenv("PUBSUB_EMULATOR_HOST", originalEmulatorHost)

	adminClient, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create admin Pub/Sub client for emulator")
	defer adminClient.Close()

	psTopic := adminClient.Topic(topicID)
	exists, _ := psTopic.Exists(ctx)
	if !exists {
		_, err = adminClient.CreateTopic(ctx, topicID)
		require.NoError(t, err, "Failed to create topic %s on emulator", topicID)
	}

	psSub := adminClient.Subscription(subID)
	exists, _ = psSub.Exists(ctx)
	if !exists {
		_, err = adminClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: psTopic, AckDeadline: 10 * time.Second})
		require.NoError(t, err, "Failed to create subscription %s for topic %s on emulator", subID, topicID)
	}

	return emulatorHost, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Pub/Sub emulator: %v", err)
		}
	}
}

// commonTestSetup is a helper to reduce boilerplate in test cases.
func commonTestSetup(t *testing.T, ctx context.Context) (string, string, func()) {
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulator(t, ctx,
		testPubSubProjectID,
		testPubSubTopicIDProcessed, testPubSubSubscriptionIDProcessed,
	)

	cleanup := func() {
		pubsubEmulatorCleanup()
		mosquittoCleanup()
	}

	return mqttBrokerURL, pubsubEmulatorHost, cleanup
}

func TestIngestionService_Integration_MQTT_To_PubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mqttBrokerURL, pubsubEmulatorHost, cleanup := commonTestSetup(t, ctx)
	defer cleanup()

	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicPattern)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", testMqttClientIDPrefix)
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectID)
	t.Setenv(envVarPubSubTopicProcessed, testPubSubTopicIDProcessed)

	serviceLogger := testLogger.With().Str("component", "IngestionService").Logger()
	mqttCfg, err := converter.LoadMQTTClientConfigFromEnv()
	require.NoError(t, err)
	pubsubCfg, err := converter.LoadGooglePubSubPublisherConfigFromEnv(envVarPubSubTopicProcessed)
	require.NoError(t, err)
	publisher, err := converter.NewGooglePubSubPublisher(ctx, pubsubCfg, serviceLogger)
	require.NoError(t, err)
	defer publisher.Stop()

	service := converter.NewIngestionService(publisher, serviceLogger, converter.DefaultIngestionServiceConfig(), mqttCfg)

	err = service.Start()
	require.NoError(t, err, "IngestionService.Start() returned an error")
	defer service.Stop()

	time.Sleep(2 * time.Second)

	mqttTestPubClient, err := createTestMqttPublisherClient(mqttBrokerURL, testMqttPublisherPrefix+"main", testLogger)
	require.NoError(t, err)
	defer mqttTestPubClient.Disconnect(250)

	subClient, err := pubsub.NewClient(ctx, testPubSubProjectID)
	require.NoError(t, err)
	defer subClient.Close()
	processedSub := subClient.Subscription(testPubSubSubscriptionIDProcessed)

	t.Run("PublishAndReceiveMessage", func(t *testing.T) {
		sampleMsgTime := time.Now().UTC().Truncate(time.Second)
		payload := fmt.Sprintf("Integration test payload at %s", sampleMsgTime.Format(time.RFC3339))
		sourceMqttMsg := converter.MQTTMessage{
			DeviceInfo:       converter.DeviceInfo{DeviceEUI: testMqttDeviceEUI},
			RawPayload:       payload,
			MessageTimestamp: sampleMsgTime,
		}
		msgBytes, err := json.Marshal(sourceMqttMsg)
		require.NoError(t, err)
		publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceEUI, 1)
		token := mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
		token.Wait()
		require.NoError(t, token.Error())

		pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
		defer pullCancel()
		var receivedPayload converter.MQTTMessage // CORRECTED: Changed back to MQTTMessage
		err = processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			msg.Ack()
			errUnmarshal := json.Unmarshal(msg.Data, &receivedPayload)
			require.NoError(t, errUnmarshal)
			pullCancel()
		})
		require.True(t, err == nil || errors.Is(err, context.Canceled), "Receive returned an unexpected error: %v", err)

		// CORRECTED: Assertions now match the MQTTMessage struct fields
		assert.Equal(t, testMqttDeviceEUI, receivedPayload.DeviceInfo.DeviceEUI, "DeviceEUI mismatch")
		assert.Equal(t, payload, receivedPayload.RawPayload, "RawPayload mismatch")
		assert.Equal(t, publishTopic, receivedPayload.Topic, "Topic in received message mismatch")
		assert.NotEmpty(t, receivedPayload.MessageID, "MessageID in received message should be populated")
	})
}

// NEW TEST CASE
func TestIngestionService_GracefulShutdown_DrainsBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	mqttBrokerURL, pubsubEmulatorHost, cleanup := commonTestSetup(t, ctx)
	defer cleanup()

	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicPattern)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", testMqttClientIDPrefix)
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectID)
	t.Setenv(envVarPubSubTopicProcessed, testPubSubTopicIDProcessed)

	serviceLogger := testLogger.With().Str("component", "IngestionServiceShutdownTest").Logger()
	mqttCfg, err := converter.LoadMQTTClientConfigFromEnv()
	require.NoError(t, err)
	pubsubCfg, err := converter.LoadGooglePubSubPublisherConfigFromEnv(envVarPubSubTopicProcessed)
	require.NoError(t, err)
	publisher, err := converter.NewGooglePubSubPublisher(ctx, pubsubCfg, serviceLogger)
	require.NoError(t, err)
	defer publisher.Stop()

	// Use a small channel capacity to ensure we test backpressure and buffer draining effectively
	serviceCfg := converter.IngestionServiceConfig{
		InputChanCapacity:    100,
		NumProcessingWorkers: 10,
	}
	service := converter.NewIngestionService(publisher, serviceLogger, serviceCfg, mqttCfg)

	err = service.Start()
	require.NoError(t, err, "IngestionService.Start() returned an error")
	// Note: We will call service.Stop() manually in this test, not with defer.

	time.Sleep(2 * time.Second) // Give time for subscriptions to establish.

	mqttTestPubClient, err := createTestMqttPublisherClient(mqttBrokerURL, testMqttPublisherPrefix+"shutdown", testLogger)
	require.NoError(t, err)
	defer mqttTestPubClient.Disconnect(250)

	subClient, err := pubsub.NewClient(ctx, testPubSubProjectID)
	require.NoError(t, err)
	defer subClient.Close()
	processedSub := subClient.Subscription(testPubSubSubscriptionIDProcessed)

	// --- Test Logic ---

	const messagesToSendBeforeShutdown = 50
	publishTopic := strings.Replace(testMqttTopicPattern, "+", "SHUTDOWN_TEST_DEV", 1)

	// 1. Publish a batch of messages that will sit in the service's buffer.
	t.Logf("Publishing %d messages before shutdown...", messagesToSendBeforeShutdown)
	for i := 0; i < messagesToSendBeforeShutdown; i++ {
		payload := fmt.Sprintf("Message %d", i)
		sourceMqttMsg := converter.MQTTMessage{RawPayload: payload, DeviceInfo: converter.DeviceInfo{DeviceEUI: "SHUTDOWN_TEST_DEV"}}
		msgBytes, _ := json.Marshal(sourceMqttMsg)
		token := mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
		token.Wait()
		require.NoError(t, token.Error())
	}
	t.Log("Finished publishing initial batch.")

	// 2. Start a goroutine to receive messages and count them.
	var receivedCount atomic.Int32
	var wgReceive sync.WaitGroup
	wgReceive.Add(1)

	receiveCtx, receiveCancel := context.WithCancel(ctx)
	defer receiveCancel()

	go func() {
		defer wgReceive.Done()
		errRcv := processedSub.Receive(receiveCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			msg.Ack()
			receivedCount.Add(1)
		})
		if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
			t.Logf("Pub/Sub receiver error: %v", errRcv)
		}
	}()

	// Give a very short time for some messages to start flowing, but not all.
	time.Sleep(100 * time.Millisecond)

	// 3. Initiate the graceful shutdown.
	t.Log("--- INITIATING GRACEFUL SHUTDOWN ---")
	shutdownDone := make(chan struct{})
	go func() {
		service.Stop()
		close(shutdownDone)
	}()

	// 4. While shutting down, try to publish more messages. These should be dropped.
	t.Log("Attempting to publish messages during shutdown (these should be dropped)...")
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("Dropped Message %d", i)
		sourceMqttMsg := converter.MQTTMessage{RawPayload: payload}
		msgBytes, _ := json.Marshal(sourceMqttMsg)
		// We don't check for errors here as the connection might be closed.
		mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
		time.Sleep(10 * time.Millisecond)
	}

	// 5. Wait for the shutdown process to fully complete.
	select {
	case <-shutdownDone:
		t.Log("--- GRACEFUL SHUTDOWN COMPLETE ---")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for service.Stop() to complete")
	}

	// 6. Wait a bit longer to ensure all in-flight messages have reached the Pub/Sub receiver.
	// We check the count periodically until it stabilizes or we time out.
	var finalCount int32
	for i := 0; i < 10; i++ {
		finalCount = receivedCount.Load()
		if finalCount == messagesToSendBeforeShutdown {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	receiveCancel()  // Stop the receiver goroutine.
	wgReceive.Wait() // Wait for it to exit cleanly.

	// 7. Assert that exactly the right number of messages were processed.
	assert.EqualValues(t, messagesToSendBeforeShutdown, receivedCount.Load(), "Should have received all messages sent before shutdown, and none sent during")
}
