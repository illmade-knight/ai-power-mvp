//go:build integration

package mqttconverter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/extractors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"

	"github.com/illmade-knight/ai-power-mpv/pkg/mqttconverter"
)

// --- Test Constants ---
var (
	testLogger                        zerolog.Logger
	testMqttTopicPattern              = "devices/+/data"
	testMqttDeviceEUI                 = "test-eui-001"
	testMqttClientIDPrefix            = "ingestion-service-test-"
	testMqttPublisherPrefix           = "test-publisher-"
	testPubSubProjectID               = "test-project"
	testPubSubTopicIDProcessed        = "processed-topic"
	testPubSubSubscriptionIDProcessed = "processed-sub"
	envVarPubSubTopicProcessed        = "PUBSUB_TOPIC_ID_PROCESSED"
)

// --- Test Setup Helpers ---

func init() {
	testLogger = zerolog.Nop() // Disable logging for cleaner test output by default
}

// createTestMqttPublisherClient creates an MQTT client for publishing test messages.
func createTestMqttPublisherClient(brokerURL string, clientID string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second)
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
		Image:        "eclipse-mosquitto:2.0",
		ExposedPorts: []string{"1883/tcp"},
		WaitingFor:   wait.ForListeningPort("1883/tcp").WithStartupTimeout(60 * time.Second),
		Files: []testcontainers.ContainerFile{
			{HostFilePath: confPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf", FileMode: 0o644},
		},
		Cmd: []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Mosquitto container")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "1883/tcp")
	require.NoError(t, err)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container started, broker URL: %s", brokerURL)

	return brokerURL, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Mosquitto container: %v", err)
		}
	}
}

// setupPubSubEmulator starts a Google Cloud Pub/Sub emulator container.
func setupPubSubEmulator(t *testing.T, ctx context.Context, projectID string) (host string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", "--project=" + projectID, "--host-port=0.0.0.0:8085"},
		WaitingFor:   wait.ForLog("started").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err = container.Endpoint(ctx, "")
	require.NoError(t, err)
	t.Logf("Pub/Sub emulator started, host: %s", host)

	return host, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Pub/Sub emulator: %v", err)
		}
	}
}

func TestIngestionService_Integration_MQTT_To_PubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// --- 1. Setup Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulator(t, ctx, testPubSubProjectID)
	defer pubsubEmulatorCleanup()

	// --- 2. Setup Environment Variables for Service Components ---
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectID)
	t.Setenv(envVarPubSubTopicProcessed, testPubSubTopicIDProcessed)

	// --- 3. Initialize IngestionService Components ---
	serviceLogger := testLogger.With().Str("component", "IngestionService").Logger()

	mqttCfg := &mqttconverter.MQTTClientConfig{
		BrokerURL:        mqttBrokerURL,
		Topic:            testMqttTopicPattern,
		ClientIDPrefix:   testMqttClientIDPrefix,
		KeepAlive:        30 * time.Second,
		ConnectTimeout:   10 * time.Second,
		ReconnectWaitMax: 1 * time.Minute,
	}

	pubsubCfg, err := mqttconverter.LoadGooglePubSubPublisherConfigFromEnv(envVarPubSubTopicProcessed)
	require.NoError(t, err, "Failed to load Pub/Sub publisher config from env")

	publisher, err := mqttconverter.NewGooglePubSubPublisher(ctx, pubsubCfg, serviceLogger)
	require.NoError(t, err, "Failed to create GooglePubSubPublisher")
	defer publisher.Stop()

	extractor := extractors.NewGardenMonitorExtractor()
	ingestionServiceCfg := mqttconverter.DefaultIngestionServiceConfig()

	service := mqttconverter.NewIngestionService(publisher, extractor, serviceLogger, ingestionServiceCfg, mqttCfg)

	// --- 4. Start the IngestionService ---
	serviceErrChan := make(chan error, 1)
	go func() {
		t.Log("Starting IngestionService in goroutine...")
		if startErr := service.Start(); startErr != nil {
			serviceErrChan <- fmt.Errorf("IngestionService.Start() failed: %w", startErr)
		}
		close(serviceErrChan)
	}()
	defer service.Stop()

	select {
	case err, ok := <-serviceErrChan:
		if ok {
			require.NoError(t, err, "IngestionService.Start() returned an error")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Timeout waiting for IngestionService.Start() to complete")
	}
	t.Log("IngestionService started successfully.")
	time.Sleep(2 * time.Second) // Give a moment for MQTT subscriptions to establish

	// --- 5. Setup Test MQTT Publisher ---
	mqttTestPubClient, err := createTestMqttPublisherClient(mqttBrokerURL, testMqttPublisherPrefix+"main")
	require.NoError(t, err, "Failed to create test MQTT publisher")
	defer mqttTestPubClient.Disconnect(250)

	// --- 6. Setup Test Pub/Sub Subscriber Client & Topic/Sub ---
	// MODIFIED: Use a single client for all Pub/Sub test operations.
	subClient, err := pubsub.NewClient(ctx, testPubSubProjectID, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err, "Failed to create Pub/Sub client for subscriptions")
	defer subClient.Close()

	// MODIFIED: Create the topic and subscription using the single test client.
	topic, err := subClient.CreateTopic(ctx, testPubSubTopicIDProcessed)
	require.NoError(t, err, "Failed to create Pub/Sub topic for test")
	_, err = subClient.CreateSubscription(ctx, testPubSubSubscriptionIDProcessed, pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err, "Failed to create Pub/Sub subscription for test")

	processedSub := subClient.Subscription(testPubSubSubscriptionIDProcessed)

	// --- 7. Publish Test Message and Verify Reception ---
	t.Run("PublishAndReceiveMessageWithAttributeExtraction", func(t *testing.T) {
		sourcePayload := map[string]interface{}{
			"Timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"Payload": map[string]string{
				"DE":       testMqttDeviceEUI,
				"Sequence": "12345",
			},
		}
		msgBytes, err := json.Marshal(sourcePayload)
		require.NoError(t, err, "Failed to marshal source MQTT message")

		publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceEUI, 1)

		token := mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
		if !token.WaitTimeout(10 * time.Second) {
			require.Fail(t, "MQTT Publish token timed out")
		}
		require.NoError(t, token.Error(), "MQTT Publish failed")
		t.Logf("Published MQTT message to topic %s", publishTopic)

		pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
		defer pullCancel()

		var wgReceive sync.WaitGroup
		wgReceive.Add(1)
		var receiveErr error
		var receivedMsg *pubsub.Message

		go func() {
			defer wgReceive.Done()
			errRcv := processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
				t.Logf("Received message on Pub/Sub topic: ID %s", msg.ID)
				msg.Ack()
				receivedMsg = msg
				pullCancel()
			})
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
				receiveErr = errRcv
			}
		}()
		wgReceive.Wait()

		require.NoError(t, receiveErr, "Error receiving from Pub/Sub subscription")
		require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub")

		assert.Equal(t, msgBytes, receivedMsg.Data, "The received payload should be identical to the sent payload")

		require.NotNil(t, receivedMsg.Attributes, "Received message should have attributes")
		assert.Equal(t, publishTopic, receivedMsg.Attributes["mqtt_topic"], "Attribute 'mqtt_topic' should match the publish topic")
		assert.Equal(t, testMqttDeviceEUI, receivedMsg.Attributes["device_eui"], "Attribute 'device_eui' should be extracted correctly")
	})

	t.Log("Ingestion service integration test completed.")
}
