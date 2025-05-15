//go:build integration

package ingestion

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testcontainers "github.com/testcontainers/testcontainers-go"
)

const (
	// Mosquitto Test Config
	testMosquittoImage = "eclipse-mosquitto:2.0" // Or your preferred version
	testMqttBrokerPort = "1883/tcp"              // Default MQTT port
	testMqttTopic      = "test/devices/+/up"
	testDeviceEUI      = "INTTEST001"

	// Pub/Sub Emulator Test Config
	// Using the official gcloud emulators image
	testPubSubEmulatorImage  = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort   = "8085/tcp" // Default port for Pub/Sub emulator
	testPubSubProjectID      = "test-project"
	testPubSubTopicID        = "enriched-device-messages"
	testPubSubSubscriptionID = "test-subscription-for-ingestion"
)

// Helper to create a simple Paho publisher client for sending test messages.
func createTestPublisherClient(brokerURL string, clientID string, logger zerolog.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetConnectTimeout(5 * time.Second)
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		logger.Error().Err(err).Str("client_id", clientID).Msg("Test publisher lost connection")
	}
	opts.OnConnect = func(client mqtt.Client) {
		logger.Info().Str("client_id", clientID).Msg("Test publisher connected")
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(5*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test publisher failed to connect: %w", token.Error())
	}
	return client, nil
}

// This function would ideally use testcontainers-go to manage the Mosquitto lifecycle.
func setupMosquittoContainer(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper() // Marks this function as a test helper

	confDir, err := os.Getwd() // Gets current working directory of the test execution
	require.NoError(t, err, "Failed to get current working directory")
	hostConfPath := filepath.Join(confDir, "testdata", "mosquitto.conf")

	// Check if the configuration file exists
	_, err = os.Stat(hostConfPath)
	if os.IsNotExist(err) {
		t.Fatalf("Mosquitto configuration file not found at %s. Please create it with content:\nlistener 1883\nallow_anonymous true\nprotocol mqtt", hostConfPath)
	}
	require.NoError(t, err, "Error checking mosquitto.conf file")

	// Define the container request
	containerReq := testcontainers.ContainerRequest{
		Image:        testMosquittoImage,
		ExposedPorts: []string{testMqttBrokerPort},
		WaitingFor:   wait.ForListeningPort(testMqttBrokerPort).WithStartupTimeout(60 * time.Second),
		// Mount the custom mosquitto.conf to allow anonymous access
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      hostConfPath,
				ContainerFilePath: "/mosquitto/config/mosquitto.conf",
				FileMode:          0o644, // Standard file mode
			},
		},
		// Command to ensure Mosquitto uses the mounted configuration file
		Cmd: []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}

	// Create and start the container
	mosquittoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: containerReq,
		Started:          true, // Start the container automatically
	})
	require.NoError(t, err, "Failed to start Mosquitto container")

	// Get the host and mapped port for the container
	host, err := mosquittoContainer.Host(ctx)
	require.NoError(t, err, "Failed to get Mosquitto container host")

	port, err := mosquittoContainer.MappedPort(ctx, testMqttBrokerPort)
	require.NoError(t, err, "Failed to get Mosquitto container mapped port")

	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container started, listening on: %s", brokerURL)

	// Define the cleanup function to terminate the container
	cleanupFunc = func() {
		t.Log("Terminating Mosquitto container...")
		if err := mosquittoContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Mosquitto container: %v", err)
		}
	}

	return brokerURL, cleanupFunc
}

// setupPubSubEmulator starts a Google Cloud Pub/Sub emulator container.
func setupPubSubEmulator(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImage,
		ExposedPorts: []string{testPubSubEmulatorPort},
		// The command to start the Pub/Sub emulator.
		// Host and port need to be 0.0.0.0 to be accessible from the host.
		Cmd:        []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPort, "/")[0])},
		WaitingFor: wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Pub/Sub emulator container")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testPubSubEmulatorPort)
	require.NoError(t, err)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator container started, listening on: %s", emulatorHost)

	// Create the topic and subscription on the emulator after it starts
	// This requires a pubsub client configured to talk to the emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost) // Temporarily set for this client
	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

	adminClient, err := pubsub.NewClient(ctx, testPubSubProjectID) // No need for option.WithEndpoint with PUBSUB_EMULATOR_HOST set
	require.NoError(t, err, "Failed to create Pub/Sub admin client for emulator")
	defer adminClient.Close()

	topic := adminClient.Topic(testPubSubTopicID)
	exists, err := topic.Exists(ctx)
	require.NoError(t, err, "Failed to check if topic exists on emulator")
	if !exists {
		_, err = adminClient.CreateTopic(ctx, testPubSubTopicID)
		require.NoError(t, err, "Failed to create topic on emulator")
		t.Logf("Created Pub/Sub topic '%s' on emulator", testPubSubTopicID)
	} else {
		t.Logf("Pub/Sub topic '%s' already exists on emulator", testPubSubTopicID)
	}

	// Create subscription for pulling messages in the test
	sub := adminClient.Subscription(testPubSubSubscriptionID)
	exists, err = sub.Exists(ctx)
	require.NoError(t, err, "Failed to check if subscription exists on emulator")
	if !exists {
		_, err = adminClient.CreateSubscription(ctx, testPubSubSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
		require.NoError(t, err, "Failed to create subscription on emulator")
		t.Logf("Created Pub/Sub subscription '%s' on emulator", testPubSubSubscriptionID)
	} else {
		t.Logf("Pub/Sub subscription '%s' already exists on emulator", testPubSubSubscriptionID)
	}

	return emulatorHost, func() {
		t.Log("Terminating Pub/Sub emulator container...")
		require.NoError(t, container.Terminate(ctx))
	}
}

// mockMetadataFetcher for integration tests
func integrationMockFetcher(t *testing.T, expectedEUI, clientID, locID, category string, errToReturn error) DeviceMetadataFetcher {
	return func(deviceEUI string) (string, string, string, error) {
		if deviceEUI == expectedEUI {
			return clientID, locID, category, errToReturn
		}
		t.Logf("Integration mock fetcher called with unexpected EUI: %s (expected %s)", deviceEUI, expectedEUI)
		return "", "", "", ErrMetadataNotFound // Or a more specific error for unexpected EUI
	}
}

func TestIngestionService_Integration_ReceiveAndProcessMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Overall test timeout
	defer cancel()

	// 1. Setup Mosquitto Container
	brokerURL, mosquittoCleanupFunc := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanupFunc()

	pubsubEmulatorHost, pubsubEmulatorCleanupFunc := setupPubSubEmulator(t, ctx)
	defer pubsubEmulatorCleanupFunc()

	// 2. Setup Environment Variables for IngestionService
	// Using t.Setenv (Go 1.17+) is preferred as it auto-cleans up.
	// For older Go or if not available, use os.Setenv and manually unset.

	t.Setenv("MQTT_BROKER_URL", brokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopic)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", "int-test-ingestion-")

	// Pub/Sub (to connect IngestionService's publisher to the emulator)
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectID)
	t.Setenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES", testPubSubTopicID)
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "") // Use emulator, no real creds needed

	// 3. Initialize IngestionService
	var logBuf bytes.Buffer
	// Use a level that shows enough detail for debugging integration issues.
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	mqttCfg, err := LoadMQTTClientConfigFromEnv()
	require.NoError(t, err, "Failed to load MQTT config from env for service")

	pubsubCfg, err := LoadGooglePubSubPublisherConfigFromEnv()
	require.NoError(t, err, "Failed to load Pub/Sub config for service")

	// Create the real GooglePubSubPublisher, it will use PUBSUB_EMULATOR_HOST
	realPublisher, err := NewGooglePubSubPublisher(ctx, pubsubCfg, logger.With().Str("component", "RealPubSubPublisher").Logger())
	require.NoError(t, err, "Failed to create real GooglePubSubPublisher for emulator")
	// defer realPublisher.Stop() // Service's Stop will call publisher.Stop()

	serviceCfg := DefaultIngestionServiceConfig()
	serviceCfg.NumProcessingWorkers = 1 // Keep it simple for this test
	serviceCfg.InputChanCapacity = 10

	mockedClientID := "TestClient123"
	mockedLocationID := "TestLocationXYZ"
	mockedCategory := "IntegrationSensor"
	fetcher := integrationMockFetcher(t, testDeviceEUI, mockedClientID, mockedLocationID, mockedCategory, nil)

	mqttCfg.InsecureSkipVerify = true
	service := NewIngestionService(fetcher, realPublisher, logger, serviceCfg, mqttCfg)
	err = service.Start() // Start the service, which includes connecting to MQTT
	require.NoError(t, err, "IngestionService failed to start")
	defer service.Stop()

	// Give the service a moment to connect and subscribe
	// A more robust way is to wait for a "subscribed" log message or use Paho's OnSubscribe handler.
	time.Sleep(2 * time.Second) // Adjust as needed

	// 4. Create a Test Publisher Client
	log.Debug().Msg("creating test publisher client")
	publisher, err := createTestPublisherClient(brokerURL, "test-publisher-client", logger)
	require.NoError(t, err, "Failed to create test publisher client")
	defer publisher.Disconnect(250)

	// 5. Create Test Pub/Sub Subscriber (to pull from emulator)
	// PUBSUB_EMULATOR_HOST is already set for this test's environment
	subClient, err := pubsub.NewClient(ctx, testPubSubProjectID)
	require.NoError(t, err, "Failed to create Pub/Sub subscriber client for emulator")
	defer subClient.Close()
	subscription := subClient.Subscription(testPubSubSubscriptionID)

	// 5. Publish a Test Message
	sampleMsgTime := time.Now().UTC().Truncate(time.Second)
	testPayload := fmt.Sprintf("Integration test payload at %s", sampleMsgTime.Format(time.RFC3339))

	// Construct the MQTTMessage struct that the service expects after JSON parsing
	sourceMsg := MQTTMessage{
		DeviceInfo:       DeviceInfo{DeviceEUI: testDeviceEUI},
		RawPayload:       testPayload,
		MessageTimestamp: sampleMsgTime,
		LoRaWAN: LoRaWANData{
			RSSI: -65, SNR: 8.2, ReceivedAt: sampleMsgTime.Add(-1 * time.Second),
		},
	}
	msgBytes, err := json.Marshal(sourceMsg)
	require.NoError(t, err, "Failed to marshal test MQTTMessage to JSON")

	// Publish to a specific sub-topic that matches the wildcard subscription
	publishTopic := strings.Replace(testMqttTopic, "+", testDeviceEUI, 1) // e.g., test/devices/INTTEST001/up
	token := publisher.Publish(publishTopic, 1, false, msgBytes)
	token.WaitTimeout(2 * time.Second)
	require.NoError(t, token.Error(), "Test publisher failed to publish message")
	logger.Info().Str("topic", publishTopic).Msg("Test message published")

	// 7. Pull and Verify Message from Pub/Sub Emulator
	var receivedEnrichedMsg EnrichedMessage
	pullCtx, pullCancel := context.WithTimeout(ctx, 20*time.Second) // Timeout for pulling message
	defer pullCancel()

	err = subscription.Receive(pullCtx, func(ctx context.Context, msg *pubsub.Message) {
		logger.Info().Str("pubsub_msg_id", msg.ID).Int("data_len", len(msg.Data)).Msg("Received message from Pub/Sub emulator")
		msg.Ack() // Acknowledge the message
		err := json.Unmarshal(msg.Data, &receivedEnrichedMsg)
		if err != nil {
			t.Errorf("Failed to unmarshal message from Pub/Sub: %v. Data: %s", err, string(msg.Data))
			// Don't call t.FailNow or t.Fatal here as it's in a goroutine from Receive.
			// Instead, signal error through a channel or check receivedEnrichedMsg afterwards.
		}
		pullCancel() // Stop receiving after one message
	})

	if err != nil && !errors.Is(err, context.Canceled) { // context.Canceled is expected after pullCancel()
		t.Fatalf("Pub/Sub Receive error: %v. Logs:\n%s", err, logBuf.String())
	}

	// Check if pullCtx was cancelled because a message was processed or if it timed out
	if pullCtx.Err() == context.DeadlineExceeded {
		t.Fatalf("Timeout waiting for message from Pub/Sub emulator. Logs:\n%s", logBuf.String())
	}

	require.NotZero(t, receivedEnrichedMsg.DeviceEUI, "Enriched message appears to be empty/not unmarshalled")
	assert.Equal(t, testDeviceEUI, receivedEnrichedMsg.DeviceEUI)
	assert.Equal(t, testPayload, receivedEnrichedMsg.RawPayload)
	assert.Equal(t, mockedClientID, receivedEnrichedMsg.ClientID)
	assert.True(t, sourceMsg.MessageTimestamp.Equal(receivedEnrichedMsg.OriginalMQTTTime))
	assert.False(t, receivedEnrichedMsg.IngestionTimestamp.IsZero())

	logs := logBuf.String()
	assert.Contains(t, logs, "GooglePubSubPublisher initialized successfully")
	assert.Contains(t, logs, "Message published successfully to Pub/Sub")
	t.Logf("Full flow integration test completed. Logs:\n%s", logs)
}

// Add more integration tests:
// - Test with malformed JSON published to MQTT.
// - Test with device EUI that results in metadata fetch error.
// - Test connection loss and reconnection (more complex, might require manipulating the Docker container or network).
// - Test behavior when RawMessagesChan is full (if you want to simulate backpressure).
