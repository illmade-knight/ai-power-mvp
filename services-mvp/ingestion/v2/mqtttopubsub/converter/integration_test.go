//go:build integration

package converter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/mqtttopubsub/converter"
	"github.com/testcontainers/testcontainers-go"
	"os"
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
	// Initialize a logger for tests. Adjust output/level as needed.
	// testLogger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.DebugLevel)
	testLogger = zerolog.Nop() // Disable logging for cleaner test output by default
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
	// Create a minimal mosquitto.conf if not providing one via bind mount
	// For this test, allow anonymous and set a listener.
	mosquittoConfContent := `
persistence false
listener 1883
allow_anonymous true
`
	// Create a temporary directory for the config
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
		} else {
			t.Log("Mosquitto container terminated.")
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

	// Set PUBSUB_EMULATOR_HOST for the admin client creating topics/subs
	originalEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	defer os.Setenv("PUBSUB_EMULATOR_HOST", originalEmulatorHost) // Restore after setup

	adminClient, err := pubsub.NewClient(ctx, projectID) // Uses PUBSUB_EMULATOR_HOST
	require.NoError(t, err, "Failed to create admin Pub/Sub client for emulator")
	defer adminClient.Close()

	psTopic := adminClient.Topic(topicID)
	exists, errExists := psTopic.Exists(ctx)
	require.NoError(t, errExists, "Error checking if topic %s exists", topicID)
	if !exists {
		_, err = adminClient.CreateTopic(ctx, topicID)
		require.NoError(t, err, "Failed to create topic %s on emulator", topicID)
		t.Logf("Created Pub/Sub topic '%s' on emulator", topicID)
	} else {
		t.Logf("Pub/Sub topic '%s' already exists on emulator", topicID)
	}

	psSub := adminClient.Subscription(subID)
	exists, errExists = psSub.Exists(ctx)
	require.NoError(t, errExists, "Error checking if subscription %s exists", subID)
	if !exists {
		_, err = adminClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: psTopic, AckDeadline: 10 * time.Second})
		require.NoError(t, err, "Failed to create subscription %s for topic %s on emulator", subID, topicID)
		t.Logf("Created Pub/Sub subscription '%s' for topic '%s' on emulator", subID, topicID)
	} else {
		t.Logf("Pub/Sub subscription '%s' already exists on emulator", subID)
	}

	return emulatorHost, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate Pub/Sub emulator: %v", err)
		} else {
			t.Log("Pub/Sub emulator terminated.")
		}
	}
}

func TestIngestionService_Integration_MQTT_To_PubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Test timeout
	defer cancel()

	// --- 1. Setup Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulator(t, ctx,
		testPubSubProjectID,
		testPubSubTopicIDProcessed, testPubSubSubscriptionIDProcessed,
	)
	defer pubsubEmulatorCleanup()

	// --- 2. Setup Environment Variables for IngestionService ---
	// These are used by LoadMQTTClientConfigFromEnv and LoadGooglePubSubPublisherConfigFromEnv
	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicPattern) // The service will subscribe to this
	t.Setenv("MQTT_CLIENT_ID_PREFIX", testMqttClientIDPrefix)
	// Optional MQTT vars, set to empty if not used by your default Mosquitto config
	t.Setenv("MQTT_USERNAME", "")
	t.Setenv("MQTT_PASSWORD", "")
	t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "60")
	t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "10")

	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost) // For the service's PubSub client
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectID)
	t.Setenv(envVarPubSubTopicProcessed, testPubSubTopicIDProcessed) // For LoadGooglePubSubPublisherConfigFromEnv
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")                      // Ensure ADC isn't used with emulator

	// --- 3. Initialize IngestionService Components ---
	serviceLogger := testLogger.With().Str("component", "IngestionService").Logger()

	mqttCfg, err := converter.LoadMQTTClientConfigFromEnv()
	require.NoError(t, err, "Failed to load MQTT client config from env")

	pubsubCfg, err := converter.LoadGooglePubSubPublisherConfigFromEnv(envVarPubSubTopicProcessed)
	require.NoError(t, err, "Failed to load Pub/Sub publisher config from env")

	// Create publisher first (needs PUBSUB_EMULATOR_HOST to be set for NewClient via env)
	publisher, err := converter.NewGooglePubSubPublisher(ctx, pubsubCfg, serviceLogger.With().Str("subcomponent", "PubSubPublisher").Logger())
	require.NoError(t, err, "Failed to create GooglePubSubPublisher")
	defer publisher.Stop()

	ingestionServiceCfg := converter.DefaultIngestionServiceConfig()
	// You can customize ingestionServiceCfg here if needed, e.g., worker count for tests
	// ingestionServiceCfg.NumProcessingWorkers = 1

	service := converter.NewIngestionService(publisher, serviceLogger, ingestionServiceCfg, mqttCfg)

	// --- 4. Start the IngestionService ---
	serviceErrChan := make(chan error, 1)
	go func() {
		t.Log("Starting IngestionService in goroutine...")
		if startErr := service.Start(); startErr != nil {
			serviceErrChan <- fmt.Errorf("IngestionService.Start() failed: %w", startErr)
		}
		close(serviceErrChan) // Close channel to signal completion if no error
		t.Log("IngestionService Start() goroutine finished.")
	}()
	defer service.Stop() // Ensure service is stopped

	// Wait for the service to start up or error out
	// Check service.Start() error first
	select {
	case errStart, ok := <-serviceErrChan:
		if ok && errStart != nil { // Check if channel sent an error and is not just closed
			require.NoError(t, errStart, "IngestionService.Start() returned an error")
		}
		// If channel is closed without error, Start() completed successfully
	case <-time.After(20 * time.Second): // Generous startup timeout
		t.Fatal("Timeout waiting for IngestionService.Start() to complete")
	}
	t.Log("IngestionService started or Start() call completed.")
	time.Sleep(2 * time.Second) // Give a moment for MQTT subscriptions to establish

	// --- 5. Setup Test MQTT Publisher ---
	mqttTestPubClient, err := createTestMqttPublisherClient(mqttBrokerURL, testMqttPublisherPrefix+"main", testLogger)
	require.NoError(t, err, "Failed to create test MQTT publisher")
	defer mqttTestPubClient.Disconnect(250)

	// --- 6. Setup Test Pub/Sub Subscriber ---
	// Client for subscriptions needs to use the same PUBSUB_EMULATOR_HOST, directly or via env var
	// Here, NewClient will pick up PUBSUB_EMULATOR_HOST set earlier if not overridden by options
	subClient, err := pubsub.NewClient(ctx, testPubSubProjectID, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err, "Failed to create Pub/Sub client for subscriptions")
	defer subClient.Close()

	processedSub := subClient.Subscription(testPubSubSubscriptionIDProcessed)

	// --- 7. Publish Test Message and Verify Reception ---
	t.Run("PublishAndReceiveMessage", func(t *testing.T) {
		sampleMsgTime := time.Now().UTC().Truncate(time.Second)
		payload := fmt.Sprintf("Integration test payload at %s", sampleMsgTime.Format(time.RFC3339))
		//messageID := fmt.Sprintf("int-msg-%d", time.Now().UnixNano())

		// The service parses this structure from MQTT
		sourceMqttMsg := converter.MQTTMessage{
			DeviceInfo: converter.DeviceInfo{DeviceEUI: testMqttDeviceEUI},
			RawPayload: payload,
			// Topic and MessageID for the converter.MQTTMessage struct will be populated
			// by the service from the actual MQTT message topic and transport-level ID.
			// The JSON payload sent over MQTT typically doesn't contain these.
			// However, the service expects RawPayload and DeviceInfo in the JSON.
			// Let's ensure the JSON we send matches what ParseMQTTMessage expects.
			// ParseMQTTMessage expects DeviceInfo and RawPayload primarily from the JSON body.
			// MessageID and Topic in the struct are set by the service *after* parsing.
			MessageTimestamp: sampleMsgTime, // This field is part of the JSON for ParseMQTTMessage
		}
		msgBytes, err := json.Marshal(sourceMqttMsg)
		require.NoError(t, err, "Failed to marshal source MQTT message")

		// Construct the actual MQTT topic the service expects
		publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceEUI, 1)

		token := mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
		if !token.WaitTimeout(10 * time.Second) {
			require.Fail(t, "MQTT Publish token timed out")
		}
		require.NoError(t, token.Error(), "MQTT Publish failed")
		t.Logf("Published MQTT message to topic %s for device %s", publishTopic, testMqttDeviceEUI)

		var receivedPayload converter.MQTTMessage // Expecting the same structure
		pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
		defer pullCancel()

		var wgReceive sync.WaitGroup
		wgReceive.Add(1)
		var receiveErr error

		go func() {
			defer wgReceive.Done()
			errRcv := processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
				t.Logf("Received message on PROCESSED Pub/Sub topic: MessageID %s, Data: %s", msg.ID, string(msg.Data))
				msg.Ack() // Acknowledge the message
				errUnmarshal := json.Unmarshal(msg.Data, &receivedPayload)
				if errUnmarshal != nil {
					t.Errorf("Failed to unmarshal processed message: %v. Data: %s", errUnmarshal, string(msg.Data))
					// Don't cancel pull on unmarshal error, let timeout handle it if no valid msg comes
					return
				}
				// If unmarshal is successful, we can cancel further receiving for this test case
				pullCancel()
			})
			// Only assign error if it's not due to context cancellation (which we trigger on success)
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) && !strings.Contains(errRcv.Error(), "context canceled") {
				receiveErr = errRcv
				t.Logf("ProcessedSub.Receive error: %v", errRcv)
			}
		}()
		wgReceive.Wait() // Wait for Receive goroutine to finish (either by message or timeout)

		if receiveErr != nil {
			// If pullCtx was cancelled by timeout, receiveErr might be context.DeadlineExceeded
			// If pullCancel was called due to successful message processing, receiveErr should ideally be nil or context.Canceled
			assert.NoError(t, receiveErr, fmt.Sprintf("Error receiving from processedSub. Service logs might provide clues. Last ErrorChan: %v", service.ErrorChan)) // TODO: Better way to get service errors
		}

		// Assertions on the received message
		require.NotEmpty(t, receivedPayload.DeviceInfo.DeviceEUI, "Received Pub/Sub message is empty or EUI is missing")
		assert.Equal(t, testMqttDeviceEUI, receivedPayload.DeviceInfo.DeviceEUI, "DeviceEUI mismatch")
		assert.Equal(t, payload, receivedPayload.RawPayload, "RawPayload mismatch")
		// The Topic and MessageID in the receivedPayload struct should be populated by the service
		// based on the actual MQTT topic it subscribed to and the transport message ID.
		// The original `sourceMqttMsg.Topic` and `sourceMqttMsg.MessageID` are not part of the JSON payload.
		// So, we verify that they are populated by the service.
		assert.Equal(t, publishTopic, receivedPayload.Topic, "Topic in received message mismatch") // service copies topic from InMessage
		// MessageID from Paho will be a number, service converts it to string.
		// The `sourceMqttMsg.MessageID` ("int-msg-...") was just for the JSON struct, not the actual transport ID.
		// We can't easily assert the Paho transport message ID here without more complex mocking or inspection.
		// For now, checking it's non-empty is a basic validation.
		assert.NotEmpty(t, receivedPayload.MessageID, "MessageID in received message should be populated by the service")

		// Compare timestamps (handle potential minor discrepancies if any)
		assert.WithinDuration(t, sampleMsgTime, receivedPayload.MessageTimestamp, time.Second, "MessageTimestamp mismatch")
	})

	t.Log("Ingestion service integration test completed.")
}
