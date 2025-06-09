//go:build e2eintegration

package converter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/ingestion/converter"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

// --- Test Constants ---
const (
	// Mosquitto - Using a public broker to listen for live, unsolicited messages
	testMqttBrokerURL      = "tcp://broker.emqx.io:1883"
	testMqttTopicPattern   = "garden_monitor/861275073104248" // Topic for a real device
	testMqttDeviceEUI      = "861275073104248"
	testMqttClientIDPrefix = "int-test-ingest-listener-"

	// Pub/Sub Emulator
	testPubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort  = "8085/tcp"
	testPubSubProjectID     = "integration-test-project"
	// Single topic for processed messages
	testPubSubTopicIDProcessed        = "processed-device-messages"
	testPubSubSubscriptionIDProcessed = "test-sub-processed"
	envVarPubSubTopicProcessed        = "PROCESSED_MESSAGES_TOPIC_ID" // Env var for the service
)

// ListenerTestConfig centralizes all time-related configurations for the passive listener test.
type ListenerTestConfig struct {
	// OverallTestTimeout is the maximum duration for the entire test function.
	// This must be longer than MessageWaitTimeout.
	OverallTestTimeout time.Duration
	// ContainerStartupTimeout is the timeout for the Pub/Sub emulator container to start.
	ContainerStartupTimeout time.Duration
	// ServiceStartupTimeout is the maximum time to wait for the IngestionService's Start() method to complete.
	ServiceStartupTimeout time.Duration
	// MessageWaitTimeout is the maximum time the test will passively wait for a message
	// to arrive from the external MQTT broker and be processed. This is the key tuning parameter.
	MessageWaitTimeout time.Duration
}

// defaultConfigForListenerTest provides a standard set of timings for the test.
// Note the long wait times, which are necessary for a passive listener.
func defaultConfigForListenerTest() ListenerTestConfig {
	return ListenerTestConfig{
		// Set a long overall timeout to accommodate the message wait time.
		OverallTestTimeout: 25 * time.Minute,
		// Standard startup timeouts.
		ContainerStartupTimeout: 1 * time.Minute,
		ServiceStartupTimeout:   20 * time.Second,
		// **CRITICAL**: This defines the test's "patience". If no message from the real device
		// is seen within this window, the test will fail. Adjust based on device frequency.
		MessageWaitTimeout: 20 * time.Minute,
	}
}

var testLogger zerolog.Logger

func init() {
	// For a long-running listener test, it can be useful to enable logging.
	testLogger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.InfoLevel)
	// testLogger = zerolog.Nop() // Disable logging for cleaner test output by default
}

// --- Test Setup Helpers ---

// setupPubSubEmulator starts a Pub/Sub emulator and creates topics/subscriptions.
func setupPubSubEmulator(t *testing.T, ctx context.Context, projectID, topicID, subID string, startupTimeout time.Duration) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImage,
		ExposedPorts: []string{testPubSubEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPort, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(startupTimeout),
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
	exists, errExists := psTopic.Exists(ctx)
	require.NoError(t, errExists, "Error checking if topic %s exists", topicID)
	if !exists {
		_, err = adminClient.CreateTopic(ctx, topicID)
		require.NoError(t, err, "Failed to create topic %s on emulator", topicID)
	}

	psSub := adminClient.Subscription(subID)
	exists, errExists = psSub.Exists(ctx)
	require.NoError(t, errExists, "Error checking if subscription %s exists", subID)
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

func TestIngestionService_PassiveListener(t *testing.T) {
	// --- 1. Test Configuration ---
	config := defaultConfigForListenerTest()
	ctx, cancel := context.WithTimeout(context.Background(), config.OverallTestTimeout)
	defer cancel()

	// --- 2. Setup External Services (Pub/Sub Emulator) ---
	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulator(t, ctx,
		testPubSubProjectID,
		testPubSubTopicIDProcessed, testPubSubSubscriptionIDProcessed, config.ContainerStartupTimeout,
	)
	defer pubsubEmulatorCleanup()

	// --- 3. Setup Environment Variables for IngestionService ---
	t.Setenv("MQTT_BROKER_URL", testMqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicPattern)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", testMqttClientIDPrefix)
	t.Setenv("MQTT_USERNAME", "")
	t.Setenv("MQTT_PASSWORD", "")
	t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "60")
	t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "10")

	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectID)
	t.Setenv(envVarPubSubTopicProcessed, testPubSubTopicIDProcessed)
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")

	// --- 4. Initialize and Start the IngestionService ---
	serviceLogger := testLogger.With().Str("component", "IngestionService").Logger()
	mqttCfg, err := converter.LoadMQTTClientConfigFromEnv()
	require.NoError(t, err)
	pubsubCfg, err := converter.LoadGooglePubSubPublisherConfigFromEnv(envVarPubSubTopicProcessed)
	require.NoError(t, err)
	publisher, err := converter.NewGooglePubSubPublisher(ctx, pubsubCfg, serviceLogger)
	require.NoError(t, err)
	defer publisher.Stop()
	service := converter.NewIngestionService(publisher, serviceLogger, converter.DefaultIngestionServiceConfig(), mqttCfg)

	serviceErrChan := make(chan error, 1)
	go func() {
		defer close(serviceErrChan)
		t.Log("Starting IngestionService in goroutine...")
		if startErr := service.Start(); startErr != nil {
			serviceErrChan <- fmt.Errorf("IngestionService.Start() failed: %w", startErr)
		}
	}()
	defer service.Stop()

	select {
	case errStart, ok := <-serviceErrChan:
		if ok {
			require.NoError(t, errStart, "IngestionService.Start() returned an error")
		}
	case <-time.After(config.ServiceStartupTimeout):
		t.Fatal("Timeout waiting for IngestionService.Start() to complete")
	}
	t.Logf("IngestionService started. Passively listening for MQTT messages on topic '%s'.", testMqttTopicPattern)

	// --- 5. Setup Test Subscriber and Wait for a Processed Message ---
	subClient, err := pubsub.NewClient(ctx, testPubSubProjectID, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err, "Failed to create Pub/Sub client for subscriptions")
	defer subClient.Close()
	processedSub := subClient.Subscription(testPubSubSubscriptionIDProcessed)

	var receivedPayload converter.GardenMonitorMessage
	var receiveErr error
	var wgReceive sync.WaitGroup
	wgReceive.Add(1)

	// This context controls how long we'll wait for the message on the Pub/Sub side.
	pullCtx, pullCancel := context.WithTimeout(ctx, config.MessageWaitTimeout)
	defer pullCancel()

	go func() {
		defer wgReceive.Done()
		t.Logf("Waiting up to %v for a processed message on Pub/Sub topic '%s'...", config.MessageWaitTimeout, testPubSubTopicIDProcessed)
		errRcv := processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			t.Logf("Received message on PROCESSED Pub/Sub topic: MessageID %s, Data: %s", msg.ID, string(msg.Data))
			msg.Ack()
			if errUnmarshal := json.Unmarshal(msg.Data, &receivedPayload); errUnmarshal != nil {
				t.Errorf("Failed to unmarshal processed message: %v. Data: %s", errUnmarshal, string(msg.Data))
				return
			}
			// Success! Cancel the context to stop receiving.
			pullCancel()
		})

		// `Receive` returns an error when the context is cancelled. We ignore the cancellation error,
		// as we trigger it ourselves on success. Any other error is a potential problem.
		if errRcv != nil && !errors.Is(errRcv, context.Canceled) && !strings.Contains(errRcv.Error(), "context canceled") {
			receiveErr = errRcv
		}
	}()

	// Wait for the receiver goroutine to finish. It will finish when a message is
	// received (and pullCancel is called) or when the MessageWaitTimeout is hit.
	wgReceive.Wait()

	// --- 6. Assertions ---
	// This will fail with a context deadline exceeded error if no message was received in time.
	require.NoError(t, receiveErr, "The listener did not receive a message within the configured timeout.")

	// Check that the payload is not empty. This also implicitly verifies that the test didn't time out.
	require.NotEmpty(t, receivedPayload.Payload, "Received Pub/Sub message was empty or failed to unmarshal.")

	// Since we don't control the input, we can only do basic validation.
	// We verify the message is for the device we expect, as the service might be subscribed to a wildcard.
	assert.Equal(t, testMqttDeviceEUI, receivedPayload.Payload.DE, "DeviceEUI mismatch in received message")
	assert.NotEmpty(t, receivedPayload.Payload.Sequence, "Sequence (from MQTT MessageID) should have been populated by the service")

	t.Log("Successfully received and verified a live message. Test complete.")
}
