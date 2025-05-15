//go:build integration

package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
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
	testMosquittoImage = "eclipse-mosquitto:2.0" // Or your preferred version
	testMqttBrokerPort = "1883/tcp"              // Default MQTT port
	testMqttTopic      = "test/devices/+/up"
	testDeviceEUI      = "INTTEST001"
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

	// Manual/External Setup: Assume Mosquitto is running externally and reachable.
	// You MUST ensure this is the case if not using testcontainers.
	// For CI, you might use Docker Compose or a service in your pipeline.
	/*
		manualBrokerURL := "tcp://localhost:1883" // Adjust if your external Mosquitto is elsewhere

		t.Logf("Assuming Mosquitto is running externally at: %s", manualBrokerURL)
		t.Log("For reliable integration tests, consider using testcontainers-go.")

		// Ping the assumed broker to see if it's there
		pingClient, err := createTestPublisherClient(manualBrokerURL, "ping-client", zerolog.Nop())
		if err != nil {
			t.Fatalf("Failed to ping assumed external Mosquitto at %s: %v. Ensure a broker is running and accessible.", manualBrokerURL, err)
		}
		pingClient.Disconnect(100)


		return manualBrokerURL, func() {
			t.Log("Cleanup for externally managed Mosquitto is manual.")
		}

	*/
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

	// 2. Setup Environment Variables for IngestionService
	// Using t.Setenv (Go 1.17+) is preferred as it auto-cleans up.
	// For older Go or if not available, use os.Setenv and manually unset.

	t.Setenv("MQTT_BROKER_URL", brokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopic)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", "int-test-ingestion-")

	// 3. Initialize IngestionService
	var logBuf bytes.Buffer
	// Use a level that shows enough detail for debugging integration issues.
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	mqttCfg, err := LoadMQTTClientConfigFromEnv()
	require.NoError(t, err, "Failed to load MQTT config from env for service")

	serviceCfg := DefaultIngestionServiceConfig()
	serviceCfg.NumProcessingWorkers = 1 // Keep it simple for this test

	mockedClientID := "TestClient123"
	mockedLocationID := "TestLocationXYZ"
	mockedCategory := "IntegrationSensor"
	fetcher := integrationMockFetcher(t, testDeviceEUI, mockedClientID, mockedLocationID, mockedCategory, nil)

	mqttCfg.InsecureSkipVerify = true
	service := NewIngestionService(fetcher, logger, serviceCfg, mqttCfg)
	err = service.Start() // Start the service, which includes connecting to MQTT
	require.NoError(t, err, "IngestionService failed to start")
	defer service.Stop()

	// Give the service a moment to connect and subscribe
	// A more robust way is to wait for a "subscribed" log message or use Paho's OnSubscribe handler.
	time.Sleep(500 * time.Millisecond) // Adjust as needed

	// 4. Create a Test Publisher Client
	log.Debug().Msg("creating test publisher client")
	publisher, err := createTestPublisherClient(brokerURL, "test-publisher-client", logger)
	require.NoError(t, err, "Failed to create test publisher client")
	defer publisher.Disconnect(250)

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

	// 6. Verify Outcome
	var receivedEnrichedMsg *EnrichedMessage
	select {
	case receivedEnrichedMsg = <-service.EnrichedMessagesChan:
		logger.Info().Interface("enriched_msg", receivedEnrichedMsg).Msg("Received enriched message from service")
	case err := <-service.ErrorChan:
		t.Fatalf("IngestionService ErrorChan received an error: %v. Logs:\n%s", err, logBuf.String())
	case <-time.After(5 * time.Second): // Generous timeout for message to go through
		t.Fatalf("Timeout waiting for enriched message from IngestionService. Logs:\n%s", logBuf.String())
	}

	require.NotNil(t, receivedEnrichedMsg, "Enriched message should not be nil")
	assert.Equal(t, testDeviceEUI, receivedEnrichedMsg.DeviceEUI, "DeviceEUI mismatch")
	assert.Equal(t, testPayload, receivedEnrichedMsg.RawPayload, "RawPayload mismatch")
	assert.Equal(t, mockedClientID, receivedEnrichedMsg.ClientID, "ClientID mismatch")
	assert.Equal(t, mockedLocationID, receivedEnrichedMsg.LocationID, "LocationID mismatch")
	assert.Equal(t, mockedCategory, receivedEnrichedMsg.DeviceCategory, "DeviceCategory mismatch")
	assert.True(t, sourceMsg.MessageTimestamp.Equal(receivedEnrichedMsg.OriginalMQTTTime), "OriginalMQTTTime mismatch")
	assert.False(t, receivedEnrichedMsg.IngestionTimestamp.IsZero(), "IngestionTimestamp should be set")

	// Check logs for specific messages if needed
	// For example, check if the Paho client connected and subscribed
	logs := logBuf.String()
	assert.Contains(t, logs, "Paho client connected to MQTT broker", "Missing Paho connect log")
	assert.Contains(t, logs, "Successfully subscribed to MQTT topic", "Missing Paho subscribe log")
	assert.Contains(t, logs, "Paho client received message", "Missing Paho message received log")
	assert.Contains(t, logs, "Successfully enriched MQTT message", "Missing enrichment success log")

	t.Logf("Integration test completed successfully. Logs:\n%s", logs)
}

// Add more integration tests:
// - Test with malformed JSON published to MQTT.
// - Test with device EUI that results in metadata fetch error.
// - Test connection loss and reconnection (more complex, might require manipulating the Docker container or network).
// - Test behavior when RawMessagesChan is full (if you want to simulate backpressure).
