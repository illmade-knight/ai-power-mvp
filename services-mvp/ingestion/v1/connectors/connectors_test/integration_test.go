//go:build integration

package connectors_test // Changed package to connectors_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	// Import the package being tested (connectors)
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/connectors" // Replace with your actual module path
)

// --- Constants (Copied from your uploaded integration_test.go, consider centralizing if used elsewhere) ---
const (
	// Mosquitto
	testMosquittoImageConnectors   = "eclipse-mosquitto:2.0"
	testMqttBrokerPortConnectors   = "1883/tcp"
	testMqttTopicConnectors        = "test/devices/connectors_pkg/+/up" // Unique topic for this test
	testDeviceEUIKnownConnectors   = "CONTESTKNOWN01"
	testDeviceEUIUnknownConnectors = "CONTESTUNKNOWN02"

	// Pub/Sub Emulator
	testPubSubEmulatorImageConnectors = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPortConnectors  = "8085/tcp"
	testPubSubProjectIDConnectors     = "test-project-connectors"
	// Enriched Messages Topic & Sub
	testPubSubTopicIDEnrichedConnectors        = "enriched-device-connectors"
	testPubSubSubscriptionIDEnrichedConnectors = "test-sub-enriched-connectors"
	// Unidentified Messages Topic & Sub
	testPubSubTopicIDUnidentifiedConnectors        = "unidentified-device-connectors"
	testPubSubSubscriptionIDUnidentifiedConnectors = "test-sub-unidentified-connectors"

	// Firestore Emulator
	testFirestoreEmulatorImageConnectors     = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testFirestoreEmulatorPortConnectors      = "8080/tcp"
	testFirestoreCollectionDevicesConnectors = "devices-metadata-connectors"
)

// --- Test Setup Helpers (Copied and adapted from your uploaded integration_test.go) ---

// createTestMqttPublisherClient creates an MQTT client for publishing test messages.
func createTestMqttPublisherClientConnectors(brokerURL string, clientID string, logger zerolog.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, err error) { // Corrected method name
			logger.Error().Err(err).Msg("Test MQTT publisher (connectors) connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) { // Corrected method name
			logger.Info().Msg("Test MQTT publisher (connectors) connected")
		})
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(15*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test mqtt publisher (connectors) Connect(): %w", token.Error())
	}
	return client, nil
}

// setupMosquittoContainerConnectors starts a Mosquitto container.
func setupMosquittoContainerConnectors(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	// Assuming mosquitto.conf is in 'testdata' subdirectory relative to *this connectors_test.go file*
	// or a shared location. For this example, assume it's in 'testdata' next to this test file.
	confDir, err := os.Getwd() // Gets current directory (ingestion/connectors)
	require.NoError(t, err)
	hostConfPath := filepath.Join(confDir, "mosquitto.conf")

	_, err = os.Stat(hostConfPath)
	require.NoError(t, err, "mosquitto.conf not found at %s. Ensure path is correct relative to test execution dir.", hostConfPath)

	req := testcontainers.ContainerRequest{
		Image:        testMosquittoImageConnectors,
		ExposedPorts: []string{testMqttBrokerPortConnectors},
		WaitingFor:   wait.ForListeningPort(testMqttBrokerPortConnectors).WithStartupTimeout(60 * time.Second),
		Files: []testcontainers.ContainerFile{
			{HostFilePath: hostConfPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf", FileMode: 0o644},
		},
		Cmd: []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Mosquitto container (connectors)")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testMqttBrokerPortConnectors)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container (connectors) started, broker URL: %s", brokerURL)
	return brokerURL, func() {
		require.NoError(t, container.Terminate(ctx), "Failed to terminate Mosquitto container (connectors)")
		t.Log("Mosquitto container (connectors) terminated.")
	}
}

// setupPubSubEmulatorConnectors starts a Pub/Sub emulator and creates topics/subscriptions.
func setupPubSubEmulatorConnectors(t *testing.T, ctx context.Context, projectID, enrichedTopicID, enrichedSubID, unidentifiedTopicID, unidentifiedSubID string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImageConnectors,
		ExposedPorts: []string{testPubSubEmulatorPortConnectors},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPortConnectors, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Pub/Sub emulator (connectors)")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testPubSubEmulatorPortConnectors)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator (connectors) started, host: %s", emulatorHost)

	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	adminClient, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create admin Pub/Sub client for emulator (connectors)")
	defer adminClient.Close()

	topicsToCreate := map[string]string{
		enrichedTopicID:     enrichedSubID,
		unidentifiedTopicID: unidentifiedSubID,
	}

	for topicID, subID := range topicsToCreate {
		topic := adminClient.Topic(topicID)
		exists, errExists := topic.Exists(ctx)
		require.NoError(t, errExists, "Error checking if topic %s exists (connectors)", topicID)
		if !exists {
			_, err = adminClient.CreateTopic(ctx, topicID)
			require.NoError(t, err, "Failed to create topic %s on emulator (connectors)", topicID)
			t.Logf("Created Pub/Sub topic '%s' on emulator (connectors)", topicID)
		} else {
			t.Logf("Pub/Sub topic '%s' already exists on emulator (connectors)", topicID)
		}

		sub := adminClient.Subscription(subID)
		exists, errExists = sub.Exists(ctx)
		require.NoError(t, errExists, "Error checking if subscription %s exists (connectors)", subID)
		if !exists {
			_, err = adminClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic, AckDeadline: 10 * time.Second})
			require.NoError(t, err, "Failed to create subscription %s for topic %s on emulator (connectors)", subID, topicID)
			t.Logf("Created Pub/Sub subscription '%s' for topic '%s' on emulator (connectors)", subID, topicID)
		} else {
			t.Logf("Pub/Sub subscription '%s' already exists on emulator (connectors)", subID)
		}
	}
	return emulatorHost, func() {
		require.NoError(t, container.Terminate(ctx), "Failed to terminate Pub/Sub emulator (connectors)")
		t.Log("Pub/Sub emulator (connectors) terminated.")
	}
}

// setupFirestoreEmulatorConnectors starts a Firestore emulator.
func setupFirestoreEmulatorConnectors(t *testing.T, ctx context.Context, projectID string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testFirestoreEmulatorImageConnectors,
		ExposedPorts: []string{testFirestoreEmulatorPortConnectors},
		Cmd:          []string{"gcloud", "beta", "emulators", "firestore", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testFirestoreEmulatorPortConnectors, "/")[0])},
		WaitingFor:   wait.ForLog("Dev App Server is now running").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Firestore emulator (connectors)")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testFirestoreEmulatorPortConnectors)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Firestore emulator (connectors) started, host: %s", emulatorHost)

	return emulatorHost, func() {
		require.NoError(t, container.Terminate(ctx), "Failed to terminate Firestore emulator (connectors)")
		t.Log("Firestore emulator (connectors) terminated.")
	}
}

// seedFirestoreDeviceDataConnectors seeds device metadata into Firestore.
func seedFirestoreDeviceDataConnectors(t *testing.T, ctx context.Context, projectID, collectionName, deviceEUI, clientID, locationID, category string) {
	t.Helper()
	fsClient, err := firestore.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create Firestore client for seeding (connectors)")
	defer fsClient.Close()

	_, err = fsClient.Collection(collectionName).Doc(deviceEUI).Set(ctx, map[string]interface{}{
		"clientID":       clientID,
		"locationID":     locationID,
		"deviceCategory": category,
	})
	require.NoError(t, err, "Failed to seed Firestore for device %s (connectors)", deviceEUI)
	t.Logf("Seeded Firestore for device %s (connectors)", deviceEUI)
}

func TestConnectors_IngestionService_Integration_Routing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// --- 1. Setup Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerConnectors(t, ctx)
	defer mosquittoCleanup()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorConnectors(t, ctx,
		testPubSubProjectIDConnectors,
		testPubSubTopicIDEnrichedConnectors, testPubSubSubscriptionIDEnrichedConnectors,
		testPubSubTopicIDUnidentifiedConnectors, testPubSubSubscriptionIDUnidentifiedConnectors,
	)
	defer pubsubEmulatorCleanup()

	firestoreEmulatorHost, firestoreEmulatorCleanup := setupFirestoreEmulatorConnectors(t, ctx, testPubSubProjectIDConnectors)
	defer firestoreEmulatorCleanup()

	// --- 2. Setup Environment Variables for Connectors ---
	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicConnectors)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", "int-connectors-ingest-")
	t.Setenv("MQTT_USERNAME", "")
	t.Setenv("MQTT_PASSWORD", "")

	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectIDConnectors)
	t.Setenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES", testPubSubTopicIDEnrichedConnectors)
	t.Setenv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES", testPubSubTopicIDUnidentifiedConnectors)
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")

	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost)
	t.Setenv("FIRESTORE_COLLECTION_DEVICES", testFirestoreCollectionDevicesConnectors)
	t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "")

	// --- 3. Seed Firestore Data ---
	knownDeviceClientID := "ClientKnownConn"
	knownDeviceLocationID := "LocationKnownConn"
	knownDeviceCategory := "SensorKnownConn"
	seedFirestoreDeviceDataConnectors(t, ctx, testPubSubProjectIDConnectors, testFirestoreCollectionDevicesConnectors,
		testDeviceEUIKnownConnectors, knownDeviceClientID, knownDeviceLocationID, knownDeviceCategory)

	// --- 4. Initialize Core IngestionService using Shared Setup Function ---
	var logBuf bytes.Buffer
	baseLogger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// Using InitializeAndGetCoreService from the 'connectors' package
	dependencies, err := connectors.InitializeAndGetCoreService(ctx, baseLogger)
	require.NoError(t, err, "connectors.InitializeAndGetCoreService failed")
	require.NotNil(t, dependencies.Service, "CoreService from dependencies is nil")

	// Cleanup for components created by InitializeAndGetCoreService
	defer func() {
		if dependencies.EnrichedPublisher != nil {
			dependencies.EnrichedPublisher.Stop()
		}
		if dependencies.UnidentifiedPublisher != nil {
			dependencies.UnidentifiedPublisher.Stop()
		}
		if dependencies.MetadataFetcher != nil {
			dependencies.MetadataFetcher.Close() // Assuming Close() is the method
		}
		// The core IngestionService's MQTT client is stopped by dependencies.Service.Stop()
	}()

	// --- 5. Start the Core IngestionService ---
	// The service instance is dependencies.Service
	service := dependencies.Service
	serviceErrChan := make(chan error, 1)
	go func() {
		t.Log("Starting connectors.IngestionService in goroutine...")
		serviceErrChan <- service.Start() // Assuming Start() is blocking or manages its own goroutines
		t.Log("connectors.IngestionService Start() goroutine finished.")
	}()
	defer service.Stop() // Ensure service is stopped at the end of the test

	// Wait for the service to start up or error out
	select {
	case errStart := <-serviceErrChan:
		require.NoError(t, errStart, "connectors.IngestionService.Start() failed. Logs:\n%s", logBuf.String())
	case <-time.After(15 * time.Second): // Increased startup timeout
		t.Fatalf("Timeout starting connectors.IngestionService. Logs:\n%s", logBuf.String())
	}
	t.Log("connectors.IngestionService started.")
	// Give a little more time for MQTT subscriptions to establish fully
	time.Sleep(3 * time.Second)

	// --- 6. Setup Test MQTT Publisher ---
	mqttTestPublisher, err := createTestMqttPublisherClientConnectors(mqttBrokerURL, "test-mqtt-pub-connectors", baseLogger)
	require.NoError(t, err, "Failed to create test MQTT publisher (connectors)")
	defer mqttTestPublisher.Disconnect(250)

	// --- 7. Setup Test Pub/Sub Subscribers ---
	subClientCtx, subClientCancel := context.WithTimeout(ctx, 30*time.Second)
	defer subClientCancel()
	// Client for subscriptions needs to use the same PUBSUB_EMULATOR_HOST
	subClient, err := pubsub.NewClient(subClientCtx, testPubSubProjectIDConnectors, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err, "Failed to create Pub/Sub client for subscriptions (connectors)")
	defer subClient.Close()

	enrichedSub := subClient.Subscription(testPubSubSubscriptionIDEnrichedConnectors)
	unidentifiedSub := subClient.Subscription(testPubSubSubscriptionIDUnidentifiedConnectors)

	// --- Scenario 1: Publish message for KNOWN device ---
	t.Run("KnownDeviceGetsEnrichedAndPublished_Connectors", func(t *testing.T) {
		sampleMsgTimeKnown := time.Now().UTC().Truncate(time.Second)
		payloadKnown := "PayloadKnownDeviceConnectors"
		sourceMqttMsgKnown := connectors.MQTTMessage{ // Using connectors.MQTTMessage
			DeviceInfo:       connectors.DeviceInfo{DeviceEUI: testDeviceEUIKnownConnectors},
			RawPayload:       payloadKnown,
			MessageTimestamp: sampleMsgTimeKnown,
		}
		msgBytesKnown, _ := json.Marshal(sourceMqttMsgKnown)
		publishTopicKnown := strings.Replace(testMqttTopicConnectors, "+", testDeviceEUIKnownConnectors, 1)

		tokenKnown := mqttTestPublisher.Publish(publishTopicKnown, 1, false, msgBytesKnown)
		if tokenKnown.WaitTimeout(10*time.Second) && tokenKnown.Error() != nil {
			require.NoError(t, tokenKnown.Error(), "MQTT Publish for known device failed (connectors)")
		}
		t.Logf("Published MQTT message for KNOWN device (connectors): %s", testDeviceEUIKnownConnectors)

		var receivedEnrichedMsg connectors.EnrichedMessage // Using connectors.EnrichedMessage
		pullCtxKnown, pullCancelKnown := context.WithTimeout(ctx, 30*time.Second)
		defer pullCancelKnown()
		var wgReceiveKnown sync.WaitGroup
		wgReceiveKnown.Add(1)
		var receiveErrKnown error

		go func() {
			defer wgReceiveKnown.Done()
			errRcv := enrichedSub.Receive(pullCtxKnown, func(ctxMsg context.Context, msg *pubsub.Message) {
				t.Logf("Received message on ENRICHED topic (connectors): MessageID %s", msg.ID)
				msg.Ack()
				errUnmarshal := json.Unmarshal(msg.Data, &receivedEnrichedMsg)
				if errUnmarshal != nil {
					t.Errorf("Failed to unmarshal enriched message (connectors): %v. Data: %s", errUnmarshal, string(msg.Data))
				}
				pullCancelKnown()
			})
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
				receiveErrKnown = errRcv
				t.Logf("EnrichedSub Receive error (connectors): %v", errRcv)
			}
		}()
		wgReceiveKnown.Wait()
		require.NoError(t, receiveErrKnown, "Error receiving from enrichedSub (connectors). Logs:\n%s", logBuf.String())
		require.NotEmpty(t, receivedEnrichedMsg.DeviceEUI, "Received enriched message is empty (connectors). Logs:\n%s", logBuf.String())

		assert.Equal(t, testDeviceEUIKnownConnectors, receivedEnrichedMsg.DeviceEUI)
		assert.Equal(t, payloadKnown, receivedEnrichedMsg.RawPayload)
		assert.Equal(t, knownDeviceClientID, receivedEnrichedMsg.ClientID)
		assert.Equal(t, knownDeviceLocationID, receivedEnrichedMsg.LocationID)
		assert.Equal(t, knownDeviceCategory, receivedEnrichedMsg.DeviceCategory)
	})

	// --- Scenario 2: Publish message for UNKNOWN device ---
	t.Run("UnknownDeviceGetsRoutedToUnidentifiedTopic_Connectors", func(t *testing.T) {
		sampleMsgTimeUnknown := time.Now().UTC().Truncate(time.Second)
		payloadUnknown := "PayloadUnknownDeviceConnectors"
		sourceMqttMsgUnknown := connectors.MQTTMessage{ // Using connectors.MQTTMessage
			DeviceInfo:       connectors.DeviceInfo{DeviceEUI: testDeviceEUIUnknownConnectors},
			RawPayload:       payloadUnknown,
			MessageTimestamp: sampleMsgTimeUnknown,
		}
		msgBytesUnknown, _ := json.Marshal(sourceMqttMsgUnknown)
		publishTopicUnknown := strings.Replace(testMqttTopicConnectors, "+", testDeviceEUIUnknownConnectors, 1)

		tokenUnknown := mqttTestPublisher.Publish(publishTopicUnknown, 1, false, msgBytesUnknown)
		if tokenUnknown.WaitTimeout(10*time.Second) && tokenUnknown.Error() != nil {
			require.NoError(t, tokenUnknown.Error(), "MQTT Publish for unknown device failed (connectors)")
		}
		t.Logf("Published MQTT message for UNKNOWN device (connectors): %s", testDeviceEUIUnknownConnectors)

		var receivedUnidentifiedMsg connectors.UnidentifiedDeviceMessage // Using connectors.UnidentifiedDeviceMessage
		pullCtxUnknown, pullCancelUnknown := context.WithTimeout(ctx, 30*time.Second)
		defer pullCancelUnknown()
		var wgReceiveUnknown sync.WaitGroup
		wgReceiveUnknown.Add(1)
		var receiveErrUnknown error

		go func() {
			defer wgReceiveUnknown.Done()
			errRcv := unidentifiedSub.Receive(pullCtxUnknown, func(ctxMsg context.Context, msg *pubsub.Message) {
				t.Logf("Received message on UNIDENTIFIED topic (connectors): MessageID %s", msg.ID)
				msg.Ack()
				errUnmarshal := json.Unmarshal(msg.Data, &receivedUnidentifiedMsg)
				if errUnmarshal != nil {
					t.Errorf("Failed to unmarshal unidentified message (connectors): %v. Data: %s", errUnmarshal, string(msg.Data))
				}
				pullCancelUnknown()
			})
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
				receiveErrUnknown = errRcv
				t.Logf("UnidentifiedSub Receive error (connectors): %v", errRcv)
			}
		}()
		wgReceiveUnknown.Wait()
		require.NoError(t, receiveErrUnknown, "Error receiving from unidentifiedSub (connectors). Logs:\n%s", logBuf.String())
		require.NotEmpty(t, receivedUnidentifiedMsg.DeviceEUI, "Received unidentified message is empty (connectors). Logs:\n%s", logBuf.String())

		assert.Equal(t, testDeviceEUIUnknownConnectors, receivedUnidentifiedMsg.DeviceEUI)
		assert.Equal(t, payloadUnknown, receivedUnidentifiedMsg.RawPayload)
		// Assuming connectors.ErrMetadataNotFound is an exported error from your connectors package
		assert.Equal(t, connectors.ErrMetadataNotFound.Error(), receivedUnidentifiedMsg.ProcessingError)
	})

	// No serverRunCancel() here as we are testing the core service directly, not the server wrapper.
	// The service.Stop() in the defer for the main test function handles cleanup.
	t.Logf("Connectors package integration test completed. Final logs (if any):\n%s", logBuf.String())
}
