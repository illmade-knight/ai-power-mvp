//go:build integration

package connectors_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/enrich/connectors"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Constants ---
const (
	// Pub/Sub Emulator
	testPubSubEmulatorImageConnectors = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPortConnectors  = "8085/tcp"
	testPubSubProjectIDConnectors     = "test-project-enrichment" // Unique project ID for this test suite

	// Input Messages Topic & Sub (for messages going INTO EnrichmentService)
	testPubSubTopicIDInputConnectors        = "input-raw-mqtt-connectors"
	testPubSubSubscriptionIDInputConnectors = "test-sub-input-raw-connectors" // Service will listen to this

	// Enriched Messages Topic & Sub (output)
	testPubSubTopicIDEnrichedConnectors        = "enriched-device-connectors"
	testPubSubSubscriptionIDEnrichedConnectors = "test-sub-enriched-connectors" // Test will listen to this

	// Unidentified Messages Topic & Sub (output)
	testPubSubTopicIDUnidentifiedConnectors        = "unidentified-device-connectors"
	testPubSubSubscriptionIDUnidentifiedConnectors = "test-sub-unidentified-connectors" // Test will listen to this

	// Firestore Emulator
	testFirestoreEmulatorImageConnectors     = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testFirestoreEmulatorPortConnectors      = "8080/tcp"
	testFirestoreCollectionDevicesConnectors = "devices-metadata-connectors"

	// Test Devices
	testDeviceEUIKnownConnectors   = "ENRICHKNOWN01"
	testDeviceEUIUnknownConnectors = "ENRICHUNKNOWN02"
)

// --- Test Setup Helpers ---

// setupPubSubEmulatorConnectors starts a Pub/Sub emulator and creates specified topics/subscriptions.
func setupPubSubEmulatorConnectors(t *testing.T, ctx context.Context, projectID string, topicsToCreate map[string]string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImageConnectors,
		ExposedPorts: []string{testPubSubEmulatorPortConnectors},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPortConnectors, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Pub/Sub emulator")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testPubSubEmulatorPortConnectors)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator started, host: %s", emulatorHost)

	// Set PUBSUB_EMULATOR_HOST for admin client and subsequent operations within this test context
	originalEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	defer t.Setenv("PUBSUB_EMULATOR_HOST", originalEmulatorHost) // Restore original value

	// Use options for the admin client to ensure it targets the emulator
	opts := []option.ClientOption{
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		// option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())), // For some environments
	}

	adminClient, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err, "Failed to create admin Pub/Sub client for emulator")
	defer adminClient.Close()

	for topicID, subID := range topicsToCreate {
		topic := adminClient.Topic(topicID)
		exists, errExists := topic.Exists(ctx)
		require.NoError(t, errExists, "Error checking if topic %s exists", topicID)
		if !exists {
			_, err = adminClient.CreateTopic(ctx, topicID)
			require.NoError(t, err, "Failed to create topic %s on emulator", topicID)
			t.Logf("Created Pub/Sub topic '%s' on emulator", topicID)
		} else {
			t.Logf("Pub/Sub topic '%s' already exists on emulator", topicID)
		}

		if subID != "" { // Only create subscription if subID is provided
			sub := adminClient.Subscription(subID)
			exists, errExists = sub.Exists(ctx)
			require.NoError(t, errExists, "Error checking if subscription %s exists", subID)
			if !exists {
				_, err = adminClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic, AckDeadline: 10 * time.Second})
				require.NoError(t, err, "Failed to create subscription %s for topic %s on emulator", subID, topicID)
				t.Logf("Created Pub/Sub subscription '%s' for topic '%s' on emulator", subID, topicID)
			} else {
				t.Logf("Pub/Sub subscription '%s' already exists on emulator", subID)
			}
		}
	}
	return emulatorHost, func() {
		require.NoError(t, container.Terminate(ctx), "Failed to terminate Pub/Sub emulator")
		t.Log("Pub/Sub emulator terminated.")
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
	require.NoError(t, err, "Failed to start Firestore emulator")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testFirestoreEmulatorPortConnectors)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Firestore emulator started, host: %s", emulatorHost)

	originalEmulatorHost := os.Getenv("FIRESTORE_EMULATOR_HOST")
	t.Setenv("FIRESTORE_EMULATOR_HOST", emulatorHost) // For subsequent client creations
	defer t.Setenv("FIRESTORE_EMULATOR_HOST", originalEmulatorHost)

	return emulatorHost, func() {
		require.NoError(t, container.Terminate(ctx), "Failed to terminate Firestore emulator")
		t.Log("Firestore emulator terminated.")
	}
}

// seedFirestoreDeviceDataConnectors seeds device metadata into Firestore.
func seedFirestoreDeviceDataConnectors(t *testing.T, ctx context.Context, projectID, collectionName, deviceEUI, clientID, locationID, category string) {
	t.Helper()
	// Firestore client implicitly uses FIRESTORE_EMULATOR_HOST if set
	opts := []option.ClientOption{
		option.WithEndpoint(os.Getenv("FIRESTORE_EMULATOR_HOST")), // Ensure it uses the set emulator host
		option.WithoutAuthentication(),
		// option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
	fsClient, err := firestore.NewClient(ctx, projectID, opts...)
	require.NoError(t, err, "Failed to create Firestore client for seeding")
	defer fsClient.Close()

	_, err = fsClient.Collection(collectionName).Doc(deviceEUI).Set(ctx, map[string]interface{}{
		"clientID":       clientID,
		"locationID":     locationID,
		"deviceCategory": category,
	})
	require.NoError(t, err, "Failed to seed Firestore for device %s", deviceEUI)
	t.Logf("Seeded Firestore for device %s", deviceEUI)
}

// createTestPubSubPublisherClient creates a Pub/Sub client for publishing test messages to a specific topic.
func createTestPubSubPublisherClient(t *testing.T, ctx context.Context, projectID, topicID, emulatorHost string) (*pubsub.Topic, func()) {
	t.Helper()
	opts := []option.ClientOption{
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
	}
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	require.NoError(t, err, "Failed to create test Pub/Sub publisher client")

	topic := client.Topic(topicID)
	return topic, func() {
		topic.Stop()
		client.Close()
		t.Logf("Test Pub/Sub publisher client for topic %s closed.", topicID)
	}
}

func TestEnrichmentService_Integration_Routing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute) // Overall test timeout
	defer cancel()

	// --- 1. Setup Emulators ---
	pubsubTopicsToCreate := map[string]string{
		testPubSubTopicIDInputConnectors:        testPubSubSubscriptionIDInputConnectors,        // Input topic and service's subscription
		testPubSubTopicIDEnrichedConnectors:     testPubSubSubscriptionIDEnrichedConnectors,     // Output topic and test's subscription
		testPubSubTopicIDUnidentifiedConnectors: testPubSubSubscriptionIDUnidentifiedConnectors, // Output topic and test's subscription
	}
	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorConnectors(t, ctx, testPubSubProjectIDConnectors, pubsubTopicsToCreate)
	defer pubsubEmulatorCleanup()

	firestoreEmulatorHost, firestoreEmulatorCleanup := setupFirestoreEmulatorConnectors(t, ctx, testPubSubProjectIDConnectors)
	defer firestoreEmulatorCleanup()

	// --- 2. Setup Environment Variables for EnrichmentService ---
	// These are used by connectors.InitializeEnrichmentServiceDependencies
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)       // For service's PubSub clients
	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost) // For service's Firestore client

	t.Setenv("GCP_PROJECT_ID", testPubSubProjectIDConnectors)                         // Used by multiple components
	t.Setenv("INPUT_PUBSUB_PROJECT_ID", testPubSubProjectIDConnectors)                // For EnrichmentService input
	t.Setenv("INPUT_PUBSUB_SUBSCRIPTION_ID", testPubSubSubscriptionIDInputConnectors) // For EnrichmentService input

	t.Setenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES", testPubSubTopicIDEnrichedConnectors)
	t.Setenv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES", testPubSubTopicIDUnidentifiedConnectors)
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "") // No creds file when using emulator

	t.Setenv("FIRESTORE_COLLECTION_DEVICES", testFirestoreCollectionDevicesConnectors)
	t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "") // No creds file when using emulator

	// Worker/channel configs (using defaults by not setting them, or set them if needed)
	// t.Setenv("ENRICHMENT_NUM_WORKERS", "2")
	// t.Setenv("ENRICHMENT_INPUT_CHAN_CAPACITY", "10")
	// t.Setenv("ENRICHMENT_MAX_OUTSTANDING_MSGS", "5")

	// --- 3. Seed Firestore Data ---
	knownDeviceClientID := "ClientKnownEnrich"
	knownDeviceLocationID := "LocationKnownEnrich"
	knownDeviceCategory := "SensorKnownEnrich"
	seedFirestoreDeviceDataConnectors(t, ctx, testPubSubProjectIDConnectors, testFirestoreCollectionDevicesConnectors,
		testDeviceEUIKnownConnectors, knownDeviceClientID, knownDeviceLocationID, knownDeviceCategory)

	// --- 4. Initialize EnrichmentService ---
	var logBuf bytes.Buffer // Capture logs from service initialization
	baseLogger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	dependencies, err := connectors.InitializeEnrichmentServiceDependencies(ctx, baseLogger)
	require.NoError(t, err, "connectors.InitializeEnrichmentServiceDependencies failed. Logs:\n%s", logBuf.String())
	require.NotNil(t, dependencies.Service, "EnrichmentService from dependencies is nil")

	// Cleanup for components created by InitializeEnrichmentServiceDependencies
	defer func() {
		// Service itself is stopped by the main defer for 'service'
		if dependencies.EnrichedPublisher != nil {
			dependencies.EnrichedPublisher.Stop()
		}
		if dependencies.UnidentifiedPublisher != nil {
			dependencies.UnidentifiedPublisher.Stop()
		}
		if dependencies.MetadataFetcher != nil {
			errClose := dependencies.MetadataFetcher.Close()
			if errClose != nil {
				t.Logf("Error closing metadata fetcher: %v", errClose)
			}
		}
	}()

	// --- 5. Start the EnrichmentService ---
	service := dependencies.Service
	serviceErrChan := make(chan error, 1)
	go func() {
		t.Log("Starting connectors.EnrichmentService in goroutine...")
		// Start should be non-blocking or manage its own goroutines if it's long-running
		// For this test, we assume Start() sets up listeners and returns, or errors quickly.
		// If Start() is blocking, this test structure might need adjustment.
		errStart := service.Start()
		if errStart != nil {
			serviceErrChan <- errStart
		}
		// No close(serviceErrChan) here as it's only for the initial Start error.
		t.Log("connectors.EnrichmentService Start() invoked.")
	}()
	defer service.Stop() // Ensure service is stopped at the end of the test

	// Wait for the service to start up or error out
	select {
	case errStart := <-serviceErrChan:
		require.NoError(t, errStart, "connectors.EnrichmentService.Start() returned an error. Logs:\n%s", logBuf.String())
	case <-time.After(15 * time.Second): // Increased startup timeout
		t.Logf("No immediate error from service.Start(), assuming startup in progress. Logs so far:\n%s", logBuf.String())
		// Allow some time for the service's Pub/Sub receiver to connect and start listening.
		// This is crucial as there's no explicit "ready" signal from service.Start().
		time.Sleep(5 * time.Second)
	}
	t.Log("connectors.EnrichmentService assumed started.")

	// --- 6. Setup Test Pub/Sub Publisher for Input Messages ---
	// This client publishes to the topic the EnrichmentService is subscribed to.
	inputTopicPublisher, cleanupInputPublisher := createTestPubSubPublisherClient(t, ctx, testPubSubProjectIDConnectors, testPubSubTopicIDInputConnectors, pubsubEmulatorHost)
	defer cleanupInputPublisher()

	// --- 7. Setup Test Pub/Sub Subscribers for Output Topics ---
	subClientCtx, subClientCancel := context.WithTimeout(ctx, 60*time.Second) // Context for subscriber client
	defer subClientCancel()

	// Client for subscriptions needs to use the same PUBSUB_EMULATOR_HOST
	subClientOpts := []option.ClientOption{
		option.WithEndpoint(pubsubEmulatorHost),
		option.WithoutAuthentication(),
	}
	subClient, err := pubsub.NewClient(subClientCtx, testPubSubProjectIDConnectors, subClientOpts...)
	require.NoError(t, err, "Failed to create Pub/Sub client for test subscriptions")
	defer subClient.Close()

	enrichedSub := subClient.Subscription(testPubSubSubscriptionIDEnrichedConnectors)
	unidentifiedSub := subClient.Subscription(testPubSubSubscriptionIDUnidentifiedConnectors)

	// --- Scenario 1: Publish message for KNOWN device to INPUT Pub/Sub topic ---
	t.Run("KnownDevice_EnrichedAndPublishedToEnrichedTopic", func(t *testing.T) {
		sampleMsgTimeKnown := time.Now().UTC().Truncate(time.Millisecond) // Use millisecond for better time comparison
		payloadKnown := fmt.Sprintf("PayloadKnownDeviceConnectors-%s", sampleMsgTimeKnown.Format(time.RFC3339Nano))
		// This is the MQTTMessage structure that the EnrichmentService expects as input
		sourceMqttMsgKnown := connectors.MQTTMessage{
			DeviceInfo:       connectors.DeviceInfo{DeviceEUI: testDeviceEUIKnownConnectors},
			RawPayload:       payloadKnown,
			MessageTimestamp: sampleMsgTimeKnown, // This should be carried through
			LoRaWAN:          connectors.LoRaWANData{ReceivedAt: sampleMsgTimeKnown.Add(-5 * time.Second)},
		}
		msgBytesKnown, errMarshal := json.Marshal(sourceMqttMsgKnown)
		require.NoError(t, errMarshal, "Failed to marshal source MQTT message for known device")

		// Publish to the INPUT topic
		publishResultKnown := inputTopicPublisher.Publish(ctx, &pubsub.Message{Data: msgBytesKnown})
		_, errPublishKnown := publishResultKnown.Get(ctx) // Wait for publish to complete
		require.NoError(t, errPublishKnown, "Pub/Sub Publish to input topic for known device failed")
		t.Logf("Published message to INPUT Pub/Sub topic for KNOWN device: %s", testDeviceEUIKnownConnectors)

		var receivedEnrichedMsg connectors.EnrichedMessage
		pullCtxKnown, pullCancelKnown := context.WithTimeout(ctx, 45*time.Second) // Increased timeout for Pub/Sub receive
		defer pullCancelKnown()
		var wgReceiveKnown sync.WaitGroup
		wgReceiveKnown.Add(1)
		var receiveErrKnown error

		go func() {
			defer wgReceiveKnown.Done()
			errRcv := enrichedSub.Receive(pullCtxKnown, func(ctxMsg context.Context, msg *pubsub.Message) {
				t.Logf("Received message on ENRICHED topic: MessageID %s, Data: %s", msg.ID, string(msg.Data))
				msg.Ack() // Ack the message!
				errUnmarshal := json.Unmarshal(msg.Data, &receivedEnrichedMsg)
				if errUnmarshal != nil {
					receiveErrKnown = fmt.Errorf("failed to unmarshal enriched message: %w. Data: %s", errUnmarshal, string(msg.Data))
				}
				pullCancelKnown() // Stop receiving after one message
			})
			// Check for errors from Receive itself, but not context.Canceled if pullCancelKnown was called.
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) && !strings.Contains(errRcv.Error(), "context canceled") {
				receiveErrKnown = fmt.Errorf("enrichedSub.Receive returned error: %w", errRcv)
			}
		}()
		wgReceiveKnown.Wait()
		require.NoError(t, receiveErrKnown, "Error during enrichedSub.Receive or processing. Logs:\n%s", logBuf.String())
		require.NotEmpty(t, receivedEnrichedMsg.DeviceEUI, "Received enriched message is empty. Logs:\n%s", logBuf.String())

		assert.Equal(t, testDeviceEUIKnownConnectors, receivedEnrichedMsg.DeviceEUI)
		assert.Equal(t, payloadKnown, receivedEnrichedMsg.RawPayload)
		assert.Equal(t, knownDeviceClientID, receivedEnrichedMsg.ClientID)
		assert.Equal(t, knownDeviceLocationID, receivedEnrichedMsg.LocationID)
		assert.Equal(t, knownDeviceCategory, receivedEnrichedMsg.DeviceCategory)
		assert.Equal(t, sourceMqttMsgKnown.MessageTimestamp.UTC(), receivedEnrichedMsg.OriginalMQTTTime.UTC(), "OriginalMQTTTime mismatch")
		assert.Equal(t, sourceMqttMsgKnown.LoRaWAN.ReceivedAt.UTC(), receivedEnrichedMsg.LoRaWANReceivedAt.UTC(), "LoRaWANReceivedAt mismatch")
		assert.NotZero(t, receivedEnrichedMsg.IngestionTimestamp, "IngestionTimestamp should be set")
	})

	// --- Scenario 2: Publish message for UNKNOWN device to INPUT Pub/Sub topic ---
	t.Run("UnknownDevice_RoutedToUnidentifiedTopic", func(t *testing.T) {
		sampleMsgTimeUnknown := time.Now().UTC().Truncate(time.Millisecond)
		payloadUnknown := fmt.Sprintf("PayloadUnknownDeviceConnectors-%s", sampleMsgTimeUnknown.Format(time.RFC3339Nano))
		sourceMqttMsgUnknown := connectors.MQTTMessage{
			DeviceInfo:       connectors.DeviceInfo{DeviceEUI: testDeviceEUIUnknownConnectors},
			RawPayload:       payloadUnknown,
			MessageTimestamp: sampleMsgTimeUnknown,
		}
		msgBytesUnknown, errMarshal := json.Marshal(sourceMqttMsgUnknown)
		require.NoError(t, errMarshal, "Failed to marshal source MQTT message for unknown device")

		// Publish to the INPUT topic
		publishResultUnknown := inputTopicPublisher.Publish(ctx, &pubsub.Message{Data: msgBytesUnknown})
		_, errPublishUnknown := publishResultUnknown.Get(ctx) // Wait for publish to complete
		require.NoError(t, errPublishUnknown, "Pub/Sub Publish to input topic for unknown device failed")
		t.Logf("Published message to INPUT Pub/Sub topic for UNKNOWN device: %s", testDeviceEUIUnknownConnectors)

		var receivedUnidentifiedMsg connectors.UnidentifiedDeviceMessage
		pullCtxUnknown, pullCancelUnknown := context.WithTimeout(ctx, 45*time.Second) // Increased timeout
		defer pullCancelUnknown()
		var wgReceiveUnknown sync.WaitGroup
		wgReceiveUnknown.Add(1)
		var receiveErrUnknown error

		go func() {
			defer wgReceiveUnknown.Done()
			errRcv := unidentifiedSub.Receive(pullCtxUnknown, func(ctxMsg context.Context, msg *pubsub.Message) {
				t.Logf("Received message on UNIDENTIFIED topic: MessageID %s, Data: %s", msg.ID, string(msg.Data))
				msg.Ack() // Ack the message!
				errUnmarshal := json.Unmarshal(msg.Data, &receivedUnidentifiedMsg)
				if errUnmarshal != nil {
					receiveErrUnknown = fmt.Errorf("failed to unmarshal unidentified message: %w. Data: %s", errUnmarshal, string(msg.Data))
				}
				pullCancelUnknown() // Stop receiving after one message
			})
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) && !strings.Contains(errRcv.Error(), "context canceled") {
				receiveErrUnknown = fmt.Errorf("unidentifiedSub.Receive returned error: %w", errRcv)
			}
		}()
		wgReceiveUnknown.Wait()
		require.NoError(t, receiveErrUnknown, "Error during unidentifiedSub.Receive or processing. Logs:\n%s", logBuf.String())
		require.NotEmpty(t, receivedUnidentifiedMsg.DeviceEUI, "Received unidentified message is empty. Logs:\n%s", logBuf.String())

		assert.Equal(t, testDeviceEUIUnknownConnectors, receivedUnidentifiedMsg.DeviceEUI)
		assert.Equal(t, payloadUnknown, receivedUnidentifiedMsg.RawPayload)
		assert.Equal(t, connectors.ErrMetadataNotFound.Error(), receivedUnidentifiedMsg.ProcessingError) // Assuming ErrMetadataNotFound is exported
		assert.Equal(t, sourceMqttMsgUnknown.MessageTimestamp.UTC(), receivedUnidentifiedMsg.OriginalMQTTTime.UTC(), "OriginalMQTTTime mismatch for unidentified")
		assert.NotZero(t, receivedUnidentifiedMsg.IngestionTimestamp, "IngestionTimestamp should be set for unidentified")
	})

	t.Logf("EnrichmentService integration test completed. Final service logs (if any):\n%s", logBuf.String())
}
