//go:build integration

package ingestion

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
)

// --- Constants ---
const (
	// Mosquitto
	testMosquittoImageFullFlowTyped = "eclipse-mosquitto:2.0"
	testMqttBrokerPortFullFlowTyped = "1883/tcp"
	testMqttTopicFullFlowTyped      = "test/devices/fullflowtyped/+/up"
	testDeviceEUIKnownTyped         = "INTTESTKNOWNTYPED01"
	testDeviceEUIUnknownTyped       = "INTTESTUNKNOWNTYPED02"

	// Pub/Sub Emulator
	testPubSubEmulatorImageFullFlowTyped = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPortFullFlowTyped  = "8085/tcp"
	testPubSubProjectIDFullFlowTyped     = "test-project-fftyped"
	// Enriched Messages Topic & Sub
	testPubSubTopicIDEnrichedTyped        = "enriched-device-fftyped"
	testPubSubSubscriptionIDEnrichedTyped = "test-sub-enriched-fftyped"
	// Unidentified Messages Topic & Sub
	testPubSubTopicIDUnidentifiedTyped        = "unidentified-device-fftyped"
	testPubSubSubscriptionIDUnidentifiedTyped = "test-sub-unidentified-fftyped"

	// Firestore Emulator
	testFirestoreEmulatorImageFullFlowTyped     = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testFirestoreEmulatorPortFullFlowTyped      = "8080/tcp"
	testFirestoreCollectionDevicesFullFlowTyped = "devices-metadata-fftyped"
)

// --- Test Setup Helpers ---
func createTestMqttPublisherClientFullFlowTyped(brokerURL string, clientID string, logger zerolog.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().AddBroker(brokerURL).SetClientID(clientID).SetConnectTimeout(10 * time.Second)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(15*time.Second) && token.Error() != nil {
		return nil, token.Error()
	}
	return client, nil
}

func setupMosquittoContainerFullFlowTyped(t *testing.T, ctx context.Context) (string, func()) {
	confDir, _ := os.Getwd()
	hostConfPath := filepath.Join(confDir, "testdata", "mosquitto.conf")
	_, err := os.Stat(hostConfPath)
	require.NoError(t, err, "mosquitto.conf not found at %s", hostConfPath)
	req := testcontainers.ContainerRequest{
		Image: testMosquittoImageFullFlowTyped, ExposedPorts: []string{testMqttBrokerPortFullFlowTyped},
		WaitingFor: wait.ForListeningPort(testMqttBrokerPortFullFlowTyped).WithStartupTimeout(60 * time.Second),
		Files:      []testcontainers.ContainerFile{{HostFilePath: hostConfPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf", FileMode: 0o644}},
		Cmd:        []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testMqttBrokerPortFullFlowTyped)
	return fmt.Sprintf("tcp://%s:%s", host, port.Port()), func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupPubSubEmulatorFullFlowTyped(t *testing.T, ctx context.Context) (string, func()) {
	req := testcontainers.ContainerRequest{
		Image: testPubSubEmulatorImageFullFlowTyped, ExposedPorts: []string{testPubSubEmulatorPortFullFlowTyped},
		Cmd:        []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testPubSubProjectIDFullFlowTyped), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPortFullFlowTyped, "/")[0])},
		WaitingFor: wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testPubSubEmulatorPortFullFlowTyped)
	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port())

	originalEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	defer os.Setenv("PUBSUB_EMULATOR_HOST", originalEmulatorHost)

	adminClient, err := pubsub.NewClient(ctx, testPubSubProjectIDFullFlowTyped)
	require.NoError(t, err)
	defer adminClient.Close()

	topicsToCreate := map[string]string{
		testPubSubTopicIDEnrichedTyped:     testPubSubSubscriptionIDEnrichedTyped,
		testPubSubTopicIDUnidentifiedTyped: testPubSubSubscriptionIDUnidentifiedTyped,
	}

	for topicID, subID := range topicsToCreate {
		topic := adminClient.Topic(topicID)
		if ex, _ := topic.Exists(ctx); !ex {
			_, err = adminClient.CreateTopic(ctx, topicID)
			require.NoError(t, err, "Failed to create topic %s on emulator", topicID)
			t.Logf("Created Pub/Sub topic '%s' on emulator", topicID)
		}
		sub := adminClient.Subscription(subID)
		if ex, _ := sub.Exists(ctx); !ex {
			_, err = adminClient.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic})
			require.NoError(t, err, "Failed to create subscription %s for topic %s on emulator", subID, topicID)
			t.Logf("Created Pub/Sub subscription '%s' on emulator", subID)
		}
	}
	return emulatorHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupFirestoreEmulatorFullFlowTyped(t *testing.T, ctx context.Context) (string, func()) {
	req := testcontainers.ContainerRequest{
		Image: testFirestoreEmulatorImageFullFlowTyped, ExposedPorts: []string{testFirestoreEmulatorPortFullFlowTyped},
		Cmd:        []string{"gcloud", "beta", "emulators", "firestore", "start", fmt.Sprintf("--project=%s", testPubSubProjectIDFullFlowTyped), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testFirestoreEmulatorPortFullFlowTyped, "/")[0])},
		WaitingFor: wait.ForLog("Dev App Server is now running").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testFirestoreEmulatorPortFullFlowTyped)
	return fmt.Sprintf("%s:%s", host, port.Port()), func() { require.NoError(t, container.Terminate(ctx)) }
}

func seedFirestoreDeviceDataFullFlowTyped(t *testing.T, ctx context.Context, projectID, coll, eui, clientID, locID, cat string) {
	fsClient, err := firestore.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer fsClient.Close()
	_, err = fsClient.Collection(coll).Doc(eui).Set(ctx, map[string]interface{}{
		"clientID": clientID, "locationID": locID, "deviceCategory": cat,
	})
	require.NoError(t, err)
}

func TestIngestionService_Integration_TypedPublisher_Routing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Setup Emulators
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerFullFlowTyped(t, ctx)
	defer mosquittoCleanup()
	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorFullFlowTyped(t, ctx)
	defer pubsubEmulatorCleanup()
	firestoreEmulatorHost, firestoreEmulatorCleanup := setupFirestoreEmulatorFullFlowTyped(t, ctx)
	defer firestoreEmulatorCleanup()

	// 2. Setup Environment Variables
	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicFullFlowTyped)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", "int-typedpub-ingest-")
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testPubSubProjectIDFullFlowTyped)
	t.Setenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES", testPubSubTopicIDEnrichedTyped)
	t.Setenv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES", testPubSubTopicIDUnidentifiedTyped)
	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost)
	t.Setenv("FIRESTORE_COLLECTION_DEVICES", testFirestoreCollectionDevicesFullFlowTyped)
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")
	t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "")

	// 3. Seed Firestore for the KNOWN device
	knownDeviceClientID := "ClientKnownTyped"
	knownDeviceLocationID := "LocationKnownTyped"
	knownDeviceCategory := "SensorKnownTyped"
	seedFirestoreDeviceDataFullFlowTyped(t, ctx, testPubSubProjectIDFullFlowTyped, testFirestoreCollectionDevicesFullFlowTyped,
		testDeviceEUIKnownTyped, knownDeviceClientID, knownDeviceLocationID, knownDeviceCategory)

	// 4. Initialize IngestionService
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	mqttCfg, err := LoadMQTTClientConfigFromEnv()
	require.NoError(t, err)
	enrichedPubCfg, err := LoadGooglePubSubPublisherConfigFromEnv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES")
	require.NoError(t, err)
	unidentifiedPubCfg, err := LoadGooglePubSubPublisherConfigFromEnv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES")
	require.NoError(t, err)
	firestoreFetcherCfg, err := LoadFirestoreFetcherConfigFromEnv()
	require.NoError(t, err)

	// Create two separate GooglePubSubPublisher instances, one for each topic
	enrichedPublisher, err := NewGooglePubSubPublisher(ctx, enrichedPubCfg, logger.With().Str("publisher_type", "enriched").Logger())
	require.NoError(t, err)
	unidentifiedPublisher, err := NewGooglePubSubPublisher(ctx, unidentifiedPubCfg, logger.With().Str("publisher_type", "unidentified").Logger())
	require.NoError(t, err)

	realFetcher, err := NewGoogleDeviceMetadataFetcher(ctx, firestoreFetcherCfg, logger.With().Str("component", "FirestoreFetcher").Logger())
	require.NoError(t, err)
	defer realFetcher.Close()

	serviceCfg := DefaultIngestionServiceConfig()
	service := NewIngestionService(realFetcher.Fetch, enrichedPublisher, unidentifiedPublisher, logger, serviceCfg, mqttCfg)

	serviceErrChan := make(chan error, 1)
	go func() { serviceErrChan <- service.Start() }()
	defer service.Stop()

	select {
	case errStart := <-serviceErrChan:
		require.NoError(t, errStart)
	case <-time.After(35 * time.Second):
		t.Fatalf("Timeout starting IngestionService. Logs:\n%s", logBuf.String())
	}
	time.Sleep(3 * time.Second)

	// 5. Test MQTT Publisher
	mqttTestPublisher, err := createTestMqttPublisherClientFullFlowTyped(mqttBrokerURL, "test-mqtt-pub-typed", logger)
	require.NoError(t, err)
	defer mqttTestPublisher.Disconnect(250)

	// 6. Test Pub/Sub Subscribers
	subClient, err := pubsub.NewClient(ctx, testPubSubProjectIDFullFlowTyped, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer subClient.Close()
	enrichedSub := subClient.Subscription(testPubSubSubscriptionIDEnrichedTyped)
	unidentifiedSub := subClient.Subscription(testPubSubSubscriptionIDUnidentifiedTyped)

	// --- Scenario 1: Publish message for KNOWN device ---
	t.Run("KnownDeviceGetsEnrichedAndPublished", func(t *testing.T) {
		sampleMsgTimeKnown := time.Now().UTC().Truncate(time.Second)
		payloadKnown := "PayloadKnownDeviceTyped"
		sourceMqttMsgKnown := MQTTMessage{DeviceInfo: DeviceInfo{DeviceEUI: testDeviceEUIKnownTyped}, RawPayload: payloadKnown, MessageTimestamp: sampleMsgTimeKnown}
		msgBytesKnown, _ := json.Marshal(sourceMqttMsgKnown)
		publishTopicKnown := strings.Replace(testMqttTopicFullFlowTyped, "+", testDeviceEUIKnownTyped, 1)
		tokenKnown := mqttTestPublisher.Publish(publishTopicKnown, 1, false, msgBytesKnown)
		if tokenKnown.WaitTimeout(5*time.Second) && tokenKnown.Error() != nil {
			require.NoError(t, tokenKnown.Error(), "Publish for known device failed")
		}

		var receivedEnrichedMsg EnrichedMessage
		pullCtxKnown, pullCancelKnown := context.WithTimeout(ctx, 20*time.Second)
		defer pullCancelKnown()
		var wgReceiveKnown sync.WaitGroup
		wgReceiveKnown.Add(1)
		var receiveErrKnown error
		go func() {
			defer wgReceiveKnown.Done()
			errRcv := enrichedSub.Receive(pullCtxKnown, func(ctxMsg context.Context, msg *pubsub.Message) {
				msg.Ack()
				json.Unmarshal(msg.Data, &receivedEnrichedMsg)
				pullCancelKnown()
			})
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
				receiveErrKnown = errRcv
			}
		}()
		wgReceiveKnown.Wait()
		require.NoError(t, receiveErrKnown, "Error receiving from enrichedSub")
		if pullCtxKnown.Err() == context.DeadlineExceeded && receivedEnrichedMsg.DeviceEUI == "" {
			t.Fatalf("Timeout waiting for KNOWN device message on enriched topic. Logs:\n%s", logBuf.String())
		}

		assert.Equal(t, testDeviceEUIKnownTyped, receivedEnrichedMsg.DeviceEUI)
		assert.Equal(t, payloadKnown, receivedEnrichedMsg.RawPayload)
		assert.Equal(t, knownDeviceClientID, receivedEnrichedMsg.ClientID)
		assert.Contains(t, logBuf.String(), "Successfully processed and published enriched message", "Known device log missing")
	})

	// --- Scenario 2: Publish message for UNKNOWN device ---
	t.Run("UnknownDeviceGetsRoutedToUnidentifiedTopic", func(t *testing.T) {
		sampleMsgTimeUnknown := time.Now().UTC().Truncate(time.Second)
		payloadUnknown := "PayloadUnknownDeviceTyped"
		sourceMqttMsgUnknown := MQTTMessage{DeviceInfo: DeviceInfo{DeviceEUI: testDeviceEUIUnknownTyped}, RawPayload: payloadUnknown, MessageTimestamp: sampleMsgTimeUnknown}
		msgBytesUnknown, _ := json.Marshal(sourceMqttMsgUnknown)
		publishTopicUnknown := strings.Replace(testMqttTopicFullFlowTyped, "+", testDeviceEUIUnknownTyped, 1)
		tokenUnknown := mqttTestPublisher.Publish(publishTopicUnknown, 1, false, msgBytesUnknown)
		if tokenUnknown.WaitTimeout(5*time.Second) && tokenUnknown.Error() != nil {
			require.NoError(t, tokenUnknown.Error(), "Publish for unknown device failed")
		}

		var receivedUnidentifiedMsg UnidentifiedDeviceMessage
		pullCtxUnknown, pullCancelUnknown := context.WithTimeout(ctx, 20*time.Second)
		defer pullCancelUnknown()
		var wgReceiveUnknown sync.WaitGroup
		wgReceiveUnknown.Add(1)
		var receiveErrUnknown error
		go func() {
			defer wgReceiveUnknown.Done()
			errRcv := unidentifiedSub.Receive(pullCtxUnknown, func(ctxMsg context.Context, msg *pubsub.Message) {
				msg.Ack()
				json.Unmarshal(msg.Data, &receivedUnidentifiedMsg)
				pullCancelUnknown()
			})
			if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
				receiveErrUnknown = errRcv
			}
		}()
		wgReceiveUnknown.Wait()
		require.NoError(t, receiveErrUnknown, "Error receiving from unidentifiedSub")
		if pullCtxUnknown.Err() == context.DeadlineExceeded && receivedUnidentifiedMsg.DeviceEUI == "" {
			t.Fatalf("Timeout waiting for UNKNOWN device message on unidentified topic. Logs:\n%s", logBuf.String())
		}

		assert.Equal(t, testDeviceEUIUnknownTyped, receivedUnidentifiedMsg.DeviceEUI)
		assert.Equal(t, payloadUnknown, receivedUnidentifiedMsg.RawPayload)
		assert.Equal(t, ErrMetadataNotFound.Error(), receivedUnidentifiedMsg.ProcessingError)
		assert.Contains(t, logBuf.String(), "Successfully published to unidentified device topic", "Unknown device log missing")
	})
	t.Logf("Full flow integration test with typed publisher methods completed. Logs:\n%s", logBuf.String())
}
