//go:build integration

package messenger_test // Package name for test file often ends with _test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"load_test/apps/devicegen/messenger"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// Mosquitto
	testMosquittoImageIntegration = "eclipse-mosquitto:2.0"
	testMqttBrokerPortIntegration = "1883/tcp"
	testMqttTopicPattern          = "devices/loadtest/{DEVICE_EUI}/up" // Topic pattern for the load test

	// Firestore Emulator
	testFirestoreEmulatorImageIntegration = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testFirestoreEmulatorPortIntegration  = "8080/tcp"
	testFirestoreProjectIDIntegration     = "loadtest-project"
	testFirestoreCollectionIntegration    = "loadtest-devices"
)

// --- Test Setup Helpers (Adapted from the provided example) ---

// setupMosquittoContainerIntegration starts a Mosquitto container.
func setupMosquittoContainerIntegration(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	confContent := []byte("allow_anonymous true\nlistener 1883\npersistence false\n")
	tempDir := t.TempDir()
	hostConfPath := filepath.Join(tempDir, "mosquitto.conf")
	err := os.WriteFile(hostConfPath, confContent, 0644)
	require.NoError(t, err, "Failed to write temporary mosquitto.conf")

	req := testcontainers.ContainerRequest{
		Image:        testMosquittoImageIntegration,
		ExposedPorts: []string{testMqttBrokerPortIntegration},
		WaitingFor:   wait.ForListeningPort(testMqttBrokerPortIntegration).WithStartupTimeout(60 * time.Second),
		Files: []testcontainers.ContainerFile{
			{HostFilePath: hostConfPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf", FileMode: 0o644},
		},
		Cmd: []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Mosquitto container")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testMqttBrokerPortIntegration)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container started, broker URL: %s", brokerURL)

	return brokerURL, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Mosquitto container: %v", err)
		}
		t.Log("Mosquitto container terminated.")
	}
}

// setupFirestoreEmulatorIntegration starts a Firestore emulator.
func setupFirestoreEmulatorIntegration(t *testing.T, ctx context.Context, projectID string) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testFirestoreEmulatorImageIntegration,
		ExposedPorts: []string{testFirestoreEmulatorPortIntegration},
		Cmd:          []string{"gcloud", "beta", "emulators", "firestore", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testFirestoreEmulatorPortIntegration, "/")[0])},
		WaitingFor:   wait.ForLog("Dev App Server is now running").WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Firestore emulator")

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testFirestoreEmulatorPortIntegration)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Firestore emulator started, host: %s", emulatorHost)

	return emulatorHost, func() {
		if err := container.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Firestore emulator: %v", err)
		}
		t.Log("Firestore emulator terminated.")
	}
}

// createTestMqttSubscriberClient creates an MQTT client for subscribing to messages.
func createTestMqttSubscriberClient(brokerURL string, clientID string, logger zerolog.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			logger.Error().Err(err).Msg("Test MQTT subscriber connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			logger.Info().Str("client_id", clientID).Msg("Test MQTT subscriber connected")
		})
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(15*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test mqtt subscriber Connect(): %w", token.Error())
	}
	return client, nil
}

func TestMessenger_Integration_DeviceGenAndPublish(t *testing.T) { // Updated test name
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Overall test timeout
	defer cancel()

	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// --- 1. Setup Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainerIntegration(t, ctx)
	defer mosquittoCleanup()

	firestoreEmulatorHost, firestoreEmulatorCleanup := setupFirestoreEmulatorIntegration(t, ctx, testFirestoreProjectIDIntegration)
	defer firestoreEmulatorCleanup()

	t.Setenv("FIRESTORE_EMULATOR_HOST", firestoreEmulatorHost)
	t.Setenv("GCP_FIRESTORE_CREDENTIALS_FILE", "")

	// --- 2. Configure DeviceGenerator ---
	numTestDevices := 3
	msgRatePerDevice := 2.0                           // messages per second
	deviceGenCfg := &messenger.DeviceGeneratorConfig{ // Updated package name
		NumDevices:            numTestDevices,
		MsgRatePerDevice:      msgRatePerDevice,
		FirestoreEmulatorHost: firestoreEmulatorHost,
		FirestoreProjectID:    testFirestoreProjectIDIntegration,
		FirestoreCollection:   testFirestoreCollectionIntegration,
	}

	// Create Firestore client for DeviceGenerator
	fsClientCtx, fsClientCancel := context.WithTimeout(ctx, 20*time.Second)
	defer fsClientCancel()
	fsClient, err := firestore.NewClient(fsClientCtx, deviceGenCfg.FirestoreProjectID,
		option.WithEndpoint(deviceGenCfg.FirestoreEmulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCConnectionPool(1),
	)
	require.NoError(t, err, "Failed to create Firestore client for DeviceGenerator")
	defer fsClient.Close()

	deviceGen, err := messenger.NewDeviceGenerator(deviceGenCfg, fsClient, logger) // Updated package name
	require.NoError(t, err, "Failed to create DeviceGenerator")

	// --- 3. Create and Seed Devices ---
	deviceGen.CreateDevices()
	require.Len(t, deviceGen.Devices, numTestDevices, "Incorrect number of devices created")

	seedCtx, seedCancel := context.WithTimeout(ctx, 30*time.Second)
	defer seedCancel()
	err = deviceGen.SeedDevices(seedCtx)
	require.NoError(t, err, "Failed to seed devices")

	// --- 4. Verify Firestore Seeding ---
	t.Run("VerifyFirestoreSeeding", func(t *testing.T) {
		verifyFsClient, errVerify := firestore.NewClient(ctx, deviceGenCfg.FirestoreProjectID,
			option.WithEndpoint(deviceGenCfg.FirestoreEmulatorHost),
			option.WithoutAuthentication(),
			option.WithGRPCConnectionPool(1),
		)
		require.NoError(t, errVerify, "Failed to create Firestore client for verification")
		defer verifyFsClient.Close()

		for i := 0; i < numTestDevices; i++ {
			expectedEUI := fmt.Sprintf("LOADTEST-%06d", i)
			docRef := verifyFsClient.Collection(deviceGenCfg.FirestoreCollection).Doc(expectedEUI)
			docSnap, errGet := docRef.Get(ctx)
			require.NoError(t, errGet, "Failed to get device %s from Firestore", expectedEUI)
			assert.True(t, docSnap.Exists(), "Device %s not found in Firestore", expectedEUI)

			var deviceData messenger.FirestoreDeviceData // Updated package name
			errData := docSnap.DataTo(&deviceData)
			require.NoError(t, errData, "Failed to map Firestore data for device %s", expectedEUI)
			assert.NotEmpty(t, deviceData.ClientID, "ClientID is empty for device %s", expectedEUI)
			assert.NotEmpty(t, deviceData.LocationID, "LocationID is empty for device %s", expectedEUI)
			assert.NotEmpty(t, deviceData.DeviceCategory, "DeviceCategory is empty for device %s", expectedEUI)
			t.Logf("Verified Firestore seeding for device: %s, Data: %+v", expectedEUI, deviceData)
		}
	})

	// --- 5. Configure Publisher ---
	pahoFactory := &messenger.PahoMQTTClientFactory{} // Updated package name
	publisherClientID := "loadtest-publisher-01"
	publisherQOS := 0

	publisher := messenger.NewPublisher( // Updated package name
		testMqttTopicPattern,
		mqttBrokerURL,
		publisherClientID,
		publisherQOS,
		logger.With().Str("component", "loadtest-publisher").Logger(),
		pahoFactory,
	)
	require.NotNil(t, publisher, "NewPublisher returned nil")

	// --- 6. Setup MQTT Subscriber to Verify Messages ---
	subscriberClientID := "loadtest-verifier-sub"
	mqttSubscriber, err := createTestMqttSubscriberClient(mqttBrokerURL, subscriberClientID, logger)
	require.NoError(t, err, "Failed to create test MQTT subscriber")
	defer mqttSubscriber.Disconnect(250)

	receivedMessages := make(map[string][]messenger.MQTTMessage) // Updated package name
	var mu sync.Mutex

	wildcardSubscribeTopic := strings.ReplaceAll(testMqttTopicPattern, "{DEVICE_EUI}", "+")
	token := mqttSubscriber.Subscribe(wildcardSubscribeTopic, byte(publisherQOS), func(client mqtt.Client, msg mqtt.Message) {
		mu.Lock()
		defer mu.Unlock()
		t.Logf("Verifier received message on topic: %s", msg.Topic())
		var mqttMsg messenger.MQTTMessage // Updated package name
		if errJson := json.Unmarshal(msg.Payload(), &mqttMsg); errJson != nil {
			t.Errorf("Failed to unmarshal received MQTT message: %v. Payload: %s", errJson, string(msg.Payload()))
			return
		}
		receivedMessages[msg.Topic()] = append(receivedMessages[msg.Topic()], mqttMsg)
	})
	token.WaitTimeout(5 * time.Second)
	require.NoError(t, token.Error(), "Failed to subscribe to MQTT topic %s", wildcardSubscribeTopic)
	t.Logf("Test subscriber subscribed to: %s", wildcardSubscribeTopic)

	// --- 7. Run Publisher ---
	// This duration is passed to publisher.Publish and used for d.delivery.ExpectedCount
	// but the actual loop is controlled by pubCtx.
	publishDurationArg := 3 * time.Second
	// This context controls how long the publishMessages loops actually run.
	loopControlDuration := publishDurationArg + 2*time.Second // 5 seconds
	pubCtx, pubCancel := context.WithTimeout(ctx, loopControlDuration)
	defer pubCancel()

	var publisherWg sync.WaitGroup
	publisherWg.Add(1)
	go func() {
		defer publisherWg.Done()
		t.Log("Starting publisher.Publish()...")
		// Pass publishDurationArg (3s) for ExpectedCount, and pubCtx (5s) for actual loop control.
		errPub := publisher.Publish(deviceGen.Devices, publishDurationArg, pubCtx)
		if errPub != nil && !errors.Is(errPub, context.Canceled) && !errors.Is(errPub, context.DeadlineExceeded) {
			t.Errorf("Publisher.Publish() returned an unexpected error: %v", errPub)
		}
		t.Log("Publisher.Publish() finished.")
	}()

	<-pubCtx.Done()
	t.Log("Publishing loop control context (pubCtx) is done.")
	publisherWg.Wait()
	t.Log("Publisher goroutine has completed.")
	time.Sleep(2 * time.Second) // Increased sleep to allow subscriber to process final messages

	// --- 8. Verify MQTT Messages ---
	t.Run("VerifyMqttMessages", func(t *testing.T) {
		mu.Lock()
		defer mu.Unlock()

		totalMessagesReceived := 0
		for _, msgs := range receivedMessages {
			totalMessagesReceived += len(msgs)
		}

		// Calculate expected messages based on loopControlDuration (5s) and msgRatePerDevice (2.0)
		// Interval = 1s / 2.0 = 0.5s
		// Theoretical max messages per device = floor(loopControlDuration / interval) = floor(5s / 0.5s) = 10
		messagesPerDeviceTheoreticMax := int(loopControlDuration.Seconds() * msgRatePerDevice) // Should be 10

		// Min expected: allow for missing the very last tick
		expectedTotalMessagesMin := numTestDevices * (messagesPerDeviceTheoreticMax - 1) // 3 * (10 - 1) = 27
		// Max expected: theoretical maximum
		expectedTotalMessagesMax := numTestDevices * messagesPerDeviceTheoreticMax // 3 * 10 = 30

		assert.GreaterOrEqualf(t, totalMessagesReceived, expectedTotalMessagesMin,
			"Expected at least %d messages, got %d. Loop duration: %s, Rate: %.1f msg/s/dev",
			expectedTotalMessagesMin, totalMessagesReceived, loopControlDuration, msgRatePerDevice)
		assert.LessOrEqualf(t, totalMessagesReceived, expectedTotalMessagesMax,
			"Expected at most %d messages, got %d. Loop duration: %s, Rate: %.1f msg/s/dev",
			expectedTotalMessagesMax, totalMessagesReceived, loopControlDuration, msgRatePerDevice)

		t.Logf("Total MQTT messages received by verifier: %d (expected range [%d, %d] for %d devices, %.1f msg/s/dev, %s loop)",
			totalMessagesReceived, expectedTotalMessagesMin, expectedTotalMessagesMax, numTestDevices, msgRatePerDevice, loopControlDuration)

		// If total messages are within global bounds, check per-device counts.
		if totalMessagesReceived >= expectedTotalMessagesMin && totalMessagesReceived <= expectedTotalMessagesMax {
			// Check if messages were received from all devices.
			// This might be too strict if some devices legitimately send 0 if the test is very short or rate low.
			// For this specific setup (3 devices, 5s loop, 2msg/s), we expect messages from all.
			if totalMessagesReceived > 0 { // Only check per-device if any messages were received at all
				assert.Lenf(t, receivedMessages, numTestDevices, "Should have received messages from all %d devices if total count (%d) is >0", numTestDevices, totalMessagesReceived)
			}

			for i := 0; i < numTestDevices; i++ {
				eui := fmt.Sprintf("LOADTEST-%06d", i)
				expectedTopic := strings.ReplaceAll(testMqttTopicPattern, "{DEVICE_EUI}", eui)
				deviceMessages, ok := receivedMessages[expectedTopic]

				// It's possible a device sent 0 if it was particularly unlucky with timing and context cancellation.
				// So, we check 'ok' and then length if 'ok'.
				if !ok && totalMessagesReceived > 0 { // If no messages for this device but others sent some.
					t.Logf("Warning: No messages received for device EUI %s on topic %s, but other devices sent messages.", eui, expectedTopic)
				}

				// Per-device min/max
				expectedMessagesPerDeviceMin := messagesPerDeviceTheoreticMax - 1 // e.g., 9
				expectedMessagesPerDeviceMax := messagesPerDeviceTheoreticMax     // e.g., 10

				// Only assert if messages were expected for this device (i.e., if ok or if we expect all devices to send)
				if ok || expectedMessagesPerDeviceMin > 0 { // If messages were found, or if we expect at least some
					assert.True(t, ok, "No messages received for device EUI %s on topic %s, but expected some.", eui, expectedTopic)
					if ok { // Only assert length if messages were actually received for this device
						assert.GreaterOrEqualf(t, len(deviceMessages), expectedMessagesPerDeviceMin,
							"Expected at least %d messages for device %s, got %d", expectedMessagesPerDeviceMin, eui, len(deviceMessages))
						assert.LessOrEqualf(t, len(deviceMessages), expectedMessagesPerDeviceMax,
							"Expected at most %d messages for device %s, got %d", expectedMessagesPerDeviceMax, eui, len(deviceMessages))

						t.Logf("Device %s: received %d messages (expected range [%d, %d]).", eui, len(deviceMessages), expectedMessagesPerDeviceMin, expectedMessagesPerDeviceMax)
						for _, msg := range deviceMessages {
							assert.Equal(t, eui, msg.DeviceInfo.DeviceEUI, "Message EUI mismatch")
							assert.NotEmpty(t, msg.RawPayload, "RawPayload is empty")
							assert.NotEmpty(t, msg.ClientMessageID, "ClientMessageID is empty")
							assert.False(t, msg.MessageTimestamp.IsZero(), "MessageTimestamp is zero")
						}
					}
				} else if expectedMessagesPerDeviceMin == 0 && !ok {
					// If we expected 0 (e.g., very short duration/low rate) and got 0, that's fine.
					t.Logf("Device %s: received 0 messages, which is acceptable if expected min is 0.", eui)
				}
			}
		} else {
			t.Logf("Skipping per-device checks as total messages (%d) is outside expected range [%d, %d]", totalMessagesReceived, expectedTotalMessagesMin, expectedTotalMessagesMax)
		}
	})
	t.Log("Integration test completed.")
}
