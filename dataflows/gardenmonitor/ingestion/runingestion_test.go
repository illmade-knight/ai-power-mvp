//go:build integration

package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/ingestion/mqinit"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Test Constants ---
const (
	testPubsubEmulatorImage  = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testProjectID            = "test-mqtt-project"
	testMqttTopic            = "devices/test-device-01/data"
	testMqttTopicFilter      = "devices/+/data"
	testPubSubTopicID        = "mqtt-converted-topic"
	testPubSubSubscriptionID = "mqtt-converted-sub"
	testHTTPPort             = ":8898"
)

// --- Full Integration Test for the MQTT Ingestion Service ---

func TestMqttIngestionService_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// --- 1. Setup Emulators ---
	log.Info().Msg("Setting up Mosquitto emulator for test...")
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()

	log.Info().Msg("Setting up Pub/Sub emulator for test...")
	pubsubOptions, pubsubCleanup := emulators.SetupPubSubEmulator(t, ctx, &emulators.PubsubConfig{
		GCImageContainer: emulators.GCImageContainer{
			ImageContainer: emulators.ImageContainer{
				EmulatorImage:    testPubsubEmulatorImage,
				EmulatorHTTPPort: "8085",
			},
			ProjectID: testProjectID,
		},
		TopicSubs: map[string]string{testPubSubTopicID: testPubSubSubscriptionID},
	})
	defer pubsubCleanup()

	// --- 2. Configure the application for the test environment ---
	cfg := &mqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  testHTTPPort,
		ProjectID: testProjectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			TopicID: testPubSubTopicID,
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicFilter,
			ClientIDPrefix: "ingestion-test-client-",
			KeepAlive:      10 * time.Second,
			ConnectTimeout: 5 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
	}

	// --- 3. Build and Start the Service ---
	testLogger := log.With().Str("service", "mqtt-ingestion-test").Logger()

	// The publisher will correctly use the PUBSUB_EMULATOR_HOST env var.
	publisher, err := mqttconverter.NewGooglePubSubPublisher(ctx, &mqttconverter.GooglePubSubPublisherConfig{
		ProjectID: cfg.ProjectID,
		TopicID:   cfg.Publisher.TopicID,
	}, testLogger)
	require.NoError(t, err)

	ingestionService := mqttconverter.NewIngestionService(publisher, nil, testLogger, cfg.Service, cfg.MQTT)

	server := mqinit.NewServer(cfg, ingestionService, testLogger)

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Server failed during test execution")
		}
	}()
	time.Sleep(2 * time.Second) // Give the server and MQTT client time to connect

	// --- 4. Test Health Check Endpoint ---
	resp, err := http.Get("http://localhost" + testHTTPPort + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Health check should return 200 OK")

	// --- 5. Prepare Test Clients (Pub/Sub Subscriber and MQTT Publisher) ---
	// Create Pub/Sub client for verification, configured explicitly to use the emulator.
	psClient, err := pubsub.NewClient(ctx, testProjectID, pubsubOptions...)
	require.NoError(t, err)
	defer psClient.Close()

	psSub := psClient.Subscription(testPubSubSubscriptionID)

	// Create MQTT client to publish the test message
	mqttPublisher, err := createTestMqttPublisher(mqttBrokerURL, "test-publisher-client")
	require.NoError(t, err)
	defer mqttPublisher.Disconnect(250)

	// --- 6. Publish Test Message and Verify Reception ---
	sourcePayload := map[string]interface{}{"value": "42", "status": "ok"}
	msgBytes, err := json.Marshal(sourcePayload)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedMsg *pubsub.Message

	go func() {
		defer wg.Done()
		pullCtx, cancelReceive := context.WithTimeout(ctx, 20*time.Second)
		defer cancelReceive()
		errRcv := psSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			log.Info().Str("id", msg.ID).Msg("Received message from Pub/Sub for verification")
			msg.Ack()
			receivedMsg = msg
			cancelReceive() // Stop receiving after the first message
		})
		if errRcv != nil && !errors.Is(errRcv, context.Canceled) {
			require.NoError(t, errRcv, "Pub/Sub Receive failed")
		}
	}()

	// Publish the message *after* starting the receiver goroutine
	token := mqttPublisher.Publish(testMqttTopic, 1, false, msgBytes)
	token.Wait()
	require.NoError(t, token.Error())
	log.Info().Str("topic", testMqttTopic).Msg("Published test message to MQTT")

	wg.Wait() // Wait for the receiver to finish

	// --- 7. Stop the Server and Verify Data ---
	server.Shutdown()

	require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub subscription")
	assert.Equal(t, msgBytes, receivedMsg.Data, "The received Pub/Sub payload should match the sent MQTT payload")
	require.NotNil(t, receivedMsg.Attributes, "Received message should have attributes")
	assert.Equal(t, testMqttTopic, receivedMsg.Attributes["mqtt_topic"], "Attribute 'mqtt_topic' should match")
}

// --- Emulator and Test Client Setup Helpers ---

func setupMosquittoContainer(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "mosquitto.conf")
	err := os.WriteFile(confPath, []byte("listener 1883\nallow_anonymous true\n"), 0644)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        "eclipse-mosquitto:2.0",
		ExposedPorts: []string{"1883/tcp"},
		WaitingFor:   wait.ForLog("mosquitto version 2.0").WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: confPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "1883/tcp")
	require.NoError(t, err)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())

	return brokerURL, func() { require.NoError(t, container.Terminate(ctx)) }
}

func createTestMqttPublisher(brokerURL, clientID string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().AddBroker(brokerURL).SetClientID(clientID)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test mqtt publisher connect error: %w", token.Error())
	}
	return client, nil
}
