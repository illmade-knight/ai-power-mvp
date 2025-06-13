//go:build integration

package main_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/testcontainers/testcontainers-go"
	"ingestion/mqinit"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	// Import initializers for both services
	"bigquery/bqinit"

	// Import library packages
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/mqttconverter"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"

	"github.com/testcontainers/testcontainers-go/wait"
)

// --- E2E Test Constants ---
const (
	testProjectID = "e2e-garden-project"

	// MQTT -> Ingestion Service
	testMosquittoImage     = "eclipse-mosquitto:2.0"
	testMqttBrokerPort     = "1883/tcp"
	testMqttTopicPattern   = "devices/+/data"
	testMqttDeviceUID      = "GARDEN_MONITOR_E2E_001"
	testMqttClientIDPrefix = "ingestion-service-e2e"

	// Pub/Sub -> Link between services
	// Pub/Sub (The link between Ingestion and Processing services)
	testPubSubEmulatorImage  = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort   = "8085/tcp"
	testPubSubTopicID        = "garden-monitor-processed"     // Ingestion publishes here
	testPubSubSubscriptionID = "garden-monitor-processed-sub" // Processing subscribes here

	// BigQuery -> BigQuery Service
	e2eBigQueryDatasetID = "garden_e2e_dataset"
	e2eBigQueryTableID   = "monitor_payloads_e2e"

	// Service Ports
	e2eMqttHTTPPort = ":9090"
	e2eBqHTTPPort   = ":9091"
)

// --- Full End-to-End Test ---

func TestE2E_MqttToBigQueryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 1. Start All Emulators ---
	log.Info().Msg("E2E: Setting up Mosquitto emulator...")
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()

	log.Info().Msg("E2E: Setting up Pub/Sub emulator...")
	pubsubEmulatorHost, pubsubCleanup := setupPubSubEmulator(t, ctx)
	defer pubsubCleanup()
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)

	log.Info().Msg("E2E: Setting up BigQuery emulator...")
	bqCleanup := setupBigQueryEmulator(t, ctx)
	defer bqCleanup()

	//ensure service start
	log.Info().Msg("Pausing before ingestion setup")
	time.Sleep(3 * time.Second)

	// --- 2. Configure and Start MQTT Ingestion Service ---
	mqttCfg := &mqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  e2eMqttHTTPPort,
		ProjectID: testProjectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			TopicID: testPubSubTopicID,
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicPattern,
			ClientIDPrefix: "ingestion-test-client-",
			KeepAlive:      10 * time.Second,
			ConnectTimeout: 5 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
	}

	mqttLogger := log.With().Str("service", "mqtt-ingestion").Logger()
	mqttPublisher, err := mqttconverter.NewGooglePubSubPublisher(ctx, &mqttconverter.GooglePubSubPublisherConfig{
		ProjectID: mqttCfg.ProjectID,
		TopicID:   mqttCfg.Publisher.TopicID,
	}, mqttLogger)
	require.NoError(t, err)

	ingestionService := mqttconverter.NewIngestionService(mqttPublisher, nil, mqttLogger, mqttCfg.Service, mqttCfg.MQTT)

	mqttServer := mqinit.NewServer(mqttCfg, ingestionService, mqttLogger)

	go func() {
		if err := mqttServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Server failed during test execution")
		}
	}()
	defer mqttServer.Shutdown()
	time.Sleep(2 * time.Second) // Give the server and MQTT client time to connect

	// --- 3. Configure and Start BigQuery Processing Service ---
	bqCfg := &bqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  e2eBqHTTPPort,
		ProjectID: testProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: testPubSubSubscriptionID},
		BigQuery: struct {
			bqstore.BigQueryDatasetConfig
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			BigQueryDatasetConfig: bqstore.BigQueryDatasetConfig{
				ProjectID: testProjectID,
				DatasetID: e2eBigQueryDatasetID,
				TableID:   e2eBigQueryTableID,
			},
			CredentialsFile: "",
		},
		BatchProcessing: struct {
			bqstore.BatchInserterConfig `mapstructure:"datasetup"`
			NumWorkers                  int `mapstructure:"num_workers"`
		}{
			BatchInserterConfig: bqstore.BatchInserterConfig{
				BatchSize:    5,
				FlushTimeout: 10 * time.Second,
			},
			NumWorkers: 2,
		},
	}

	bqLogger := log.With().Str("service", "bq-processor").Logger()
	bqClient := newEmulatorBigQueryClient(t, ctx, bqCfg.ProjectID)
	defer bqClient.Close()

	bqConsumer, err := consumers.NewGooglePubSubConsumer(ctx, &consumers.GooglePubSubConsumerConfig{
		ProjectID:      bqCfg.ProjectID,
		SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new, single convenience constructor ***
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, &bqCfg.BatchProcessing.BatchInserterConfig, &bqCfg.BigQuery.BigQueryDatasetConfig, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new service constructor ***
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](bqCfg.BatchProcessing.NumWorkers, bqConsumer, batchInserter, types.ConsumedMessageTransformer, bqLogger)
	require.NoError(t, err)

	bqServer := bqinit.NewServer(bqCfg, processingService, bqLogger)

	go func() {
		if err := bqServer.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("BigQuery Processing server failed: %v", err)
		}
	}()
	defer bqServer.Shutdown()

	time.Sleep(3 * time.Second) // Give services time to start and connect

	// --- 4. Publish a Test Message to MQTT ---
	mqttTestPublisher, err := createTestMqttPublisherClient(mqttBrokerURL, "e2e-test-publisher")
	require.NoError(t, err)
	defer mqttTestPublisher.Disconnect(250)

	const messageCount = 1
	testPayload := types.GardenMonitorReadings{
		DE:       testMqttDeviceUID,
		Sequence: 123,
		Battery:  98,
	}
	msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &testPayload})
	require.NoError(t, err)
	publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceUID, 1)
	token := mqttTestPublisher.Publish(publishTopic, 1, false, msgBytes)
	token.Wait()
	require.NoError(t, token.Error())
	log.Info().Msg("E2E: Published test message to MQTT.")

	// --- 5. Verify Data in BigQuery using the correct polling method ---
	var receivedRows []types.GardenMonitorReadings
	var lastQueryErr error
	timeout := time.After(30 * time.Second)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("Test timed out waiting for BigQuery results. Last error: %v", lastQueryErr)
		case <-tick.C:
			queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid", e2eBigQueryDatasetID, e2eBigQueryTableID)
			query := bqClient.Query(queryString)
			query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testMqttDeviceUID}}

			it, err := query.Read(ctx)
			if err != nil {
				lastQueryErr = fmt.Errorf("polling query failed during Read: %w", err)
				continue // Try again on the next tick
			}

			var currentRows []types.GardenMonitorReadings
			for {
				var row types.GardenMonitorReadings
				err := it.Next(&row)
				if errors.Is(err, iterator.Done) {
					break
				}
				if err != nil {
					lastQueryErr = fmt.Errorf("polling query failed during Next: %w", err)
					continue VerificationLoop // Start the whole query over
				}
				currentRows = append(currentRows, row)
			}

			if len(currentRows) >= messageCount {
				receivedRows = currentRows
				log.Info().Int("count", len(receivedRows)).Msg("E2E: Successfully found rows, breaking verification loop.")
				break VerificationLoop // Success!
			}
			lastQueryErr = fmt.Errorf("still waiting for rows: got %d, want %d", len(currentRows), messageCount)
		}
	}

	// --- 6. Final Assertions ---
	require.Len(t, receivedRows, messageCount)
	assert.Equal(t, testPayload.DE, receivedRows[0].DE)
	assert.Equal(t, testPayload.Sequence, receivedRows[0].Sequence)
	assert.Equal(t, testPayload.Battery, receivedRows[0].Battery)
	log.Info().Msg("E2E: Verification successful!")
}

// --- Combined Emulator Setup Helpers ---

func setupMosquittoContainer(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "mosquitto.conf")
	err := os.WriteFile(confPath, []byte("listener 1883\nallow_anonymous true\n"), 0644)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        testMosquittoImage,
		ExposedPorts: []string{testMqttBrokerPort},
		WaitingFor:   wait.ForLog("mosquitto version 2.0").WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: confPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testMqttBrokerPort)
	require.NoError(t, err)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())

	return brokerURL, func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupPubSubEmulator(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImage,
		ExposedPorts: []string{testPubSubEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testProjectID), "--host-port=0.0.0.0:8085"},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testPubSubEmulatorPort)
	require.NoError(t, err)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator container started, listening on: %s", emulatorHost)
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	adminClient, err := pubsub.NewClient(ctx, testProjectID, option.WithEndpoint(emulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer adminClient.Close()

	topic := adminClient.Topic(testPubSubTopicID)
	exists, err := topic.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		_, err = adminClient.CreateTopic(ctx, testPubSubTopicID)
		require.NoError(t, err, "Failed to create Pub/Sub topic")
	}

	sub := adminClient.Subscription(testPubSubSubscriptionID)
	exists, err = sub.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		_, err = adminClient.CreateSubscription(ctx, testPubSubSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
		require.NoError(t, err, "Failed to create Pub/Sub subscription")
	}
	return emulatorHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupBigQueryEmulator(t *testing.T, ctx context.Context) (cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/goccy/bigquery-emulator:0.6.6",
		ExposedPorts: []string{"9050/tcp", "9060/tcp"},
		Cmd:          []string{"--project=" + testProjectID, "--port=9050", "--grpc-port=9060"},
		WaitingFor:   wait.ForAll(wait.ForListeningPort("9050/tcp"), wait.ForListeningPort("9060/tcp")),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	grpcPort, err := container.MappedPort(ctx, "9060/tcp")
	require.NoError(t, err)
	restPort, err := container.MappedPort(ctx, "9050/tcp")
	require.NoError(t, err)

	t.Setenv("BIGQUERY_EMULATOR_HOST", fmt.Sprintf("%s:%s", host, grpcPort.Port()))
	t.Setenv("BIGQUERY_API_ENDPOINT", fmt.Sprintf("http://%s:%s", host, restPort.Port()))

	adminClient := newEmulatorBigQueryClient(t, ctx, testProjectID)
	defer adminClient.Close()

	err = adminClient.Dataset(e2eBigQueryDatasetID).Create(ctx, &bigquery.DatasetMetadata{Name: e2eBigQueryDatasetID})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err)
	}

	table := adminClient.Dataset(e2eBigQueryDatasetID).Table(e2eBigQueryTableID)
	schema, err := bigquery.InferSchema(types.GardenMonitorReadings{})
	require.NoError(t, err)
	err = table.Create(ctx, &bigquery.TableMetadata{Name: e2eBigQueryTableID, Schema: schema})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err)
	}

	return func() { require.NoError(t, container.Terminate(ctx)) }
}

func newEmulatorBigQueryClient(t *testing.T, ctx context.Context, projectID string) *bigquery.Client {
	t.Helper()
	emulatorHost := os.Getenv("BIGQUERY_API_ENDPOINT")
	require.NotEmpty(t, emulatorHost)
	client, err := bigquery.NewClient(ctx, projectID, option.WithEndpoint(emulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	return client
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
