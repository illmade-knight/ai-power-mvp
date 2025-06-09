//go:build integration

package endtoend_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	// Import the packages for the two services under test
	bq "github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/bigquery"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/ingestion/converter"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- E2E Test Constants ---
const (
	// Shared Project ID
	testProjectID = "e2e-garden-project"

	// Mosquitto (Ingestion Service Input)
	testMosquittoImage     = "eclipse-mosquitto:2.0"
	testMqttBrokerPort     = "1883/tcp"
	testMqttTopicPattern   = "devices/garden-monitor/+/telemetry"
	testMqttDeviceUID      = "GARDEN_MONITOR_E2E_001"
	testMqttClientIDPrefix = "ingestion-service-e2e"

	// Pub/Sub (The link between Ingestion and Processing services)
	testPubSubEmulatorImage      = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort       = "8085/tcp"
	testPubSubTopicID            = "garden-monitor-processed"       // Ingestion publishes here
	testPubSubSubscriptionID     = "garden-monitor-processed-sub"   // Processing subscribes here
	envVarIngestionPubSubTopicID = "GARDEN_MONITOR_TOPIC_PROCESSED" // Env var for ingestion service

	// BigQuery (Processing Service Output)
	testBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testBigQueryGRPCPortStr   = "9060"
	testBigQueryRestPortStr   = "9050"
	testBigQueryGRPCPort      = testBigQueryGRPCPortStr + "/tcp"
	testBigQueryRestPort      = testBigQueryRestPortStr + "/tcp"
	testBigQueryDatasetID     = "garden_e2e_dataset"
	testBigQueryTableID       = "monitor_payloads_e2e"
)

// --- Emulator Setup Helpers (Combined from both integration tests) ---

func setupMosquittoContainer(t *testing.T, ctx context.Context) (brokerURL string, cleanupFunc func()) {
	t.Helper()
	mosquittoConfContent := "persistence false\nlistener 1883\nallow_anonymous true\n"
	tempDir := t.TempDir()
	confPath := filepath.Join(tempDir, "mosquitto.conf")
	err := os.WriteFile(confPath, []byte(mosquittoConfContent), 0644)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        testMosquittoImage,
		ExposedPorts: []string{testMqttBrokerPort},
		WaitingFor:   wait.ForListeningPort(testMqttBrokerPort).WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: confPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
		Cmd:          []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testMqttBrokerPort)
	require.NoError(t, err)
	brokerURL = fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container started, broker URL: %s", brokerURL)
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
		Image:        testBigQueryEmulatorImage,
		ExposedPorts: []string{testBigQueryGRPCPort, testBigQueryRestPort},
		Cmd:          []string{"--project=" + testProjectID, "--port=" + testBigQueryRestPortStr, "--grpc-port=" + testBigQueryGRPCPortStr},
		WaitingFor:   wait.ForAll(wait.ForListeningPort(testBigQueryGRPCPort), wait.ForListeningPort(testBigQueryRestPort)),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)
	grpcMappedPort, err := container.MappedPort(ctx, testBigQueryGRPCPort)
	require.NoError(t, err)
	emulatorGRPCHost := fmt.Sprintf("%s:%s", host, grpcMappedPort.Port())
	restMappedPort, err := container.MappedPort(ctx, testBigQueryRestPort)
	require.NoError(t, err)
	emulatorRESTHost := fmt.Sprintf("http://%s:%s", host, restMappedPort.Port())
	t.Logf("BigQuery emulator container started, gRPC on: %s, REST on: %s", emulatorGRPCHost, emulatorRESTHost)

	t.Setenv("BIGQUERY_EMULATOR_HOST", emulatorGRPCHost)
	t.Setenv("BIGQUERY_API_ENDPOINT", emulatorRESTHost)

	// Admin client to create dataset/table
	adminBqClient := newEmulatorBigQueryClient(ctx, t, testProjectID)
	defer adminBqClient.Close()
	err = adminBqClient.Dataset(testBigQueryDatasetID).Create(ctx, &bigquery.DatasetMetadata{Name: testBigQueryDatasetID})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create BQ dataset")
	}

	table := adminBqClient.Dataset(testBigQueryDatasetID).Table(testBigQueryTableID)
	schema, err := bigquery.InferSchema(bq.GardenMonitorPayload{})
	require.NoError(t, err, "Failed to infer schema")
	tableMeta := &bigquery.TableMetadata{Name: testBigQueryTableID, Schema: schema}
	err = table.Create(ctx, tableMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create BQ table")
	}

	return func() { require.NoError(t, container.Terminate(ctx)) }
}

func newEmulatorBigQueryClient(ctx context.Context, t *testing.T, projectID string) *bigquery.Client {
	t.Helper()
	emulatorHost := os.Getenv("BIGQUERY_API_ENDPOINT")
	require.NotEmpty(t, emulatorHost)
	client, err := bigquery.NewClient(ctx, projectID,
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}),
	)
	require.NoError(t, err)
	return client
}

// --- End-to-End Test ---

func TestE2E_MQTT_to_BigQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var logBuf bytes.Buffer
	writer := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}, &logBuf)
	logger := zerolog.New(writer).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// --- 1. Start Emulators ---
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()
	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulator(t, ctx)
	defer pubsubEmulatorCleanup()
	bqEmulatorCleanup := setupBigQueryEmulator(t, ctx)
	defer bqEmulatorCleanup()

	// --- 2. Set Environment Variables for BOTH services ---
	t.Setenv("GCP_PROJECT_ID", testProjectID)
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")
	t.Setenv("GCP_BQ_CREDENTIALS_FILE", "")

	// Ingestion Service
	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", testMqttTopicPattern)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", testMqttClientIDPrefix)
	t.Setenv(envVarIngestionPubSubTopicID, testPubSubTopicID)

	// Processing Service
	t.Setenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT", testPubSubSubscriptionID)
	t.Setenv("BQ_DATASET_ID", testBigQueryDatasetID)
	t.Setenv("BQ_TABLE_ID_METER_READINGS", testBigQueryTableID)

	// --- 3. Initialize and Start Ingestion Service ---
	ingestionMQTTConfig, err := converter.LoadMQTTClientConfigFromEnv()
	require.NoError(t, err)
	ingestionPubSubConfig, err := converter.LoadGooglePubSubPublisherConfigFromEnv(envVarIngestionPubSubTopicID)
	require.NoError(t, err)
	ingestionPublisher, err := converter.NewGooglePubSubPublisher(ctx, ingestionPubSubConfig, logger.With().Str("service", "IngestionPublisher").Logger())
	require.NoError(t, err)
	ingestionService := converter.NewIngestionService(ingestionPublisher, logger.With().Str("service", "Ingestion").Logger(), converter.DefaultIngestionServiceConfig(), ingestionMQTTConfig)

	go func() {
		t.Log("Starting Ingestion Service...")
		if err := ingestionService.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("Ingestion service failed to start: %v", err)
		}
	}()
	defer ingestionService.Stop()

	// --- 4. Initialize and Start Processing Service ---
	processingServiceConfig, err := bq.LoadServiceConfigFromEnv()
	require.NoError(t, err)
	processingServiceConfig.BatchSize = 10
	processingServiceConfig.FlushTimeout = 5 * time.Second // Flush after 5s of inactivity

	bqInserterConfig, err := bq.LoadBigQueryInserterConfigFromEnv()
	require.NoError(t, err)

	bqClientForService := newEmulatorBigQueryClient(ctx, t, bqInserterConfig.ProjectID)
	defer bqClientForService.Close()
	bqInserter, err := bq.NewBigQueryInserter(ctx, bqClientForService, bqInserterConfig, logger.With().Str("service", "BigQueryInserter").Logger())
	require.NoError(t, err)

	processingService, err := bq.NewProcessingService(ctx, processingServiceConfig, bqInserter, logger.With().Str("service", "Processing").Logger())
	require.NoError(t, err)

	go func() {
		t.Log("Starting Processing Service...")
		if err := processingService.Start(); err != nil {
			t.Errorf("Processing service failed to start: %v", err)
		}
	}()
	defer processingService.Stop()

	time.Sleep(5 * time.Second) // Give services time to start and subscribe

	// --- 5. Publish Test Messages to MQTT ---
	mqttTestPublisher := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(mqttBrokerURL).SetClientID("e2e-test-publisher"))
	token := mqttTestPublisher.Connect()
	require.True(t, token.WaitTimeout(10*time.Second), "MQTT publisher failed to connect")
	require.NoError(t, token.Error())
	defer mqttTestPublisher.Disconnect(250)

	const messageCount = 3
	var lastTestPayload converter.GardenMonitorPayload
	for i := 0; i < messageCount; i++ {
		testPayload := converter.GardenMonitorPayload{
			DE:           testMqttDeviceUID,
			SIM:          "8901234567890123456",
			RSSI:         "-75dBm",
			Version:      "2.0.0-e2e",
			Sequence:     42 + i,
			Battery:      99 - i,
			Temperature:  25,
			Humidity:     55,
			SoilMoisture: 600,
		}
		lastTestPayload = testPayload
		payloadBytes, err := json.Marshal(testPayload)
		require.NoError(t, err)

		publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceUID, 1)
		pubToken := mqttTestPublisher.Publish(publishTopic, 1, false, payloadBytes)
		require.True(t, pubToken.WaitTimeout(10*time.Second), "MQTT publish timed out")
		require.NoError(t, pubToken.Error())
	}
	t.Logf("Published %d test messages to MQTT", messageCount)

	// --- 6. Verification Step: Poll BigQuery until the data appears ---
	// FIX: Use a manual polling loop with a single client to avoid connection churn.
	var receivedRows []bq.GardenMonitorPayload
	var lastQueryErr error
	timeout := time.After(30 * time.Second)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	queryClient := newEmulatorBigQueryClient(ctx, t, testProjectID)
	defer queryClient.Close()

VerificationLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("Test timed out waiting for BigQuery results. Last error: %v. Logs:\n%s", lastQueryErr, logBuf.String())
		case <-tick.C:
			queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid", testBigQueryDatasetID, testBigQueryTableID)
			query := queryClient.Query(queryString)
			query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testMqttDeviceUID}}

			it, err := query.Read(ctx)
			if err != nil {
				lastQueryErr = fmt.Errorf("polling query failed during Read: %w", err)
				continue // Try again on the next tick
			}

			var currentRows []bq.GardenMonitorPayload
			for {
				var row bq.GardenMonitorPayload
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

			if len(currentRows) == messageCount {
				receivedRows = currentRows
				t.Logf("Successfully found %d rows, breaking verification loop.", messageCount)
				break VerificationLoop // Success!
			}
			lastQueryErr = fmt.Errorf("still waiting for rows: got %d, want %d", len(currentRows), messageCount)
		}
	}

	// --- 7. Final Assertions ---
	require.Len(t, receivedRows, messageCount, "The number of rows in BigQuery does not match the number of messages sent.")

	finalRow := receivedRows[len(receivedRows)-1]
	assert.Equal(t, lastTestPayload.DE, finalRow.UID, "UID mismatch")
	assert.Equal(t, lastTestPayload.SIM, finalRow.SIM, "SIM mismatch")
	assert.Equal(t, lastTestPayload.Version, finalRow.Version, "Version mismatch")
	assert.Equal(t, lastTestPayload.Battery, finalRow.Battery, "Battery mismatch")
	assert.Equal(t, lastTestPayload.Temperature, finalRow.Temperature, "Temperature mismatch")
	assert.Equal(t, lastTestPayload.SoilMoisture, finalRow.SoilMoisture, "SoilMoisture mismatch")

	t.Logf("Successfully verified E2E data flow in BigQuery for UID: %s", testMqttDeviceUID)
}
