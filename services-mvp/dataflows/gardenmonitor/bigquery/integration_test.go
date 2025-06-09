//go:build integration

package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Constants for the integration test environment.
const (
	// Pub/Sub Emulator Test Config
	testPubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort  = "8085/tcp"
	testProjectID           = "test-garden-project"
	testInputTopicID        = "garden-monitor-topic"
	testInputSubscriptionID = "garden-monitor-sub"

	// BigQuery Emulator Test Config
	testBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testBigQueryGRPCPortStr   = "9060"
	testBigQueryRestPortStr   = "9050"
	testBigQueryGRPCPort      = testBigQueryGRPCPortStr + "/tcp"
	testBigQueryRestPort      = testBigQueryRestPortStr + "/tcp"
	testBigQueryDatasetID     = "garden_data_dataset"
	testBigQueryTableID       = "monitor_payloads"

	// Device specific constants for test message
	testDeviceUID = "GARDEN_MONITOR_001"
)

// --- Test Setup Helpers ---

// newEmulatorBigQueryClient creates a BigQuery client configured to connect to the emulator.
// This is the original setup function from the initial test version.
func newEmulatorBigQueryClient(ctx context.Context, t *testing.T, projectID string) *bigquery.Client {
	t.Helper()
	emulatorHost := os.Getenv("BIGQUERY_API_ENDPOINT")
	require.NotEmpty(t, emulatorHost, "BIGQUERY_API_ENDPOINT env var must be set for newEmulatorBigQueryClient")

	clientOpts := []option.ClientOption{
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}),
	}

	client, err := bigquery.NewClient(ctx, projectID, clientOpts...)
	require.NoError(t, err, "Failed to create BigQuery client for emulator. EmulatorHost: %s", emulatorHost)
	return client
}

// setupPubSubEmulatorForProcessingTest starts a Pub/Sub emulator in a test container.
func setupPubSubEmulatorForProcessingTest(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubEmulatorImage,
		ExposedPorts: []string{testPubSubEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testProjectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubEmulatorPort, "/")[0])},
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

	// Create an admin client to set up the topic and subscription.
	adminClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer adminClient.Close()

	topic := adminClient.Topic(testInputTopicID)
	exists, err := topic.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		_, err = adminClient.CreateTopic(ctx, testInputTopicID)
		require.NoError(t, err)
	}

	sub := adminClient.Subscription(testInputSubscriptionID)
	exists, err = sub.Exists(ctx)
	require.NoError(t, err)
	if !exists {
		_, err = adminClient.CreateSubscription(ctx, testInputSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
		require.NoError(t, err)
	}

	return emulatorHost, func() {
		require.NoError(t, container.Terminate(ctx))
	}
}

// setupBigQueryEmulatorForProcessingTest starts a BigQuery emulator in a test container.
func setupBigQueryEmulatorForProcessingTest(t *testing.T, ctx context.Context) (cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testBigQueryEmulatorImage,
		ExposedPorts: []string{testBigQueryGRPCPort, testBigQueryRestPort},
		Cmd: []string{
			"--project=" + testProjectID,
			"--port=" + testBigQueryRestPortStr,
			"--grpc-port=" + testBigQueryGRPCPortStr,
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(testBigQueryGRPCPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(testBigQueryRestPort).WithStartupTimeout(60*time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start BigQuery emulator container.")

	host, err := container.Host(ctx)
	require.NoError(t, err)

	grpcMappedPort, err := container.MappedPort(ctx, testBigQueryGRPCPort)
	require.NoError(t, err)
	emulatorGRPCHost := fmt.Sprintf("%s:%s", host, grpcMappedPort.Port())

	restMappedPort, err := container.MappedPort(ctx, testBigQueryRestPort)
	require.NoError(t, err)
	emulatorRESTHost := fmt.Sprintf("http://%s:%s", host, restMappedPort.Port())

	// Set environment variables for the clients.
	t.Setenv("GOOGLE_CLOUD_PROJECT", testProjectID)
	t.Setenv("BIGQUERY_EMULATOR_HOST", emulatorGRPCHost)
	t.Setenv("BIGQUERY_API_ENDPOINT", emulatorRESTHost)

	adminBqClient := newEmulatorBigQueryClient(ctx, t, testProjectID)
	require.NotNil(t, adminBqClient, "Admin BQ client should not be nil")
	defer adminBqClient.Close()

	dataset := adminBqClient.Dataset(testBigQueryDatasetID)
	err = dataset.Create(ctx, &bigquery.DatasetMetadata{Name: testBigQueryDatasetID})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create dataset '%s' on BQ emulator.", testBigQueryDatasetID)
	}

	table := dataset.Table(testBigQueryTableID)
	schema, err := bigquery.InferSchema(GardenMonitorPayload{})
	require.NoError(t, err, "Failed to infer schema from GardenMonitorPayload")

	// Create a simple, non-partitioned table.
	tableMeta := &bigquery.TableMetadata{Name: testBigQueryTableID, Schema: schema}
	err = table.Create(ctx, tableMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create table '%s' on BQ emulator", testBigQueryTableID)
	}

	return func() {
		require.NoError(t, container.Terminate(ctx))
	}
}

// TestProcessingService_Integration_FullFlow tests the entire microservice flow with batching.
func TestProcessingService_Integration_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorForProcessingTest(t, ctx)
	defer pubsubEmulatorCleanup()
	bqEmulatorCleanup := setupBigQueryEmulatorForProcessingTest(t, ctx)
	defer bqEmulatorCleanup()

	t.Setenv("GCP_PROJECT_ID", testProjectID)
	t.Setenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT", testInputSubscriptionID)
	t.Setenv("BQ_DATASET_ID", testBigQueryDatasetID)
	t.Setenv("BQ_TABLE_ID_METER_READINGS", testBigQueryTableID)
	t.Setenv("GCP_BQ_CREDENTIALS_FILE", "")
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")

	var logBuf bytes.Buffer
	writer := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}, &logBuf)
	logger := zerolog.New(writer).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	serviceCfg, err := LoadServiceConfigFromEnv()
	require.NoError(t, err)
	serviceCfg.NumProcessingWorkers = 2 // Use 2 worker to deterministically test shutdown
	serviceCfg.BatchSize = 5
	serviceCfg.FlushTimeout = 10 * time.Second // Longer timeout to ensure flush happens on stop

	bqInserterCfg, err := LoadBigQueryInserterConfigFromEnv()
	require.NoError(t, err)

	bqClientForService := newEmulatorBigQueryClient(ctx, t, bqInserterCfg.ProjectID)
	require.NotNil(t, bqClientForService)
	defer bqClientForService.Close()

	realInserter, err := NewBigQueryInserter(ctx, bqClientForService, bqInserterCfg, logger.With().Str("component", "RealBQInserter").Logger())
	require.NoError(t, err, "Failed to create real BigQueryInserter for emulator.")

	processingService, err := NewProcessingService(ctx, serviceCfg, realInserter, logger)
	require.NoError(t, err)

	go func() {
		_ = processingService.Start()
	}()

	const messageCount = 7
	pubsubTestPublisherClient, err := pubsub.NewClient(ctx, testProjectID, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer pubsubTestPublisherClient.Close()
	inputTopic := pubsubTestPublisherClient.Topic(testInputTopicID)
	defer inputTopic.Stop()

	var lastTestPayload GardenMonitorPayload
	for i := 0; i < messageCount; i++ {
		testPayload := GardenMonitorPayload{
			UID:          testDeviceUID,
			SIM:          "8944500100200300400",
			RSSI:         "-85dBm",
			Version:      "1.1.0",
			Sequence:     1337 + i,
			Battery:      95 - i,
			Temperature:  28,
			Humidity:     62,
			SoilMoisture: 550,
			WaterFlow:    0,
			WaterQuality: 750,
			TankLevel:    88,
			AmbientLight: 12000,
		}
		lastTestPayload = testPayload

		testUpstreamMsg := GardenMonitorMessage{
			Topic:     "devices/garden-monitor/telemetry",
			MessageID: "test-message-id-" + strconv.Itoa(i),
			Timestamp: time.Now().UTC().Truncate(time.Second),
			Payload:   &testPayload,
		}
		msgDataBytes, err := json.Marshal(testUpstreamMsg)
		require.NoError(t, err)

		pubResult := inputTopic.Publish(ctx, &pubsub.Message{Data: msgDataBytes})
		_, err = pubResult.Get(ctx)
		require.NoError(t, err)
	}
	t.Logf("%d test messages published to Pub/Sub topic: %s", messageCount, testInputTopicID)

	// Wait for the first batch to be flushed by timeout to ensure some data is there.
	time.Sleep(serviceCfg.FlushTimeout + 1*time.Second)

	// Stop the service to ensure the final batch is flushed *before* verification begins.
	processingService.Stop()
	// Give the emulator a moment to commit the final streamed writes.
	time.Sleep(2 * time.Second)

	// --- Verification Step ---
	queryClient := newEmulatorBigQueryClient(ctx, t, testProjectID)
	defer queryClient.Close()

	queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid ORDER BY sequence",
		testBigQueryDatasetID, testBigQueryTableID)
	query := queryClient.Query(queryString)
	query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testDeviceUID}}

	it, err := query.Read(ctx)
	require.NoError(t, err, "query.Read failed")

	var receivedRows []GardenMonitorPayload
	for {
		var row GardenMonitorPayload
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err, "it.Next failed")
		receivedRows = append(receivedRows, row)
	}

	require.Len(t, receivedRows, messageCount, "The number of rows in BigQuery does not match the number of messages sent.")

	finalRow := receivedRows[len(receivedRows)-1]
	assert.Equal(t, lastTestPayload.UID, finalRow.UID, "UID mismatch")
	assert.Equal(t, lastTestPayload.SIM, finalRow.SIM, "SIM mismatch")
	assert.Equal(t, lastTestPayload.Sequence, finalRow.Sequence, "Sequence mismatch")
	assert.Equal(t, lastTestPayload.Battery, finalRow.Battery, "Battery mismatch")

	t.Logf("Successfully verified %d rows in BigQuery for UID: %s", len(receivedRows), testDeviceUID)
	t.Logf("Full flow integration test completed.")
}
