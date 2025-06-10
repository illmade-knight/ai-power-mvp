//go:build integration

package bqstore_test

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

	// Import the new generic bqstore package
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore" // <-- IMPORTANT: Replace with the actual import path of your new library

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Test-Specific Data Structures ---
// These are defined here to make the test self-contained. In a real project,
// they might be imported from a types package.

type GardenMonitorMessage struct {
	Topic     string                `json:"topic"`
	MessageID string                `json:"message_id"`
	Timestamp time.Time             `json:"timestamp"`
	Payload   *GardenMonitorPayload `json:"payload"`
}

type GardenMonitorPayload struct {
	UID          string `json:"DE" bigquery:"uid"`
	SIM          string `json:"SIM" bigquery:"sim"`
	RSSI         string `json:"RS" bigquery:"rssi"`
	Version      string `json:"VR" bigquery:"version"`
	Sequence     int    `json:"SQ" bigquery:"sequence"`
	Battery      int    `json:"BA" bigquery:"battery"`
	Temperature  int    `json:"TM" bigquery:"temperature"`
	Humidity     int    `json:"HM" bigquery:"humidity"`
	SoilMoisture int    `json:"SM1" bigquery:"soil_moisture"`
	WaterFlow    int    `json:"FL1" bigquery:"waterflow"`
	WaterQuality int    `json:"WQ" bigquery:"water_quality"`
	TankLevel    int    `json:"DL1" bigquery:"tank_level"`
	AmbientLight int    `json:"AM" bigquery:"ambient_light"`
}

// --- Constants for the integration test environment ---
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

// --- Emulator Setup Helpers (Unchanged as requested) ---

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

func setupPubSubEmulatorForProcessingTest(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	// ... This function's implementation is identical to the original ...
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

func setupBigQueryEmulatorForProcessingTest(t *testing.T, ctx context.Context) (cleanupFunc func()) {
	// ... This function's implementation is identical to the original ...
	// The explicit table creation with a known schema is still useful for a test.
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

// TestProcessingService_Integration_FullFlow tests the entire generic bqstore flow.
func TestProcessingService_Integration_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorForProcessingTest(t, ctx)
	defer pubsubEmulatorCleanup()
	bqEmulatorCleanup := setupBigQueryEmulatorForProcessingTest(t, ctx)
	defer bqEmulatorCleanup()

	// --- Configuration setup ---
	var logBuf bytes.Buffer
	writer := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}, &logBuf)
	logger := zerolog.New(writer).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// Create configs directly instead of loading from env for better test isolation.
	consumerCfg := &bqstore.GooglePubSubConsumerConfig{
		ProjectID:      testProjectID,
		SubscriptionID: testInputSubscriptionID,
	}
	serviceCfg := &bqstore.ServiceConfig{
		NumProcessingWorkers: 2,
	}
	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    5,
		FlushTimeout: 10 * time.Second,
	}
	bqInserterCfg := &bqstore.BigQueryInserterConfig{
		ProjectID: testProjectID,
		DatasetID: testBigQueryDatasetID,
		TableID:   testBigQueryTableID,
	}

	// --- Define the Decoder for the specific test type ---
	// This is the implementation of the PayloadDecoder[T] for our GardenMonitorPayload.
	gardenPayloadDecoder := func(payload []byte) (*GardenMonitorPayload, error) {
		var upstreamMsg GardenMonitorMessage
		if err := json.Unmarshal(payload, &upstreamMsg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal upstream message: %w", err)
		}
		// The payload inside the message could be nil.
		return upstreamMsg.Payload, nil
	}

	// --- Initialize Generic Pipeline Components ---
	consumer, err := bqstore.NewGooglePubSubConsumer(ctx, consumerCfg, logger)
	require.NoError(t, err)

	bqClient := newEmulatorBigQueryClient(ctx, t, bqInserterCfg.ProjectID)
	require.NotNil(t, bqClient)
	defer bqClient.Close()

	// Instantiate generic components with the concrete type: GardenMonitorPayload
	bigQueryInserter, err := bqstore.NewBigQueryInserter[GardenMonitorPayload](ctx, bqClient, bqInserterCfg, logger)
	require.NoError(t, err)

	batchInserter := bqstore.NewBatchInserter[GardenMonitorPayload](batcherCfg, bigQueryInserter, logger)

	processingService, err := bqstore.NewProcessingService[GardenMonitorPayload](serviceCfg, consumer, batchInserter, gardenPayloadDecoder, logger)
	require.NoError(t, err)

	// --- Test Execution ---
	go func() {
		err := processingService.Start()
		assert.NoError(t, err, "ProcessingService.Start() should not return an error on graceful shutdown")
	}()

	const messageCount = 7
	pubsubTestPublisherClient, err := pubsub.NewClient(ctx, testProjectID, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	require.NoError(t, err)
	defer pubsubTestPublisherClient.Close()
	inputTopic := pubsubTestPublisherClient.Topic(testInputTopicID)
	defer inputTopic.Stop()

	var lastTestPayload GardenMonitorPayload
	// Publishing logic is identical to original test.
	for i := 0; i < messageCount; i++ {
		testPayload := GardenMonitorPayload{
			UID:      testDeviceUID,
			Sequence: 1337 + i,
			Battery:  95 - i,
			// ... (other fields omitted for brevity but would be here) ...
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

	// Stop the service to flush the final batch.
	time.Sleep(2 * time.Second) // Give workers time to process some messages.
	processingService.Stop()
	time.Sleep(2 * time.Second) // Give the emulator a moment to commit writes.

	// --- Verification Step (Identical to original test) ---
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

	require.Len(t, receivedRows, messageCount, "The number of rows in BigQuery should match the number of messages sent.")

	finalRow := receivedRows[len(receivedRows)-1]
	assert.Equal(t, lastTestPayload.UID, finalRow.UID, "DE mismatch")
	assert.Equal(t, lastTestPayload.Sequence, finalRow.Sequence, "Sequence mismatch")
	assert.Equal(t, lastTestPayload.Battery, finalRow.Battery, "Battery mismatch")

	t.Logf("Successfully verified %d rows in BigQuery for DE: %s", len(receivedRows), testDeviceUID)
	t.Logf("Full flow integration test completed.")
}
