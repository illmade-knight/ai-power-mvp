//go:build integration

package bqstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/consumers" // Using shared consumers package
	//"github.com/illmade-knight/ai-power-mpv/pkg/types"    // Using shared types package
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Constants for the integration test environment (Unchanged) ---
const (
	testPubSubEmulatorImage   = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort    = "8085/tcp"
	testProjectID             = "test-garden-project"
	testInputTopicID          = "garden-monitor-topic"
	testInputSubscriptionID   = "garden-monitor-sub"
	testBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testBigQueryGRPCPortStr   = "9060"
	testBigQueryRestPortStr   = "9050"
	testBigQueryGRPCPort      = testBigQueryGRPCPortStr + "/tcp"
	testBigQueryRestPort      = testBigQueryRestPortStr + "/tcp"
	testBigQueryDatasetID     = "garden_data_dataset"
	testBigQueryTableID       = "monitor_payloads"
	testDeviceUID             = "GARDEN_MONITOR_001"
)

// --- Emulator Setup Helpers (Unchanged) ---
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

func setupPubSubEmulatorForProcessingTest(t *testing.T, ctx context.Context) (string, func()) {
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
	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator container started, listening on: %s", emulatorHost)
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

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

func setupBigQueryEmulatorForProcessingTest(t *testing.T, ctx context.Context) func() {
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
	schema, err := bigquery.InferSchema(types.GardenMonitorReadings{})
	require.NoError(t, err, "Failed to infer schema from GardenMonitorReadings")

	tableMeta := &bigquery.TableMetadata{Name: testBigQueryTableID, Schema: schema}
	err = table.Create(ctx, tableMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create table '%s' on BQ emulator", testBigQueryTableID)
	}
	return func() {
		require.NoError(t, container.Terminate(ctx))
	}
}

type TestUpstreamMessage struct {
	Topic     string
	MessageID string
	Timestamp time.Time
	Payload   *types.GardenMonitorReadings
}

// TestBigQueryService_Integration_FullFlow tests the entire generic bqstore flow.
func TestBigQueryService_Integration_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, pubsubEmulatorCleanup := setupPubSubEmulatorForProcessingTest(t, ctx)
	defer pubsubEmulatorCleanup()
	bqEmulatorCleanup := setupBigQueryEmulatorForProcessingTest(t, ctx)
	defer bqEmulatorCleanup()

	// --- Configuration setup (Unchanged) ---
	var logBuf bytes.Buffer
	writer := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}, &logBuf)
	logger := zerolog.New(writer).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	consumerCfg := &consumers.GooglePubSubConsumerConfig{
		ProjectID:      testProjectID,
		SubscriptionID: testInputSubscriptionID,
	}
	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    5,
		FlushTimeout: 10 * time.Second,
	}
	bqInserterCfg := &bqstore.BigQueryDatasetConfig{
		ProjectID: testProjectID,
		DatasetID: testBigQueryDatasetID,
		TableID:   testBigQueryTableID,
	}

	// --- Define the Decoder (Unchanged) ---
	//gardenPayloadDecoder := func(payload []byte) (*GardenMonitorReadings, error) {
	//	var upstreamMsg GardenMonitorMessage
	//	if err := json.Unmarshal(payload, &upstreamMsg); err != nil {
	//		return nil, fmt.Errorf("failed to unmarshal upstream message: %w", err)
	//	}
	//	return upstreamMsg.Payload, nil
	//}

	//gardenPayloadTransformer := func(msg types.ConsumedMessage) (*types.GardenMonitorReadings, bool, error) {
	//	var upstreamMsg types.GardenMonitorMessage
	//	if err := json.Unmarshal(msg.Payload, &upstreamMsg); err != nil {
	//		// This is a malformed message, return an error to Nack it.
	//		return nil, false, fmt.Errorf("failed to unmarshal upstream message: %w", err)
	//	}
	//	// If the inner payload is nil, we want to skip this message but still Ack it.
	//	if upstreamMsg.Payload == nil {
	//		return nil, true, nil
	//	}
	//	// Success case
	//	return upstreamMsg.Payload, false, nil
	//}

	// --- Initialize Components with new, refactored structure ---
	consumer, err := consumers.NewGooglePubSubConsumer(ctx, consumerCfg, logger)
	require.NoError(t, err)

	bqClient := newEmulatorBigQueryClient(ctx, t, bqInserterCfg.ProjectID)
	require.NotNil(t, bqClient)
	defer bqClient.Close()

	// *** REFACTORED PART: Use the new, single convenience constructor ***
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, batcherCfg, bqInserterCfg, logger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new service constructor ***
	numWorkers := 2
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](numWorkers, consumer, batchInserter, types.ConsumedMessageTransformer, logger)
	require.NoError(t, err)

	// --- Test Execution (Unchanged) ---
	go func() {
		err := processingService.Start()
		assert.NoError(t, err, "ProcessingService.Start() should not return an error on graceful shutdown")
	}()

	const messageCount = 7
	pubsubTestPublisherClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer pubsubTestPublisherClient.Close()
	inputTopic := pubsubTestPublisherClient.Topic(testInputTopicID)
	defer inputTopic.Stop()

	var lastTestPayload types.GardenMonitorReadings
	for i := 0; i < messageCount; i++ {
		testPayload := types.GardenMonitorReadings{
			DE:       testDeviceUID,
			Sequence: 1337 + i,
			Battery:  95 - i,
		}
		lastTestPayload = testPayload

		testUpstreamMsg := TestUpstreamMessage{
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

	time.Sleep(2 * time.Second)
	processingService.Stop()
	time.Sleep(2 * time.Second)

	// --- Verification Step (Unchanged) ---
	queryClient := newEmulatorBigQueryClient(ctx, t, testProjectID)
	defer queryClient.Close()

	queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid ORDER BY sequence", testBigQueryDatasetID, testBigQueryTableID)
	query := queryClient.Query(queryString)
	query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testDeviceUID}}

	it, err := query.Read(ctx)
	require.NoError(t, err, "query.Read failed")

	var receivedRows []types.GardenMonitorReadings
	for {
		var row types.GardenMonitorReadings
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err, "it.Next failed")
		receivedRows = append(receivedRows, row)
	}

	require.Len(t, receivedRows, messageCount, "The number of rows in BigQuery should match the number of messages sent.")

	finalRow := receivedRows[len(receivedRows)-1]
	assert.Equal(t, lastTestPayload.DE, finalRow.DE, "DE mismatch")
	assert.Equal(t, lastTestPayload.Sequence, finalRow.Sequence, "Sequence mismatch")
	assert.Equal(t, lastTestPayload.Battery, finalRow.Battery, "Battery mismatch")

	t.Logf("Successfully verified %d rows in BigQuery for DE: %s", len(receivedRows), testDeviceUID)
	t.Logf("Full flow integration test completed.")
}
