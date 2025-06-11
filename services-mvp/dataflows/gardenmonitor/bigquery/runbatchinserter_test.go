//go:build integration

package main_test

import (
	"bigquery/bqinit"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Constants for the integration test environment ---
const (
	testProjectID           = "test-garden-project"
	testInputTopicID        = "garden-monitor-topic"
	testInputSubscriptionID = "garden-monitor-sub"
	testBigQueryDatasetID   = "garden_data_dataset"
	testBigQueryTableID     = "monitor_payloads"
	testHTTPPort            = ":8899"
	testDeviceUID           = "E2E_GARDEN_MONITOR_001"

	// Pub/Sub Emulator Test Config
	testPubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort  = "8085/tcp"

	// BigQuery Emulator Test Config
	testBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testBigQueryGRPCPortStr   = "9060"
	testBigQueryRestPortStr   = "9050"
	testBigQueryGRPCPort      = testBigQueryGRPCPortStr + "/tcp"
	testBigQueryRestPort      = testBigQueryRestPortStr + "/tcp"
)

// --- Integration Test for the Service ---

func TestGardenMonitorService_FullFlow(t *testing.T) {
	// Setup is now performed per-test, ensuring a clean environment.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Setting up Pub/Sub emulator for test...")
	_, pubsubCleanup := setupPubSubEmulatorForProcessingTest(ctx, t)
	defer pubsubCleanup()

	log.Info().Msg("Setting up BigQuery emulator for test...")
	bqCleanup := setupBigQueryEmulatorForProcessingTest(ctx, t)
	defer bqCleanup()

	// --- 1. Configure the application for the test environment ---
	cfg := &bqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  testHTTPPort,
		ProjectID: testProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			SubscriptionID: testInputSubscriptionID,
		},
		BigQuery: struct {
			DatasetID       string `mapstructure:"dataset_id"`
			TableID         string `mapstructure:"table_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			DatasetID: testBigQueryDatasetID,
			TableID:   testBigQueryTableID,
		},
		BatchProcessing: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{
			NumWorkers:   2,
			BatchSize:    5,
			FlushTimeout: 3 * time.Second,
		},
	}

	// --- 2. Build and Start the Service ---
	testLogger := log.With().Str("service", "garden-monitor-test").Logger()

	// Create one client using the test helper and reuse it for the inserter and verification.
	// This aligns with the simpler, working approach from the library's integration test.
	bqClient := newEmulatorBigQueryClient(ctx, t, cfg.ProjectID)
	defer bqClient.Close()

	consumer, err := bqstore.NewGooglePubSubConsumer(ctx, &bqstore.GooglePubSubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}, testLogger)
	require.NoError(t, err)

	inserter, err := bqstore.NewBigQueryInserter[types.GardenMonitorPayload](ctx, bqClient, &bqstore.BigQueryInserterConfig{
		ProjectID: cfg.ProjectID,
		DatasetID: cfg.BigQuery.DatasetID,
		TableID:   cfg.BigQuery.TableID,
	}, testLogger)
	require.NoError(t, err)

	batcher := bqstore.NewBatchInserter[types.GardenMonitorPayload](&bqstore.BatchInserterConfig{
		BatchSize:    cfg.BatchProcessing.BatchSize,
		FlushTimeout: cfg.BatchProcessing.FlushTimeout,
	}, inserter, testLogger)

	pipeline, err := bqstore.NewBatchingService[types.GardenMonitorPayload](&bqstore.ServiceConfig{
		NumProcessingWorkers: cfg.BatchProcessing.NumWorkers,
	}, consumer, batcher, types.GardenMonitorDecoder, testLogger)
	require.NoError(t, err)

	server := bqinit.NewServer(cfg, pipeline, testLogger)

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Server failed during test execution")
		}
	}()
	time.Sleep(500 * time.Millisecond)

	// --- 3. Test Health Check Endpoint ---
	resp, err := http.Get("http://localhost" + testHTTPPort + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Health check should return 200 OK")

	// --- 4. Publish Test Messages ---
	const messageCount = 7
	var lastTestPayload types.GardenMonitorPayload
	publisherClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer publisherClient.Close()
	topic := publisherClient.Topic(testInputTopicID)
	defer topic.Stop()

	for i := 0; i < messageCount; i++ {
		payload := types.GardenMonitorPayload{
			DE:       testDeviceUID,
			Sequence: 100 + i,
			Battery:  88 - i,
		}
		lastTestPayload = payload
		msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &payload})
		require.NoError(t, err)
		res := topic.Publish(ctx, &pubsub.Message{Data: msgBytes})
		_, err = res.Get(ctx)
		require.NoError(t, err)
	}

	// --- 5. Stop the Server and Verify Data ---
	time.Sleep(4 * time.Second) // Allow time for flush
	server.Shutdown()

	// Use the same client for verification.
	queryStr := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid ORDER BY sequence",
		testBigQueryDatasetID, testBigQueryTableID)
	query := bqClient.Query(queryStr)
	query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testDeviceUID}}

	it, err := query.Read(ctx)
	require.NoError(t, err)

	var receivedRows []types.GardenMonitorPayload
	for {
		var row types.GardenMonitorPayload
		if err := it.Next(&row); err == iterator.Done {
			break
		}
		require.NoError(t, err)
		receivedRows = append(receivedRows, row)
	}

	assert.Len(t, receivedRows, messageCount, "Incorrect number of rows found in BigQuery")
	assert.Equal(t, lastTestPayload.Sequence, receivedRows[len(receivedRows)-1].Sequence, "Data mismatch in last row")
}

// --- Emulator Setup Helpers ---

// newEmulatorBigQueryClient uses the BIGQUERY_API_ENDPOINT (REST) for management operations.
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

// setupPubSubEmulatorForProcessingTest uses the working setup from the library test.
func setupPubSubEmulatorForProcessingTest(ctx context.Context, t *testing.T) (emulatorHost string, cleanupFunc func()) {
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
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

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

// setupBigQueryEmulatorForProcessingTest is the corrected setup function.
func setupBigQueryEmulatorForProcessingTest(ctx context.Context, t *testing.T) (cleanupFunc func()) {
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
	schema, err := bigquery.InferSchema(types.GardenMonitorPayload{})
	require.NoError(t, err, "Failed to infer schema from types.GardenMonitorPayload")

	tableMeta := &bigquery.TableMetadata{Name: testBigQueryTableID, Schema: schema}
	err = table.Create(ctx, tableMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create table '%s' on BQ emulator", testBigQueryTableID)
	}

	return func() {
		require.NoError(t, container.Terminate(ctx))
	}
}
