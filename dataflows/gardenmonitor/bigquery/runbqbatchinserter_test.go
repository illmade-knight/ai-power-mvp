//go:build integration

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/bigquery/bqinit"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Constants for the integration test environment (Unchanged) ---
const (
	testProjectID             = "test-garden-project"
	testInputTopicID          = "garden-monitor-topic"
	testInputSubscriptionID   = "garden-monitor-sub"
	testBigQueryDatasetID     = "garden_data_dataset"
	testBigQueryTableID       = "monitor_payloads"
	testHTTPPort              = ":8899"
	testDeviceUID             = "E2E_GARDEN_MONITOR_001"
	testPubSubEmulatorImage   = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort    = "8085"
	testBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testBigQueryGRPCPort      = "9060"
	testBigQueryRestPort      = "9050"
	//testBigQueryGRPCNatPort      = testBigQueryGRPCPort + "/tcp"
	//testBigQueryRestNatPort      = testBigQueryRestPort + "/tcp"
)

// --- Integration Test for the Service ---

func TestGardenMonitorService_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Setting up Pub/Sub emulator for test...")
	//_, pubsubCleanup := setupPubSubEmulatorForProcessingTest(ctx, t)
	pubsubOptions, pubsubCleanup := emulators.SetupPubSubEmulator(t, ctx, &emulators.PubsubConfig{
		GCImageContainer: emulators.GCImageContainer{
			ImageContainer: emulators.ImageContainer{
				EmulatorImage:    testPubSubEmulatorImage,
				EmulatorHTTPPort: testPubSubEmulatorPort,
			},
			ProjectID:       testProjectID,
			SetEnvVariables: true,
		},
		TopicSubs: map[string]string{testInputTopicID: testInputSubscriptionID},
	})
	defer pubsubCleanup()

	log.Info().Msg("Setting up BigQueryConfig emulator for test...")

	bqOptions, bqCleanup := emulators.SetupBigQueryEmulator(t, ctx, emulators.BigQueryConfig{
		GCImageContainer: emulators.GCImageContainer{
			ImageContainer: emulators.ImageContainer{
				EmulatorImage:    testBigQueryEmulatorImage,
				EmulatorHTTPPort: testBigQueryRestPort,
				EmulatorGRPCPort: testBigQueryGRPCPort,
			},
			ProjectID:       testProjectID,
			SetEnvVariables: true,
		},
		DatasetTables: map[string]string{testBigQueryDatasetID: testBigQueryTableID},
		Schemas:       map[string]interface{}{testBigQueryTableID: types.GardenMonitorMessage{}},
	})
	defer bqCleanup()

	// --- 1. Configure the application for the test environment (Unchanged) ---
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
		BigQueryConfig: bqstore.BigQueryDatasetConfig{
			DatasetID: testBigQueryDatasetID,
			TableID:   testBigQueryTableID,
		},
		BatchProcessing: struct {
			bqstore.BatchInserterConfig `mapstructure:"datasetup"`
			NumWorkers                  int `mapstructure:"num_workers"`
		}{bqstore.BatchInserterConfig{
			BatchSize:    10,
			FlushTimeout: 10,
		}, 2},
	}

	// --- 2. Build and Start the Service (Updated) ---
	testLogger := log.With().Str("service", "garden-monitor-test").Logger()
	bqClient, err := bigquery.NewClient(ctx, testProjectID, bqOptions...)
	require.NotNil(t, bqClient)
	defer bqClient.Close()

	// Create the consumer from the shared package.
	// definitely want to remove emulator stuff from consumers
	//var opts []option.ClientOption
	//pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	//if pubsubEmulatorHost != "" {
	//	logger.Info().Str("emulator_host", pubsubEmulatorHost).Str("subscription_id", cfg.SubscriptionID).Msg("Using Pub/Sub emulator for consumer.")
	//	opts = append(opts, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	//}
	consumer, err := consumers.NewGooglePubSubConsumer(ctx, &consumers.GooglePubSubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}, pubsubOptions, testLogger)
	require.NoError(t, err)

	// Create the bqstore components.
	inserter, err := bqstore.NewBigQueryInserter[types.GardenMonitorReadings](ctx, bqClient, &bqstore.BigQueryDatasetConfig{
		ProjectID: cfg.ProjectID,
		DatasetID: cfg.BigQueryConfig.DatasetID,
		TableID:   cfg.BigQueryConfig.TableID,
	}, testLogger)
	require.NoError(t, err)

	batcher := bqstore.NewBatcher[types.GardenMonitorReadings](&bqstore.BatchInserterConfig{
		BatchSize:    cfg.BatchProcessing.BatchSize,
		FlushTimeout: cfg.BatchProcessing.FlushTimeout,
	}, inserter, testLogger)

	// Use the new service constructor, which correctly assembles the bigQueryService.
	bigQueryService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batcher,
		types.ConsumedMessageTransformer,
		testLogger,
	)
	require.NoError(t, err)

	server := bqinit.NewServer(cfg, bigQueryService, testLogger)

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Server failed during test execution")
		}
	}()
	time.Sleep(500 * time.Millisecond)

	// --- 3. Test Health Check Endpoint (Unchanged) ---
	resp, err := http.Get("http://localhost" + testHTTPPort + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Health check should return 200 OK")

	// --- 4. Publish Test Messages (Unchanged) ---
	const messageCount = 7
	var lastTestPayload types.GardenMonitorReadings
	publisherClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer publisherClient.Close()
	topic := publisherClient.Topic(testInputTopicID)
	defer topic.Stop()

	for i := 0; i < messageCount; i++ {
		payload := types.GardenMonitorReadings{DE: testDeviceUID, Sequence: 100 + i, Battery: 88 - i}
		lastTestPayload = payload
		msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &payload})
		require.NoError(t, err)
		res := topic.Publish(ctx, &pubsub.Message{Data: msgBytes})
		_, err = res.Get(ctx)
		require.NoError(t, err)
	}

	// --- 5. Stop the Server and Verify Data (Unchanged) ---
	time.Sleep(4 * time.Second) // Allow time for flush
	server.Shutdown()

	queryStr := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid ORDER BY sequence",
		testBigQueryDatasetID, testBigQueryTableID)
	query := bqClient.Query(queryStr)
	query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testDeviceUID}}

	it, err := query.Read(ctx)
	require.NoError(t, err)

	var receivedRows []types.GardenMonitorReadings
	for {
		var row types.GardenMonitorReadings
		if err := it.Next(&row); err == iterator.Done {
			break
		}
		require.NoError(t, err)
		receivedRows = append(receivedRows, row)
	}

	assert.Len(t, receivedRows, messageCount, "Incorrect number of rows found in BigQueryConfig")
	assert.Equal(t, lastTestPayload.Sequence, receivedRows[len(receivedRows)-1].Sequence, "Data mismatch in last row")
}

// --- Emulator Setup Helpers (Unchanged) ---
func newEmulatorBigQueryClient(ctx context.Context, t *testing.T, projectID string) *bigquery.Client {
	t.Helper()
	emulatorHost := os.Getenv("BIGQUERY_API_ENDPOINT")
	require.NotEmpty(t, emulatorHost, "BIGQUERY_API_ENDPOINT env var must be set")
	client, err := bigquery.NewClient(ctx, projectID, option.WithEndpoint(emulatorHost), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{}))
	require.NoError(t, err)
	return client
}

func setupPubSubEmulatorForProcessingTest(ctx context.Context, t *testing.T) (string, func()) {
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
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	adminClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer adminClient.Close()
	topic, err := adminClient.CreateTopic(ctx, testInputTopicID)
	require.NoError(t, err)
	_, err = adminClient.CreateSubscription(ctx, testInputSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err)
	return emulatorHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupBigQueryEmulatorForProcessingTest(ctx context.Context, t *testing.T) func() {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testBigQueryEmulatorImage,
		ExposedPorts: []string{testBigQueryGRPCPort, testBigQueryRestPort},
		Cmd:          []string{"--project=" + testProjectID, "--port=" + testBigQueryRestPort, "--grpc-port=" + testBigQueryGRPCPort},
		WaitingFor:   wait.ForAll(wait.ForListeningPort(testBigQueryGRPCPort), wait.ForListeningPort(testBigQueryRestPort)),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	grpcPort, _ := container.MappedPort(ctx, testBigQueryGRPCPort)
	restPort, _ := container.MappedPort(ctx, testBigQueryRestPort)
	os.Setenv("BIGQUERY_EMULATOR_HOST", fmt.Sprintf("%s:%s", host, grpcPort.Port()))
	os.Setenv("BIGQUERY_API_ENDPOINT", fmt.Sprintf("http://%s:%s", host, restPort.Port()))
	adminClient := newEmulatorBigQueryClient(ctx, t, testProjectID)
	defer adminClient.Close()
	err = adminClient.Dataset(testBigQueryDatasetID).Create(ctx, &bigquery.DatasetMetadata{})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err)
	}
	schema, _ := bigquery.InferSchema(types.GardenMonitorReadings{})
	err = adminClient.Dataset(testBigQueryDatasetID).Table(testBigQueryTableID).Create(ctx, &bigquery.TableMetadata{Schema: schema})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err)
	}
	return func() { require.NoError(t, container.Terminate(ctx)) }
}
