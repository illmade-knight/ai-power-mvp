//go:build integration

package xdevice

import (
	"bytes"
	"context"
	telemetry "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry"
	"net/http"

	// "encoding/binary" // No longer needed here if createValidTestHexPayload is external
	// "encoding/hex"    // No longer needed here
	"encoding/json"
	"errors"
	"fmt"
	"os" // Needed for os.Getenv in the new helper
	// "math"            // No longer needed here
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

const (
	// Pub/Sub Emulator Test Config for Processing Service
	testProcessingPubSubEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testProcessingPubSubEmulatorPort  = "8085/tcp"
	testProcessingPubSubProjectID     = "test-proc-project" // Used for PubSub and BQ emulators
	testProcessingInputTopicID        = "xdevice-input-topic"
	testProcessingInputSubscriptionID = "xdevice-input-sub"

	// BigQuery Emulator Test Config for Processing Service
	testProcessingBigQueryEmulatorImage = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testProcessingBigQueryGRPCPortStr   = "9060" // goccy/bigquery-emulator default gRPC port (as per user's provided docs)
	testProcessingBigQueryRestPortStr   = "9050" // goccy/bigquery-emulator default HTTP/REST API port (as per user's provided docs)
	testProcessingBigQueryGRPCPort      = testProcessingBigQueryGRPCPortStr + "/tcp"
	testProcessingBigQueryRestPort      = testProcessingBigQueryRestPortStr + "/tcp"
	testProcessingBigQueryDatasetID     = "meter_data_dataset"
	testProcessingBigQueryTableID       = "meter_readings"

	// Device specific constants for test message
	testProcessingDeviceEUI = "PROC_EUI_001"
	testProcessingDeviceUID = "P001"
)

// --- Test Setup Helpers ---

// newEmulatorBigQueryClient uses the configuration that was working for admin client (REST focused).
// It relies on BIGQUERY_API_ENDPOINT environment variable being set by the caller (using t.Setenv).
func newEmulatorBigQueryClient(ctx context.Context, t *testing.T, projectID string) *bigquery.Client {
	t.Helper()

	emulatorHost := os.Getenv("BIGQUERY_API_ENDPOINT") // This should be set by t.Setenv in the calling test
	//emulatorHost := os.Getenv("BIGQUERY_EMULATOR_HOST") // This should be set by t.Setenv in the calling test
	require.NotEmpty(t, emulatorHost, "BIGQUERY_API_ENDPOINT env var must be set before calling newEmulatorBigQueryClient")
	require.NotEmpty(t, projectID, "projectID must be set for newEmulatorBigQueryClient")

	clientOpts := []option.ClientOption{
		option.WithEndpoint(emulatorHost), // Point to the REST/grpc endpoint
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}), // Use a plain http.Client to force HTTP
	}

	client, err := bigquery.NewClient(ctx, projectID, clientOpts...)
	require.NoError(t, err, "newEmulatorBigQueryClient: Failed to create BigQuery client. EmulatorHost: %s", emulatorHost)
	return client
}

func setupPubSubEmulatorForProcessingTest(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testProcessingPubSubEmulatorImage,
		ExposedPorts: []string{testProcessingPubSubEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testProcessingPubSubProjectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testProcessingPubSubEmulatorPort, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testProcessingPubSubEmulatorPort)
	require.NoError(t, err)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator (for processing service) container started, listening on: %s", emulatorHost)
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	adminClient, err := pubsub.NewClient(ctx, testProcessingPubSubProjectID)
	require.NoError(t, err)
	defer adminClient.Close()
	topic := adminClient.Topic(testProcessingInputTopicID)
	if exists, err := topic.Exists(ctx); err == nil && !exists {
		_, err = adminClient.CreateTopic(ctx, testProcessingInputTopicID)
		require.NoError(t, err)
		t.Logf("Created Pub/Sub input topic '%s' on emulator", testProcessingInputTopicID)
	} else {
		require.NoError(t, err)
	}
	sub := adminClient.Subscription(testProcessingInputSubscriptionID)
	if exists, err := sub.Exists(ctx); err == nil && !exists {
		_, err = adminClient.CreateSubscription(ctx, testProcessingInputSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
		require.NoError(t, err)
		t.Logf("Created Pub/Sub input subscription '%s' on emulator", testProcessingInputSubscriptionID)
	} else {
		require.NoError(t, err)
	}
	return emulatorHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupBigQueryEmulatorForProcessingTest(t *testing.T, ctx context.Context) (emulatorGRPCHost, emulatorRESTHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testProcessingBigQueryEmulatorImage,
		ExposedPorts: []string{testProcessingBigQueryGRPCPort, testProcessingBigQueryRestPort},
		Cmd: []string{
			"--project=" + testProcessingPubSubProjectID,
			"--port=" + testProcessingBigQueryRestPortStr,
			"--grpc-port=" + testProcessingBigQueryGRPCPortStr,
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(testProcessingBigQueryGRPCPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(testProcessingBigQueryRestPort).WithStartupTimeout(60*time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start BigQuery emulator container. Check container logs and command arguments.")

	host, err := container.Host(ctx)
	require.NoError(t, err)

	grpcMappedPort, err := container.MappedPort(ctx, testProcessingBigQueryGRPCPort)
	require.NoError(t, err)
	emulatorGRPCHost = fmt.Sprintf("%s:%s", host, grpcMappedPort.Port()) // Just host:port
	t.Logf("BigQuery emulator container started, gRPC on: %s", emulatorGRPCHost)

	restMappedPort, err := container.MappedPort(ctx, testProcessingBigQueryRestPort)
	require.NoError(t, err)
	emulatorRESTHost = fmt.Sprintf("http://%s:%s", host, restMappedPort.Port()) // Includes http:// scheme
	t.Logf("BigQuery emulator container started, REST on: %s", emulatorRESTHost)

	// Set environment variables *before* creating the adminBqClient
	t.Setenv("GOOGLE_CLOUD_PROJECT", testProcessingPubSubProjectID)
	t.Setenv("BIGQUERY_EMULATOR_HOST", emulatorGRPCHost) // For gRPC based API calls by client library
	t.Setenv("BIGQUERY_API_ENDPOINT", emulatorRESTHost)  // For REST based API calls by client library

	// Use the newEmulatorBigQueryClient helper, which now uses BIGQUERY_API_ENDPOINT
	adminBqClient := newEmulatorBigQueryClient(ctx, t, testProcessingPubSubProjectID)
	require.NotNil(t, adminBqClient, "Admin BQ client should not be nil")
	defer adminBqClient.Close()

	dataset := adminBqClient.Dataset(testProcessingBigQueryDatasetID)
	err = dataset.Create(ctx, &bigquery.DatasetMetadata{Name: testProcessingBigQueryDatasetID})
	if err != nil {
		if strings.Contains(err.Error(), "http: server gave HTTP response to HTTPS client") {
			t.Logf("CRITICAL: dataset.Create failed with HTTP/HTTPS mismatch. BIGQUERY_API_ENDPOINT was: %s", emulatorRESTHost)
		}
		if !strings.Contains(err.Error(), "Already Exists") {
			require.NoError(t, err, "Failed to create dataset '%s' on BQ emulator.", testProcessingBigQueryDatasetID)
		} else {
			t.Logf("BigQuery dataset '%s' already exists on emulator.", testProcessingBigQueryDatasetID)
		}
	} else {
		t.Logf("Created BigQuery dataset '%s' on emulator", testProcessingBigQueryDatasetID)
	}

	table := dataset.Table(testProcessingBigQueryTableID)
	schema, err := bigquery.InferSchema(telemetry.MeterReading{})
	require.NoError(t, err)
	tableMeta := &bigquery.TableMetadata{Name: testProcessingBigQueryTableID, Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Field: "original_mqtt_time"},
	}
	err = table.Create(ctx, tableMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create table '%s' on BQ emulator", testProcessingBigQueryTableID)
	} else if err == nil {
		t.Logf("Created BigQuery table '%s' on emulator", testProcessingBigQueryTableID)
	} else {
		t.Logf("BigQuery table '%s' already exists on emulator or other error during creation: %v", testProcessingBigQueryTableID, err)
	}

	return emulatorGRPCHost, emulatorRESTHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

func TestProcessingService_Integration_FullFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pubsubEmulatorHost, pubsubEmulatorCleanup := setupPubSubEmulatorForProcessingTest(t, ctx)
	defer pubsubEmulatorCleanup()
	bqEmulatorGRPCHost, bqEmulatorRESTHost, bqEmulatorCleanup := setupBigQueryEmulatorForProcessingTest(t, ctx)
	defer bqEmulatorCleanup()

	// Setup ALL Environment Variables for the ProcessingService *BEFORE* loading configs
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorHost)
	t.Setenv("GCP_PROJECT_ID", testProcessingPubSubProjectID)
	t.Setenv("GOOGLE_CLOUD_PROJECT", testProcessingPubSubProjectID)
	t.Setenv("PROCESSING_UPSTREAM_SUB_ENV_VAR", "XDEVICE_TEST_SUB_ID_ENV_FULLFLOW")
	t.Setenv("XDEVICE_TEST_SUB_ID_ENV_FULLFLOW", testProcessingInputSubscriptionID)

	t.Setenv("BIGQUERY_EMULATOR_HOST", bqEmulatorGRPCHost)
	t.Setenv("BIGQUERY_API_ENDPOINT", bqEmulatorRESTHost)

	t.Setenv("BQ_DATASET_ID", testProcessingBigQueryDatasetID)
	t.Setenv("BQ_TABLE_ID_METER_READINGS", testProcessingBigQueryTableID)
	t.Setenv("GCP_BQ_CREDENTIALS_FILE", "")
	t.Setenv("GCP_PUBSUB_CREDENTIALS_FILE", "")

	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	serviceCfg, err := LoadServiceConfigFromEnv()
	require.NoError(t, err)
	serviceCfg.NumProcessingWorkers = 1

	bqInserterCfg, err := LoadBigQueryInserterConfigFromEnv()
	require.NoError(t, err)

	// Use the helper to create the BigQuery client for the service's inserter
	// This client needs to support gRPC for Put and potentially REST for metadata (like table.Metadata in NewBigQueryInserter)
	// The newEmulatorBigQueryClient is now primarily REST-focused.
	// For the service inserter, which does data Puts (gRPC), we might need a gRPC-focused client.
	// However, NewBigQueryInserter also does table.Metadata().
	// Let's use the helper, ensuring BIGQUERY_API_ENDPOINT is set for REST parts,
	// and BIGQUERY_EMULATOR_HOST for gRPC parts. The helper uses WithEndpoint(BIGQUERY_API_ENDPOINT from env)
	// and WithHTTPClient. This should be fine for metadata.
	// For Put operations, the client library should ideally use gRPC via BIGQUERY_EMULATOR_HOST.
	bqClientForService := newEmulatorBigQueryClient(ctx, t, bqInserterCfg.ProjectID)
	require.NotNil(t, bqClientForService)
	defer bqClientForService.Close()

	realInserter, err := NewBigQueryInserter(ctx, bqClientForService, bqInserterCfg, logger.With().Str("component", "RealBQInserter").Logger())
	require.NoError(t, err, "Failed to create real BigQueryInserter for emulator.")

	processingService, err := NewProcessingService(ctx, serviceCfg, realInserter, logger)
	require.NoError(t, err)

	serviceErrChan := make(chan error, 1)
	go func() { serviceErrChan <- processingService.Start() }()
	defer processingService.Stop()

	select {
	case errStart := <-serviceErrChan:
		require.NoError(t, errStart, "ProcessingService failed to start")
	case <-time.After(25 * time.Second):
		t.Fatalf("Timeout waiting for ProcessingService to start. Logs:\n%s", logBuf.String())
	}
	time.Sleep(3 * time.Second)

	pubsubTestPublisherClient, err := pubsub.NewClient(ctx, testProcessingPubSubProjectID)
	require.NoError(t, err)
	defer pubsubTestPublisherClient.Close()
	inputTopic := pubsubTestPublisherClient.Topic(testProcessingInputTopicID)
	defer inputTopic.Stop()

	validHexPayload, errHex := createValidTestHexPayload(testProcessingDeviceUID, 123.45, 1.2, 2.3, 240.1, 230.5)
	require.NoError(t, errHex)
	testUpstreamMsg := ConsumedUpstreamMessage{
		DeviceEUI: testProcessingDeviceEUI, RawPayload: validHexPayload,
		OriginalMQTTTime:   time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second),
		IngestionTimestamp: time.Now().Add(-5 * time.Minute).UTC().Truncate(time.Second),
		ClientID:           "IntTestClient", LocationID: "IntTestLocation", DeviceCategory: "IntegrationSensor",
	}
	msgDataBytes, err := json.Marshal(testUpstreamMsg)
	require.NoError(t, err)
	pubResult := inputTopic.Publish(ctx, &pubsub.Message{Data: msgDataBytes})
	_, err = pubResult.Get(ctx)
	require.NoError(t, err)
	t.Logf("Test message published to Pub/Sub topic: %s", testProcessingInputTopicID)

	time.Sleep(5 * time.Second)

	// Use the helper to create the query client
	// This client is for queries (gRPC) but might also do metadata (REST).
	queryClient := newEmulatorBigQueryClient(ctx, t, testProcessingPubSubProjectID)
	require.NotNil(t, queryClient)
	defer queryClient.Close()

	//language=BigQuery
	queryString := fmt.Sprintf("SELECT uid, reading, device_eui, client_id, location_id, device_category, device_type FROM `%s.%s` WHERE device_eui = @eui LIMIT 1",
		testProcessingBigQueryDatasetID, testProcessingBigQueryTableID)
	query := queryClient.Query(queryString)
	query.Parameters = []bigquery.QueryParameter{{Name: "eui", Value: testProcessingDeviceEUI}}

	it, err := query.Read(ctx)
	require.NoError(t, err)
	var row telemetry.MeterReading
	err = it.Next(&row)
	if errors.Is(err, iterator.Done) {
		t.Fatalf("No rows returned from BQ emulator for EUI %s. Logs:\n%s", testProcessingDeviceEUI, logBuf.String())
	}
	require.NoError(t, err)
	assert.Equal(t, testProcessingDeviceUID, row.Uid)
	assert.InDelta(t, 123.45, row.Reading, 0.001)
	assert.Equal(t, "XDevice", row.DeviceType)

	t.Logf("Full flow integration test for ProcessingService completed. Logs:\n%s", logBuf.String())
}

// createValidTestHexPayload is now assumed to be defined in another _test.go file
// within the same 'xdevice' package (e.g., xdevice_decoder_test.go).
