//go:build integration

package servicemanager

import (
	"context"
	"fmt"
	telemetry "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	// "cloud.google.com/go/pubsub" // Not used in this BQ-focused integration test
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	// "google.golang.org/grpc" // No longer needed if gRPC options removed from this specific helper
	// "google.golang.org/grpc/credentials/insecure" // No longer needed if gRPC options removed from this specific helper

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// BigQuery Emulator Test Config for Service Manager
	testSMBQEmulatorImage       = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	testSMBQEmulatorGRPCPortStr = "9060"
	testSMBQEmulatorRestPortStr = "9050"
	testSMBQEmulatorGRPCPort    = testSMBQEmulatorGRPCPortStr + "/tcp"
	testSMBQEmulatorRestPort    = testSMBQEmulatorRestPortStr + "/tcp"
	testSMBQProjectID           = "sm-bq-test-project"
	testSMBQAdminDatasetID      = "sm_test_dataset_admin"
	testSMBQDatasetID           = "sm_test_dataset"
	testSMBQAnotherDatasetID    = "another_dataset_sm"
	testSMBQTableID             = "sm_test_meter_readings"

	// Dummy Pub/Sub names for YAML validation
	dummyTopicForValidationIntegration = "dummy-topic-for-sm-bq-integration-test"
	dummySubForValidationIntegration   = "dummy-sub-for-sm-bq-integration-test"
)

// newEmulatorBQClient creates a BigQuery client configured for the emulator,
// primarily using settings that work for REST-based metadata operations.
// It relies on GOOGLE_CLOUD_PROJECT and BIGQUERY_API_ENDPOINT (the REST endpoint)
// environment variables being set by the caller (using t.Setenv).
func newEmulatorBQClient(ctx context.Context, t *testing.T, projectID string) *bigquery.Client {
	t.Helper()

	emulatorRESTHost := os.Getenv("BIGQUERY_API_ENDPOINT") // Should be like "http://localhost:9050"

	require.NotEmpty(t, projectID, "projectID must be set for newEmulatorBQClient")
	require.NotEmpty(t, emulatorRESTHost, "BIGQUERY_API_ENDPOINT env var must be set for newEmulatorBQClient")

	clientOpts := []option.ClientOption{
		option.WithEndpoint(emulatorRESTHost), // Point to the REST endpoint
		option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{}), // Use a plain http.Client to force HTTP
		// NO gRPC specific options here.
		// The client library should still be able to perform gRPC operations (like Put or Query)
		// by respecting the BIGQUERY_EMULATOR_HOST env var for those specific calls if needed.
	}

	client, err := bigquery.NewClient(ctx, projectID, clientOpts...)
	require.NoError(t, err, "newEmulatorBQClient: Failed to create BigQuery client. Project: %s, RESTHost: %s", projectID, emulatorRESTHost)
	return client
}

func setupBigQueryEmulatorForManagerTest(t *testing.T, ctx context.Context) (emulatorGRPCHost, emulatorRESTHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testSMBQEmulatorImage,
		ExposedPorts: []string{testSMBQEmulatorGRPCPort, testSMBQEmulatorRestPort},
		Cmd: []string{
			"--project=" + testSMBQProjectID,
			"--port=" + testSMBQEmulatorRestPortStr,
			"--grpc-port=" + testSMBQEmulatorGRPCPortStr,
			"--log-level=debug",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(testSMBQEmulatorGRPCPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(testSMBQEmulatorRestPort).WithStartupTimeout(60*time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start BigQuery emulator container for manager test")

	host, err := container.Host(ctx)
	require.NoError(t, err)

	grpcMappedPort, err := container.MappedPort(ctx, testSMBQEmulatorGRPCPort)
	require.NoError(t, err)
	emulatorGRPCHost = fmt.Sprintf("%s:%s", host, grpcMappedPort.Port())
	t.Logf("BigQuery emulator container started, gRPC on: %s", emulatorGRPCHost)

	restMappedPort, err := container.MappedPort(ctx, testSMBQEmulatorRestPort)
	require.NoError(t, err)
	emulatorRESTHost = fmt.Sprintf("http://%s:%s", host, restMappedPort.Port())
	t.Logf("BigQuery emulator container started, REST on: %s", emulatorRESTHost)

	// Set environment variables *before* creating any BigQuery clients
	t.Setenv("GOOGLE_CLOUD_PROJECT", testSMBQProjectID)
	t.Setenv("BIGQUERY_EMULATOR_HOST", emulatorGRPCHost) // For gRPC based API calls by client library
	t.Setenv("BIGQUERY_API_ENDPOINT", emulatorRESTHost)  // For REST based API calls by client library

	// Use the helper to create the admin client
	adminBqClient := newEmulatorBQClient(ctx, t, testSMBQProjectID)
	require.NotNil(t, adminBqClient, "Admin BQ client should not be nil")
	defer adminBqClient.Close()

	dataset := adminBqClient.Dataset(testSMBQAdminDatasetID)
	err = dataset.Create(ctx, &bigquery.DatasetMetadata{Name: testSMBQAdminDatasetID})
	if err != nil {
		if strings.Contains(err.Error(), "http: server gave HTTP response to HTTPS client") {
			t.Logf("CRITICAL: dataset.Create failed with HTTP/HTTPS mismatch. BIGQUERY_API_ENDPOINT was: %s", emulatorRESTHost)
		}
		if !strings.Contains(err.Error(), "Already Exists") {
			require.NoError(t, err, "Failed to create dataset '%s' on BQ emulator.", testSMBQAdminDatasetID)
		} else {
			t.Logf("BigQuery dataset '%s' already exists on emulator.", testSMBQAdminDatasetID)
		}
	} else {
		t.Logf("Created BigQuery dataset '%s' on emulator", testSMBQDatasetID)
	}

	// Table creation is now handled by the manager's Setup call in the test itself.

	return emulatorGRPCHost, emulatorRESTHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

// createManagerTestYAMLFile is assumed to be defined in another _test.go file
// within the same 'servicemanager' package.
/*
import "path/filepath"
func createManagerTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "manager_test_config_bq_sm.yaml")
	err := os.WriteFile(filePath, []byte(content), 0600)
	require.NoError(t, err, "Failed to write temporary manager YAML file")
	return filePath
}
*/

func TestBigQueryManager_Integration_SetupAndTeardown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// SetupBigQueryEmulatorForManagerTest now sets the necessary env vars
	_, _, bqEmulatorCleanupFunc := setupBigQueryEmulatorForManagerTest(t, ctx)
	defer bqEmulatorCleanupFunc()

	// Define a sample configuration YAML content
	yamlTimePartitioningField := "original_mqtt_time"
	yamlClusteringFields := []string{"location_id", "device_type"}

	yamlContent := fmt.Sprintf(`
default_project_id: "%s" 
default_location: "US" 
environments:
  integration_test_bq_sm: 
    project_id: "%s"
    default_location: "EU" 
    teardown_protection: false
resources:
  pubsub_topics: 
    - name: "%s" 
  pubsub_subscriptions: 
    - name: "%s"
      topic: "%s"
  bigquery_datasets:
    - name: "%s"
      description: "Test dataset for meter readings (SM)"
      labels:
        env: "integration_test_bq_sm"
        owner: "sm_test_bq"
    - name: "%s" 
      location: "US" 
  bigquery_tables:
    - name: "%s"
      dataset: "%s"
      description: "Stores decoded meter readings from various devices (SM)."
      schema_source_type: "go_struct"
      schema_source_identifier: "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading"
      time_partitioning_field: "%s" 
      time_partitioning_type: "DAY"
      clustering_fields: ["%s", "%s"]
`, testSMBQProjectID, testSMBQProjectID,
		dummyTopicForValidationIntegration, dummySubForValidationIntegration, dummyTopicForValidationIntegration,
		testSMBQDatasetID, testSMBQAnotherDatasetID,
		testSMBQTableID, testSMBQDatasetID, yamlTimePartitioningField, yamlClusteringFields[0], yamlClusteringFields[1])

	configFilePath := createManagerTestYAMLFile(t, yamlContent) // Assumes this helper is available in the package

	cfg, err := LoadAndValidateConfig(configFilePath)
	require.NoError(t, err, "Failed to load and validate test config")
	require.NotNil(t, cfg, "Config should not be nil")

	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	// Create the BQ client for the manager using the general helper.
	// GOOGLE_CLOUD_PROJECT, BIGQUERY_EMULATOR_HOST, BIGQUERY_API_ENDPOINT are set by setupBigQueryEmulatorForManagerTest
	managerConcreteClient := newEmulatorBQClient(ctx, t, testSMBQProjectID)
	require.NotNil(t, managerConcreteClient, "Manager BQ client should not be nil")
	defer managerConcreteClient.Close()

	managerBQClientAdapter := NewBigQueryClientAdapter(managerConcreteClient)
	require.NotNil(t, managerBQClientAdapter, "Manager BQ client adapter should not be nil")

	manager, err := NewBigQueryManager(managerBQClientAdapter, logger, nil)
	require.NoError(t, err)

	// --- Test Setup ---
	t.Run("SetupBigQueryResources", func(t *testing.T) {
		err = manager.Setup(ctx, cfg, "integration_test_bq_sm")
		require.NoError(t, err, "BigQueryManager.Setup failed")

		// Verification client - create a new one using the same helper
		verifyClient := newEmulatorBQClient(ctx, t, testSMBQProjectID)
		defer verifyClient.Close()

		// Verify dataset1
		ds1 := verifyClient.Dataset(testSMBQDatasetID)
		ds1Meta, err := ds1.Metadata(ctx)
		require.NoError(t, err, "Error getting metadata for dataset %s", testSMBQDatasetID)
		assert.Equal(t, "Test dataset for meter readings (SM)", ds1Meta.Description)
		assert.Equal(t, "integration_test_bq_sm", ds1Meta.Labels["env"])
		assert.Equal(t, "EU", ds1Meta.Location, "Dataset location should be EU from env default")

		// Verify dataset2
		ds2 := verifyClient.Dataset(testSMBQAnotherDatasetID)
		ds2Meta, err := ds2.Metadata(ctx)
		require.NoError(t, err, "Error getting metadata for dataset %s", testSMBQAnotherDatasetID)
		assert.Equal(t, "US", ds2Meta.Location, "Dataset location should be US as specified")

		// Verify table
		table := ds1.Table(testSMBQTableID) // Table is in dataset1
		tableMeta, err := table.Metadata(ctx)
		require.NoError(t, err, "Error getting metadata for table %s", testSMBQTableID)
		assert.Equal(t, "Stores decoded meter readings from various devices (SM).", tableMeta.Description)
		require.NotNil(t, tableMeta.TimePartitioning, "Time partitioning should be set")
		assert.Equal(t, bigquery.DayPartitioningType, tableMeta.TimePartitioning.Type)

		// Assert against the value defined in YAML for partitioning field
		assert.Equal(t, yamlTimePartitioningField, tableMeta.TimePartitioning.Field, "TimePartitioning.Field mismatch")

		// Check if a column compatible with partitioning field exists in the schema
		var partitioningFieldColumnExists bool
		for _, fieldSchema := range tableMeta.Schema {
			// With MeterReadingBQWrapper, schema columns are snake_case
			if fieldSchema.Name == yamlTimePartitioningField {
				partitioningFieldColumnExists = true
				break
			}
		}
		assert.True(t, partitioningFieldColumnExists, "Column for partitioning field '%s' not found in actual table schema. Schema: %+v", yamlTimePartitioningField, tableMeta.Schema)

		require.NotNil(t, tableMeta.Clustering, "Clustering should be set")
		assert.ElementsMatch(t, yamlClusteringFields, tableMeta.Clustering.Fields, "Clustering fields mismatch")

		// Verify schema field names are now snake_case because MeterReadingBQWrapper is used
		tempSaver := telemetry.MeterReadingBQWrapper{} // Assumes this is defined in servicemanager (e.g. bqinfer.go)
		expectedSchemaMap, _, _ := tempSaver.Save()

		actualSchemaFieldNames := make(map[string]bool)
		var actualFieldNamesForLog []string
		for _, fieldSchema := range tableMeta.Schema {
			actualSchemaFieldNames[fieldSchema.Name] = true
			actualFieldNamesForLog = append(actualFieldNamesForLog, fieldSchema.Name)
		}
		t.Logf("Actual table schema fields from BQ metadata: %v", actualFieldNamesForLog)
		t.Logf("Expected schema fields (snake_case from Saver): %v", mapsKeys(expectedSchemaMap))

		for expectedName := range expectedSchemaMap {
			assert.True(t, actualSchemaFieldNames[expectedName], "Expected snake_case field '%s' (from Saver) not found in table schema. Actual fields: %v", expectedName, actualFieldNamesForLog)
		}
		assert.Equal(t, len(expectedSchemaMap), len(tableMeta.Schema), "Schema field count mismatch (expected from Saver)")
	})

	// --- Test Teardown ---
	t.Run("TeardownBigQueryResources", func(t *testing.T) {
		cfg.Environments["integration_test_bq_sm"] = EnvironmentSpec{
			ProjectID:          testSMBQProjectID,
			TeardownProtection: false,
		}

		err = manager.Teardown(ctx, cfg, "integration_test_bq_sm")
		require.NoError(t, err, "BigQueryManager.Teardown failed")

		verifyClient := newEmulatorBQClient(ctx, t, testSMBQProjectID)
		defer verifyClient.Close()

		table := verifyClient.Dataset(testSMBQDatasetID).Table(testSMBQTableID)
		_, err = table.Metadata(ctx)
		require.Error(t, err, "Table %s should not exist after teardown", testSMBQTableID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for table GetMetadata should be 'notFound'")

		ds := verifyClient.Dataset(testSMBQDatasetID)
		_, err = ds.Metadata(ctx)
		require.Error(t, err, "Dataset %s should not exist after teardown", testSMBQDatasetID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for dataset GetMetadata should be 'notFound'")

		dsAnother := verifyClient.Dataset(testSMBQAnotherDatasetID)
		_, err = dsAnother.Metadata(ctx)
		require.Error(t, err, "Dataset %s should not exist after teardown", testSMBQAnotherDatasetID)
		assert.True(t, strings.Contains(err.Error(), "notFound"), "Error for dataset GetMetadata should be 'notFound'")
	})
}

// Helper to get keys from a map for logging/debugging
func mapsKeys(m map[string]bigquery.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// createValidTestHexPayload is now assumed to be defined in another _test.go file
// within the same 'xdevice' package (e.g., xdevice_decoder_test.go).
// For this servicemanager test, it's not directly used, but the MeterReading proto is.
