//go:build integration

package main_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/illmade-knight/ai-power-mpv/pkg/helpers/loadgen"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"

	// Import initializers for both services
	"bigquery/bqinit"
	"ingestion/mqinit"

	// Import library packages
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/mqttconverter"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
)

// --- Cloud Load Test Constants ---
const (
	// Load test parameters
	loadTestDuration       = 30 * time.Second // Keep duration short for a test, increase for real load.
	loadTestNumDevices     = 10
	loadTestRatePerDevice  = 2.0  // messages per second per device
	loadTestSuccessPercent = 0.98 // The percentage of messages that must be received for the test to pass.

	// Constants copied from the original e2e test
	cloudTestGCPProjectID   = "gemini-power-test"
	testMosquittoImage      = "eclipse-mosquitto:2.0"
	testMqttBrokerPort      = "1883/tcp"
	cloudTestRunPrefix      = "cloud_load_test"
	testMqttTopicPattern    = "devices/+/data"
	testMqttClientIDPrefix  = "ingestion-service-cloud-load"
	bqDatasetDefaultTTLDays = 1
	cloudTestMqttHTTPPort   = ":9092"
	cloudTestBqHTTPPort     = ":9093"
	cloudLoadTestTimeout    = 10 * time.Minute // Increased timeout for the load test
)

// --- Test Flags ---
var (
	keepDataset = flag.Bool("keep-dataset", false, "If true, the BigQuery dataset will not be deleted after the test, but will expire automatically.")
)

// TestE2E_Cloud_LoadTest subjects the entire pipeline to load from simulated devices.
// It verifies that a high percentage of the generated messages are successfully stored in BigQuery.
func TestE2E_Cloud_LoadTest(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	// --- 1. Authentication and Configuration (copied from E2E test) ---
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud load test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		_, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), cloudLoadTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 2. Generate Unique Resource Names (copied from E2E test) ---
	runID := uuid.New().String()[:8]
	topicID := fmt.Sprintf("%s_processed_%s", cloudTestRunPrefix, runID)
	subscriptionID := fmt.Sprintf("%s_processed_sub_%s", cloudTestRunPrefix, runID)
	datasetID := fmt.Sprintf("%s_dataset_%s", cloudTestRunPrefix, runID)
	tableID := fmt.Sprintf("monitor_payloads_%s", runID)

	// --- 3. Setup Temporary Cloud & Local Infrastructure (copied from E2E test) ---
	log.Info().Msg("LoadTest: Setting up Mosquitto container...")
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()

	log.Info().Str("topic", topicID).Str("subscription", subscriptionID).Msg("LoadTest: Setting up real Cloud Pub/Sub resources...")
	pubsubCleanup := setupRealPubSub(t, ctx, projectID, topicID, subscriptionID)
	defer pubsubCleanup()

	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("LoadTest: Setting up real BigQuery resources...")
	bqCleanup := setupRealBigQuery(t, ctx, projectID, datasetID, tableID, *keepDataset)
	defer bqCleanup()

	log.Info().Msg("LoadTest: Pausing to allow cloud resources to initialize...")
	time.Sleep(5 * time.Second)

	// --- 4. Configure and Start MQTT Ingestion Service (copied from E2E test) ---
	// Using higher capacity and more workers to handle the load
	mqttCfg := &mqinit.Config{
		LogLevel:  "info",
		HTTPPort:  cloudTestMqttHTTPPort,
		ProjectID: projectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{TopicID: topicID},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicPattern,
			ClientIDPrefix: testMqttClientIDPrefix,
			KeepAlive:      30 * time.Second,
			ConnectTimeout: 10 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    1000,
			NumProcessingWorkers: 20,
		},
	}
	mqttLogger := log.With().Str("service", "mqtt-ingestion").Logger()
	mqttPublisher, err := mqttconverter.NewGooglePubSubPublisher(ctx, &mqttconverter.GooglePubSubPublisherConfig{
		ProjectID: mqttCfg.ProjectID,
		TopicID:   mqttCfg.Publisher.TopicID,
	}, mqttLogger)
	require.NoError(t, err)

	ingestionService := mqttconverter.NewIngestionService(mqttPublisher, nil, mqttLogger, mqttCfg.Service, &mqttCfg.MQTT)
	mqttServer := mqinit.NewServer(mqttCfg, ingestionService, mqttLogger)
	go func() {
		if err := mqttServer.Start(); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("Ingestion server failed during test execution")
		}
	}()
	defer mqttServer.Shutdown()

	// --- 5. Configure and Start BigQuery Processing Service (copied from E2E test) ---
	// Using larger batches and more workers to handle the load
	bqCfg := &bqinit.Config{
		LogLevel:  "info",
		HTTPPort:  cloudTestBqHTTPPort,
		ProjectID: projectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: subscriptionID},
		BigQuery: struct {
			DatasetID       string `mapstructure:"dataset_id"`
			TableID         string `mapstructure:"table_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{DatasetID: datasetID, TableID: tableID},
		BatchProcessing: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{NumWorkers: 10, BatchSize: 200, FlushTimeout: 10 * time.Second},
	}
	bqLogger := log.With().Str("service", "bq-processor").Logger()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real BigQuery client")
	defer bqClient.Close()
	bqConsumer, err := bqstore.NewGooglePubSubConsumer(ctx, &bqstore.GooglePubSubConsumerConfig{
		ProjectID:      bqCfg.ProjectID,
		SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, bqLogger)
	require.NoError(t, err)
	bqInserter, err := bqstore.NewBigQueryInserter[types.GardenMonitorPayload](ctx, bqClient, &bqstore.BigQueryInserterConfig{
		ProjectID: bqCfg.ProjectID,
		DatasetID: bqCfg.BigQuery.DatasetID,
		TableID:   bqCfg.BigQuery.TableID,
	}, bqLogger)
	require.NoError(t, err)
	batcher := bqstore.NewBatchInserter[types.GardenMonitorPayload](&bqstore.BatchInserterConfig{
		BatchSize:    bqCfg.BatchProcessing.BatchSize,
		FlushTimeout: bqCfg.BatchProcessing.FlushTimeout,
	}, bqInserter, bqLogger)
	processingService, err := bqstore.NewBatchingService[types.GardenMonitorPayload](&bqstore.ServiceConfig{
		NumProcessingWorkers: bqCfg.BatchProcessing.NumWorkers,
	}, bqConsumer, batcher, types.GardenMonitorDecoder, bqLogger)
	require.NoError(t, err)
	bqServer := bqinit.NewServer(bqCfg, processingService, bqLogger)
	go func() {
		if err := bqServer.Start(); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("BigQuery Processing server failed: %v", err)
		}
	}()
	defer bqServer.Shutdown()

	log.Info().Msg("LoadTest: Pausing to allow services to start and connect...")
	time.Sleep(10 * time.Second)

	// --- 6. Start Load Generation ---
	log.Info().Msg("LoadTest: Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()

	// 6.1 Create the MQTT client for the load generator
	loadgenClient := loadgen.NewMqttClient(mqttBrokerURL, testMqttTopicPattern, 1, loadgenLogger)

	// 6.2 Create the simulated devices
	devices := make([]*loadgen.Device, loadTestNumDevices)
	payloadGenerator := &gardenMonitorPayloadGenerator{}
	for i := 0; i < loadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{
			ID:               fmt.Sprintf("load-test-device-%d", i),
			MessageRate:      loadTestRatePerDevice,
			PayloadGenerator: payloadGenerator,
		}
	}

	// 6.3 Create and run the load generator
	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)
	err = loadGenerator.Run(ctx, loadTestDuration)
	require.NoError(t, err, "Load generator returned an error")

	// --- 7. Verify Data in Real BigQuery ---
	expectedMessages := int(float64(loadTestNumDevices) * loadTestRatePerDevice * loadTestDuration.Seconds())
	successThreshold := int(float64(expectedMessages) * loadTestSuccessPercent)
	log.Info().
		Int("total_expected", expectedMessages).
		Int("success_threshold", successThreshold).
		Msg("LoadTest: Verifying results in BigQuery...")

	var finalCount int64
	// Allow ample time for all data to propagate through the live cloud services.
	verificationTimeout := time.After(5 * time.Minute)
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-verificationTimeout:
			t.Fatalf("Test timed out waiting for BigQuery results. Final count: %d, Threshold: %d", finalCount, successThreshold)
		case <-ctx.Done():
			t.Fatalf("Test context cancelled waiting for BigQuery results. Final count: %d, Threshold: %d", finalCount, successThreshold)
		case <-tick.C:
			log.Info().Msg("LoadTest: Polling BigQuery for row count...")
			queryString := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s`", datasetID, tableID)
			query := bqClient.Query(queryString)

			it, err := query.Read(ctx)
			if err != nil {
				log.Warn().Err(err).Msg("Polling query failed during Read")
				continue // Try again on the next tick
			}

			var row struct {
				Count int64 `bigquery:"count"`
			}
			err = it.Next(&row)
			if errors.Is(err, iterator.Done) {
				log.Warn().Msg("Polling query returned no rows for COUNT(*)")
				continue
			}
			if err != nil {
				log.Warn().Err(err).Msg("Polling query failed during Next")
				continue
			}
			finalCount = row.Count
			log.Info().Int64("current_count", finalCount).Int("threshold", successThreshold).Msg("Polled count")
			if finalCount >= int64(successThreshold) {
				log.Info().Msg("LoadTest: Success threshold met. Verification successful!")
				break VerificationLoop
			}
		}
	}

	// --- 8. Final Assertions ---
	require.GreaterOrEqual(t, finalCount, int64(successThreshold),
		"Expected at least %d messages in BigQuery, but found %d", successThreshold, finalCount)

	log.Info().Msg("LoadTest: Verification successful!")
}

// --- Helper functions (setupRealPubSub, setupRealBigQuery, etc.) are assumed to be identical ---
// --- to those in cloude2e_test.go and are included here for completeness.             ---

// setupRealPubSub creates a topic and subscription on GCP for the test run.
func setupRealPubSub(t *testing.T, ctx context.Context, projectID, topicID, subID string) func() {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real Pub/Sub client")

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err, "Failed to create real Pub/Sub topic")
	t.Logf("Created Cloud Pub/Sub Topic: %s", topic.ID())

	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
	require.NoError(t, err, "Failed to create real Pub/Sub subscription")
	t.Logf("Created Cloud Pub/Sub Subscription: %s", sub.ID())

	return func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		t.Logf("Tearing down Cloud Pub/Sub resources...")

		subRef := client.Subscription(subID)
		if err := subRef.Delete(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete subscription '%s': %v", subID, err)
		} else {
			t.Logf("Deleted subscription '%s'", subID)
		}

		topicRef := client.Topic(topicID)
		if err := topicRef.Delete(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete topic '%s': %v", topicID, err)
		} else {
			t.Logf("Deleted topic '%s'", topicID)
		}
		client.Close()
	}
}

// setupRealBigQuery creates a dataset and table on GCP for the test run.
func setupRealBigQuery(t *testing.T, ctx context.Context, projectID, datasetID, tableID string, keep bool) func() {
	t.Helper()
	client, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real BigQuery client")

	datasetMeta := &bigquery.DatasetMetadata{
		Name:                   datasetID,
		DefaultTableExpiration: time.Duration(bqDatasetDefaultTTLDays*24) * time.Hour,
		Description:            "Temporary dataset for cloud load test",
	}
	err = client.Dataset(datasetID).Create(ctx, datasetMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create BQ dataset %s", datasetID)
	}
	t.Logf("Created temporary BigQuery Dataset: %s", datasetID)

	schema, err := bigquery.InferSchema(types.GardenMonitorPayload{})
	require.NoError(t, err)
	tableRef := client.Dataset(datasetID).Table(tableID)
	err = tableRef.Create(ctx, &bigquery.TableMetadata{Name: tableID, Schema: schema})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err)
	}
	t.Logf("Created BigQuery table %s in dataset %s", tableID, datasetID)

	return func() {
		defer client.Close()
		if keep {
			t.Logf("Keeping BigQuery dataset '%s' due to -keep-dataset flag. It will expire automatically.", datasetID)
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		t.Logf("Tearing down Cloud BigQuery resources...")

		if err := client.Dataset(datasetID).DeleteWithContents(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete BQ dataset '%s': %v", datasetID, err)
		} else {
			t.Logf("Deleted BQ dataset '%s'", datasetID)
		}
	}
}

// setupMosquittoContainer sets up the MQTT broker in a container.
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
