//go:build integration

package endtoendload_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// Testcontainers
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	// --- Your Packages ---
	bq "github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/bigquery"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/ingestion/converter"
	"load_test/apps/messagegen/messenger"
	"load_test/endtoendload"
)

// --- Test Constants ---
const (
	cloudTestGCPProjectID        = "gemini-power-test"
	cloudTestRunPrefix           = "e2e_load"
	cloudTestMqttTopicPattern    = "devices/garden-monitor/+/telemetry"
	cloudTestMqttClientIDPrefix  = "ingestion-service-cloud-load"
	envVarBqDatasetTTLDays       = "BQ_DATASET_TTL_DAYS"
	defaultBqDatasetTTLDays      = 1
	testMosquittoImage           = "eclipse-mosquitto:2.0"
	testMqttBrokerPort           = "1883/tcp"
	envVarIngestionPubSubTopicID = "GARDEN_MONITOR_TOPIC_PROCESSED"
)

// --- Test Setup Helpers ---
func setupMosquittoContainer(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()
	confContent := []byte("allow_anonymous true\nlistener 1883\npersistence false\n")
	tempDir := t.TempDir()
	hostConfPath := filepath.Join(tempDir, "mosquitto.conf")
	err := os.WriteFile(hostConfPath, confContent, 0644)
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        testMosquittoImage,
		ExposedPorts: []string{testMqttBrokerPort},
		WaitingFor:   wait.ForListeningPort(testMqttBrokerPort).WithStartupTimeout(60 * time.Second),
		Files:        []testcontainers.ContainerFile{{HostFilePath: hostConfPath, ContainerFilePath: "/mosquitto/config/mosquitto.conf"}},
		Cmd:          []string{"mosquitto", "-c", "/mosquitto/config/mosquitto.conf"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Mosquitto container")
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, testMqttBrokerPort)
	brokerURL := fmt.Sprintf("tcp://%s:%s", host, port.Port())
	t.Logf("Mosquitto container started: %s", brokerURL)
	return brokerURL, func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate Mosquitto container: %v", err)
		}
	}
}

func setupCloudPubSub(t *testing.T, ctx context.Context, projectID, topicID, subID string) func() {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real Pub/Sub client")

	topic, err := client.CreateTopic(ctx, topicID)
	require.NoError(t, err, "Failed to create real Pub/Sub topic")
	t.Logf("Created Cloud Pub/Sub Topic: %s", topic.ID())

	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err, "Failed to create real Pub/Sub subscription")
	t.Logf("Created Cloud Pub/Sub Subscription: %s", sub.ID())

	return func() {
		cleanupCtx := context.Background()
		t.Logf("Tearing down Cloud Pub/Sub resources...")
		subRef := client.Subscription(subID)
		if err := subRef.Delete(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete subscription '%s': %v", subID, err)
		}
		topicRef := client.Topic(topicID)
		if err := topicRef.Delete(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete topic '%s': %v", topicID, err)
		}
		client.Close()
	}
}

func setupCloudBigQuery(t *testing.T, ctx context.Context, projectID, datasetID, tableID string) func() {
	t.Helper()
	client, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real BigQuery client")

	ttlDaysStr := os.Getenv(envVarBqDatasetTTLDays)
	if ttlDaysStr == "" {
		ttlDaysStr = fmt.Sprintf("%d", defaultBqDatasetTTLDays)
	}
	ttlDays, _ := strconv.Atoi(ttlDaysStr)
	datasetDefaultTableTTL := time.Duration(ttlDays*24) * time.Hour

	datasetMeta := &bigquery.DatasetMetadata{
		Name:                   datasetID,
		DefaultTableExpiration: datasetDefaultTableTTL,
	}
	require.NoError(t, client.Dataset(datasetID).Create(ctx, datasetMeta), "Failed to create BQ dataset %s", datasetID)
	t.Logf("Created temporary BigQuery Dataset: %s", datasetID)

	schema, err := bigquery.InferSchema(bq.GardenMonitorPayload{})
	require.NoError(t, err)
	require.NoError(t, client.Dataset(datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{Schema: schema}))
	t.Logf("Created BigQuery table %s", tableID)

	return func() {
		t.Logf("BigQuery teardown for dataset '%s' is intentionally skipped.", datasetID)
		client.Close()
	}
}

// --- Main Load Test Function ---
func TestCloudLoadScenario(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud load test: GOOGLE_CLOUD_PROJECT must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Log("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		_, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud integration test: ADC check failed with: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
	}
	t.Setenv("GCP_PROJECT_ID", projectID)

	cfg, err := endtoendload.LoadTestConfigFromFile("config.json")
	if err != nil {
		t.Log("config.json not found, using default load test config")
		cfg = endtoendload.DefaultLoadTestConfig()
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.OverallTestTimeout)
	defer cancel()

	var logBuf bytes.Buffer
	writer := io.MultiWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}, &logBuf)
	logger := zerolog.New(writer).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	runID := uuid.New().String()[:8]
	topicID := fmt.Sprintf("%s_topic_%s", cloudTestRunPrefix, runID)
	subscriptionID := fmt.Sprintf("%s_sub_%s", cloudTestRunPrefix, runID)
	datasetID := fmt.Sprintf("%s_ds_%s", cloudTestRunPrefix, runID)
	tableID := fmt.Sprintf("monitor_payloads_load_%s", runID)

	// Setup cloud resources
	mqttBrokerURL, mosquittoCleanup := setupMosquittoContainer(t, ctx)
	defer mosquittoCleanup()
	pubsubCleanup := setupCloudPubSub(t, ctx, projectID, topicID, subscriptionID)
	defer pubsubCleanup()
	bigqueryCleanup := setupCloudBigQuery(t, ctx, projectID, datasetID, tableID)
	defer bigqueryCleanup()

	// Set environment variables for services
	t.Setenv("MQTT_BROKER_URL", mqttBrokerURL)
	t.Setenv("MQTT_TOPIC", cloudTestMqttTopicPattern)
	t.Setenv("MQTT_CLIENT_ID_PREFIX", cloudTestMqttClientIDPrefix)
	t.Setenv(envVarIngestionPubSubTopicID, topicID)
	t.Setenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT", subscriptionID)
	t.Setenv("BQ_DATASET_ID", datasetID)
	t.Setenv("BQ_TABLE_ID_METER_READINGS", tableID)

	// Start services
	ingestionService := startIngestionService(t, ctx, &cfg.ConverterServiceConfig, logger)
	defer ingestionService.Stop()
	processingService := startProcessingService(t, ctx, cfg, logger) // Pass the whole load test config
	defer processingService.Stop()

	logger.Info().Dur("delay", cfg.CloudServiceStartupDelay).Msg("Waiting for services to initialize...")
	time.Sleep(cfg.CloudServiceStartupDelay)

	// Run load generator
	var loadGenWg sync.WaitGroup
	loadGenWg.Add(1)
	go func() {
		defer loadGenWg.Done()
		loadGenCtx, loadGenCancel := context.WithTimeout(ctx, cfg.LoadGenDuration)
		defer loadGenCancel()
		runLoadGenerator(loadGenCtx, t, mqttBrokerURL, cfg, logger)
	}()

	logger.Info().Msg("Waiting for load generator to complete...")
	loadGenWg.Wait()
	logger.Info().Dur("grace_period", cfg.GracePeriodForMessageCompletion).Msg("Load generation finished. Waiting for messages to propagate...")
	time.Sleep(cfg.GracePeriodForMessageCompletion)

	// Verification
	totalMessagesSent := int(float64(cfg.NumDevices) * cfg.MsgRatePerDevice * cfg.LoadGenDuration.Seconds())
	minExpectedMessages := int(float64(totalMessagesSent) * cfg.MinMessagesMultiplier)

	verifyDataInBigQuery(t, ctx, projectID, datasetID, tableID, minExpectedMessages, &logBuf)
}

// --- Helper Functions to Start Components ---

func runLoadGenerator(ctx context.Context, t *testing.T, brokerURL string, cfg *endtoendload.LoadTestConfig, logger zerolog.Logger) {
	deviceGenConfig := &messenger.DeviceGeneratorConfig{
		NumDevices:       cfg.NumDevices,
		MsgRatePerDevice: cfg.MsgRatePerDevice,
	}
	deviceGenerator, err := messenger.NewDeviceGenerator(deviceGenConfig, logger)
	require.NoError(t, err)
	deviceGenerator.CreateDevices()

	opts := mqtt.NewClientOptions().AddBroker(brokerURL).SetClientID("load-test-runner")
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		require.NoError(t, token.Error(), "Load generator failed to connect to MQTT broker")
	}
	defer client.Disconnect(250)

	var wg sync.WaitGroup
	logger.Info().Int("device_count", cfg.NumDevices).Msg("Starting load generator goroutines...")

	for _, device := range deviceGenerator.Devices {
		wg.Add(1)
		go func(d *messenger.Device) {
			defer wg.Done()
			topic := strings.Replace(cloudTestMqttTopicPattern, "+", d.GetEUI(), 1)
			if d.MessageRate() <= 0 {
				logger.Warn().Str("device", d.GetEUI()).Msg("Device has zero message rate, will not send messages.")
				return
			}
			interval := time.Duration(float64(time.Second) / d.MessageRate())
			// FIX: Add check for non-positive interval to prevent panic.
			if interval <= 0 {
				logger.Warn().Str("device", d.GetEUI()).Float64("rate", d.MessageRate()).Msg("Calculated interval is non-positive due to high message rate. Skipping device.")
				return
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					payload, errGen := d.GeneratePayload()
					if errGen != nil {
						logger.Error().Err(errGen).Str("device", d.GetEUI()).Msg("Payload generation failed")
						continue
					}
					client.Publish(topic, 1, false, payload)
				case <-ctx.Done():
					return
				}
			}
		}(device)
	}

	<-ctx.Done()
	wg.Wait()
	logger.Info().Msg("All load generator goroutines have stopped.")
}

func startIngestionService(t *testing.T, ctx context.Context, cfg *converter.IngestionServiceConfig, logger zerolog.Logger) *converter.IngestionService {
	serviceLogger := logger.With().Str("service", "ingestion").Logger()

	mqttCfg, err := converter.LoadMQTTClientConfigFromEnv()
	require.NoError(t, err)
	pubsubCfg, err := converter.LoadGooglePubSubPublisherConfigFromEnv(envVarIngestionPubSubTopicID)
	require.NoError(t, err)
	publisher, err := converter.NewGooglePubSubPublisher(ctx, pubsubCfg, serviceLogger)
	require.NoError(t, err)

	service := converter.NewIngestionService(publisher, serviceLogger, *cfg, mqttCfg)

	go func() {
		if err := service.Start(); err != nil && !errors.Is(err, context.Canceled) {
			serviceLogger.Error().Err(err).Msg("Ingestion service failed to start or exited with an error.")
		}
	}()
	return service
}

func startProcessingService(t *testing.T, ctx context.Context, cfg *endtoendload.LoadTestConfig, logger zerolog.Logger) *bq.ProcessingService {
	serviceLogger := logger.With().Str("service", "processing").Logger()

	// Use the explicit config from the load test file, not from environment variables.
	processingServiceConfig := &bq.ServiceConfig{
		NumProcessingWorkers: cfg.ProcessingServiceConfig.NumProcessingWorkers,
		BatchSize:            cfg.ProcessingServiceConfig.BatchSize,
		FlushTimeout:         cfg.ProcessingServiceConfig.FlushTimeout,
	}

	bqInserterConfig, err := bq.LoadBigQueryInserterConfigFromEnv()
	require.NoError(t, err)
	bqClient, err := bigquery.NewClient(ctx, bqInserterConfig.ProjectID)
	require.NoError(t, err)

	inserter, err := bq.NewBigQueryInserter(ctx, bqClient, bqInserterConfig, serviceLogger)
	require.NoError(t, err)

	service, err := bq.NewProcessingService(ctx, processingServiceConfig, inserter, serviceLogger)
	require.NoError(t, err)

	go func() {
		if err := service.Start(); err != nil && !errors.Is(err, context.Canceled) {
			serviceLogger.Error().Err(err).Msg("Processing service failed to start or exited with an error.")
		}
	}()
	return service
}

func verifyDataInBigQuery(t *testing.T, ctx context.Context, projectID, datasetID, tableID string, minExpectedCount int, logBuf *bytes.Buffer) {
	var rowCount int
	var lastQueryErr error
	timeout := time.After(90 * time.Second)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	queryClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer queryClient.Close()

VerificationLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("Test timed out waiting for BigQuery results. Last error: %v. Logs:\n%s", lastQueryErr, logBuf.String())
		case <-tick.C:
			queryString := fmt.Sprintf("SELECT count(*) as count FROM `%s.%s`", datasetID, tableID)
			query := queryClient.Query(queryString)

			it, err := query.Read(ctx)
			if err != nil {
				lastQueryErr = fmt.Errorf("polling query failed during Read: %w", err)
				continue
			}
			var result struct{ Count int }
			err = it.Next(&result)
			if err != nil {
				lastQueryErr = fmt.Errorf("polling query failed during Next: %w", err)
				continue VerificationLoop
			}

			rowCount = result.Count
			if rowCount >= minExpectedCount {
				t.Logf("Successfully found %d rows (which is >= minimum of %d), breaking verification loop.", rowCount, minExpectedCount)
				break VerificationLoop
			}
			lastQueryErr = fmt.Errorf("still waiting for rows: got %d, want at least %d", rowCount, minExpectedCount)
		}
	}

	assert.GreaterOrEqual(t, rowCount, minExpectedCount, "The final number of messages in BigQuery is below the acceptable threshold")
	t.Logf("Successfully verified %d rows in real BigQuery.", rowCount)
}
