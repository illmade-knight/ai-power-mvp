//go:build integration

package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"google.golang.org/api/option"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	// Import initializers for both services
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/bigquery/bqinit"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/ingestion/mqinit"

	// Import library packages
	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/illmade-knight/go-iot/pkg/types"
)

// --- Cloud Test Constants ---
const (
	cloudTestGCPProjectID = "gemini-power-test"

	// Note: Your GCP Project ID should be set via the GOOGLE_CLOUD_PROJECT env var
	cloudTestRunPrefix      = "cloud_test"
	testMqttTopicPattern    = "devices/+/data" // Reusing from integration test
	testMqttDeviceUID       = "GARDEN_MONITOR_CLOUD_001"
	testMqttClientIDPrefix  = "ingestion-service-cloud"
	bqDatasetDefaultTTLDays = 1 // Datasets created for tests will be auto-deleted after this many days
	//cloudTestMqttHTTPPort   = ":9090"
	//cloudTestBqHTTPPort     = ":9091"
	cloudTestTimeout = 5 * time.Minute
)

// TestE2E_Cloud_MqttToBigQueryFlow is a full end-to-end test against a real GCP project.
// It requires authentication (GOOGLE_APPLICATION_CREDENTIALS or gcloud ADC) and GOOGLE_CLOUD_PROJECT to be set.
func TestE2E_Cloud_MqttToBigQueryFlow(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	// --- 1. Authentication and Configuration ---
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		// Perform a quick check to see if ADC is likely to work.
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		_, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), cloudTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 2. Generate Unique Resource Names for this Test Run ---
	runID := uuid.New().String()[:8]
	topicID := fmt.Sprintf("%s_processed_%s", cloudTestRunPrefix, runID)
	subscriptionID := fmt.Sprintf("%s_processed_sub_%s", cloudTestRunPrefix, runID)
	datasetID := fmt.Sprintf("%s_dataset_%s", cloudTestRunPrefix, runID)
	tableID := fmt.Sprintf("monitor_payloads_%s", runID)

	// --- 3. Setup Temporary Cloud & Local Infrastructure ---
	log.Info().Msg("E2E: Setting up Mosquitto emulator...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	defer mosquittoCleanup()

	log.Info().Str("topic", topicID).Str("subscription", subscriptionID).Msg("CloudTest: Setting up real Cloud Pub/Sub resources...")
	pubsubCleanup := setupRealPubSub(t, ctx, projectID, topicID, subscriptionID)
	defer pubsubCleanup()

	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("CloudTest: Setting up real BigQuery resources...")
	bqCleanup := setupRealBigQuery(t, ctx, projectID, datasetID, tableID)
	defer bqCleanup()

	log.Info().Msg("CloudTest: Pausing to allow cloud resources to initialize...")
	time.Sleep(5 * time.Second)

	// --- 4. Configure and Start MQTT Ingestion Service ---
	// This service connects our local MQTT broker to the real Google Cloud Pub/Sub topic.
	mqttCfg := &mqinit.Config{
		LogLevel: "debug",
		//HTTPPort:  cloudTestMqttHTTPPort,
		ProjectID: projectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{TopicID: topicID},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicPattern,
			ClientIDPrefix: testMqttClientIDPrefix,
			KeepAlive:      10 * time.Second,
			ConnectTimeout: 5 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
	}
	mqttLogger := log.With().Str("service", "mqtt-ingestion").Logger()
	// This publisher will use the real projectID and topicID, and authenticate via ADC.
	mqttPublisher, err := mqttconverter.NewGooglePubsubPublisher(ctx, mqttconverter.GooglePubsubPublisherConfig{
		ProjectID:       mqttCfg.ProjectID,
		TopicID:         mqttCfg.Publisher.TopicID,
		ClientOptions:   make([]option.ClientOption, 0),
		PublishSettings: mqttconverter.GetDefaultPublishSettings(),
	}, mqttLogger)
	require.NoError(t, err)

	ingestionService := mqttconverter.NewIngestionService(mqttPublisher, nil, mqttLogger, mqttCfg.Service, mqttCfg.MQTT)
	mqttServer := mqinit.NewServer(mqttCfg, ingestionService, mqttLogger)
	go func() {
		if err := mqttServer.Start(); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("Ingestion server failed during test execution")
		}
	}()
	defer mqttServer.Shutdown()

	bqCfg := &bqinit.Config{
		LogLevel: "debug",
		//HTTPPort:  cloudTestBqHTTPPort,
		ProjectID: projectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: subscriptionID},
		BigQuery: bqstore.BigQueryDatasetConfig{
			ProjectID: projectID,
			DatasetID: datasetID,
			TableID:   tableID,
		},
		BatchProcessing: struct {
			bqstore.BatchInserterConfig `mapstructure:"datasetup"`
			NumWorkers                  int `mapstructure:"num_workers"`
		}{
			BatchInserterConfig: bqstore.BatchInserterConfig{
				BatchSize:    5,
				FlushTimeout: 10 * time.Second,
			},
			NumWorkers: 2,
		},
	}

	bqLogger := log.With().Str("service", "bq-processor").Logger()

	// This client connects to the real BigQuery service.
	bqClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real BigQuery client")
	defer bqClient.Close()

	bqConsumer, err := consumers.NewGooglePubSubConsumer(ctx, &consumers.GooglePubSubConsumerConfig{
		ProjectID:      bqCfg.ProjectID,
		SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, make([]option.ClientOption, 0), bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new, single convenience constructor ***
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, &bqCfg.BatchProcessing.BatchInserterConfig, &bqCfg.BigQuery, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new service constructor ***
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](bqCfg.BatchProcessing.NumWorkers, bqConsumer, batchInserter, types.ConsumedMessageTransformer, bqLogger)
	require.NoError(t, err)

	bqServer := bqinit.NewServer(bqCfg, processingService, bqLogger)
	go func() {
		if err := bqServer.Start(); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("BigQuery Processing server failed: %v", err)
		}
	}()
	defer bqServer.Shutdown()

	log.Info().Msg("CloudTest: Pausing to allow services to start and connect...")
	time.Sleep(5 * time.Second)

	// --- 6. Publish a Test Message to MQTT ---
	mqttTestPublisher, err := emulators.CreateTestMqttPublisher(mqttBrokerURL, "cloud-test-publisher")
	require.NoError(t, err)
	defer mqttTestPublisher.Disconnect(250)

	testPayload := types.GardenMonitorReadings{
		DE:       testMqttDeviceUID,
		Sequence: 456,
		Battery:  99,
		// Ensure all fields that might be in the schema are present.
		Humidity:     65,
		Temperature:  22,
		SoilMoisture: 45,
	}
	msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &testPayload})
	require.NoError(t, err)
	publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceUID, 1)
	token := mqttTestPublisher.Publish(publishTopic, 1, false, msgBytes)
	token.Wait()
	require.NoError(t, token.Error())
	log.Info().Msg("CloudTest: Published test message to MQTT.")

	// --- 7. Verify Data in Real BigQuery ---
	var receivedRows []types.GardenMonitorReadings
	var lastQueryErr error
	// Allow ample time for data to propagate through the live cloud services.
	verificationTimeout := time.After(2 * time.Minute)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-verificationTimeout:
			t.Fatalf("Test timed out waiting for BigQuery results. Last error: %v", lastQueryErr)
		case <-tick.C:
			log.Info().Msg("CloudTest: Polling BigQuery for results...")
			// Use the real BigQuery client and resource names.
			queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid", datasetID, tableID)
			query := bqClient.Query(queryString)
			query.Parameters = []bigquery.QueryParameter{{Name: "uid", Value: testMqttDeviceUID}}

			it, err := query.Read(ctx)
			if err != nil {
				lastQueryErr = fmt.Errorf("polling query failed during Read: %w", err)
				continue // Try again on the next tick
			}

			var currentRows []types.GardenMonitorReadings
			for {
				var row types.GardenMonitorReadings
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

			if len(currentRows) >= 1 {
				receivedRows = currentRows
				log.Info().Int("count", len(receivedRows)).Msg("CloudTest: Successfully found row(s), breaking verification loop.")
				break VerificationLoop // Success!
			}
			lastQueryErr = fmt.Errorf("still waiting for rows: got %d, want %d", len(currentRows), 1)
		}
	}

	// --- 8. Final Assertions ---
	require.Len(t, receivedRows, 1, "Expected exactly one row in BigQuery for the test message")
	assert.Equal(t, testPayload.DE, receivedRows[0].DE)
	assert.Equal(t, testPayload.Sequence, receivedRows[0].Sequence)
	assert.Equal(t, testPayload.Battery, receivedRows[0].Battery)
	log.Info().Msg("CloudTest: Verification successful!")
}

// --- Cloud Resource Setup Helpers ---

// setupRealPubSub creates a topic and subscription on GCP for the test run.
// It returns a cleanup function to delete them.
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
// It returns a cleanup function to delete the dataset.
func setupRealBigQuery(t *testing.T, ctx context.Context, projectID, datasetID, tableID string) func() {
	t.Helper()
	client, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real BigQuery client")

	// Create a dataset that will automatically expire.
	datasetMeta := &bigquery.DatasetMetadata{
		Name:                   datasetID,
		DefaultTableExpiration: time.Duration(bqDatasetDefaultTTLDays*24) * time.Hour,
		Description:            "Temporary dataset for cloud integration test",
	}
	err = client.Dataset(datasetID).Create(ctx, datasetMeta)
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err, "Failed to create BQ dataset %s", datasetID)
	}
	t.Logf("Created temporary BigQuery Dataset: %s", datasetID)

	// Infer schema from the payload struct and create the table.
	schema, err := bigquery.InferSchema(types.GardenMonitorReadings{})
	require.NoError(t, err)
	tableRef := client.Dataset(datasetID).Table(tableID)
	err = tableRef.Create(ctx, &bigquery.TableMetadata{Name: tableID, Schema: schema})
	if err != nil && !strings.Contains(err.Error(), "Already Exists") {
		require.NoError(t, err)
	}
	t.Logf("Created BigQuery table %s in dataset %s", tableID, datasetID)

	return func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		t.Logf("Tearing down Cloud BigQuery resources...")

		// Deleting the dataset will also delete all tables within it.
		if err := client.Dataset(datasetID).DeleteWithContents(cleanupCtx); err != nil {
			t.Logf("Warning - failed to delete BQ dataset '%s': %v", datasetID, err)
		} else {
			t.Logf("Deleted BQ dataset '%s'", datasetID)
		}
		client.Close()
	}
}
