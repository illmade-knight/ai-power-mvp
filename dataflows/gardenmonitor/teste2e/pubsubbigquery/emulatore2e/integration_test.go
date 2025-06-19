//go:build integration

package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
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

// --- E2E Test Constants ---
const (
	testProjectID = "e2e-garden-project"

	// MQTT -> Ingestion Service
	testMosquittoImage     = "eclipse-mosquitto:2.0"
	testMqttBrokerPort     = "1883/tcp"
	testMqttTopicPattern   = "devices/+/data"
	testMqttDeviceUID      = "GARDEN_MONITOR_E2E_001"
	testMqttClientIDPrefix = "ingestion-service-e2e"

	// Pub/Sub -> Link between services
	// Pub/Sub (The link between Ingestion and Processing services)
	testPubSubEmulatorImage  = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubEmulatorPort   = "8085/tcp"
	testPubsubTopicID        = "garden-monitor-processed"     // Ingestion publishes here
	testPubsubSubscriptionID = "garden-monitor-processed-sub" // Processing subscribes here

	// BigQueryConfig -> BigQueryConfig Service
	e2eBigQueryDatasetID = "garden_e2e_dataset"
	e2eBigQueryTableID   = "monitor_payloads_e2e"

	// Service Ports
	e2eMqttHTTPPort = ":9090"
	e2eBqHTTPPort   = ":9091"
)

// --- Full End-to-End Test ---

func TestE2E_MqttToBigQueryFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 1. Start All Emulators ---
	log.Info().Msg("E2E: Setting up Mosquitto emulator...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	defer mosquittoCleanup()

	log.Info().Msg("E2E: Setting up Pub/Sub emulator...")
	pubsubCfg := emulators.GetDefaultPubsubConfig(testProjectID, map[string]string{testPubsubTopicID: testPubsubSubscriptionID})
	pubsubOptions, pubsubCleanup := emulators.SetupPubSubEmulator(t, ctx, pubsubCfg)
	defer pubsubCleanup()

	log.Info().Msg("E2E: Setting up BigQueryConfig emulator...")
	bqCfg := emulators.GetDefaultBigQueryConfig(testProjectID, map[string]string{e2eBigQueryDatasetID: e2eBigQueryTableID}, map[string]interface{}{e2eBigQueryTableID: types.GardenMonitorReadings{}})
	bqOptions, bqCleanup := emulators.SetupBigQueryEmulator(t, ctx, bqCfg)
	defer bqCleanup()

	//ensure service start
	log.Info().Msg("Pausing before ingestion setup")
	time.Sleep(3 * time.Second)

	// --- 2. Configure and Start MQTT Ingestion Service ---
	mqttCfg := &mqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  e2eMqttHTTPPort,
		ProjectID: testProjectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{
			TopicID: testPubsubTopicID,
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttBrokerURL,
			Topic:          testMqttTopicPattern,
			ClientIDPrefix: "ingestion-test-client-",
			KeepAlive:      10 * time.Second,
			ConnectTimeout: 5 * time.Second,
		},
		Service: mqttconverter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
	}

	mqttLogger := log.With().Str("service", "mqtt-ingestion").Logger()
	mqttPublisher, err := mqttconverter.NewGooglePubsubPublisher(ctx, mqttconverter.GooglePubsubPublisherConfig{
		ProjectID:       mqttCfg.ProjectID,
		TopicID:         mqttCfg.Publisher.TopicID,
		ClientOptions:   pubsubOptions,
		PublishSettings: mqttconverter.GetDefaultPublishSettings(),
	}, mqttLogger)
	require.NoError(t, err)

	ingestionService := mqttconverter.NewIngestionService(mqttPublisher, nil, mqttLogger, mqttCfg.Service, mqttCfg.MQTT)

	mqttServer := mqinit.NewServer(mqttCfg, ingestionService, mqttLogger)

	go func() {
		if err := mqttServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Server failed during test execution")
		}
	}()
	defer mqttServer.Shutdown()
	time.Sleep(2 * time.Second) // Give the server and MQTT client time to connect

	// --- 3. Configure and Start BigQueryConfig Processing Service ---
	bqInitCfg := &bqinit.Config{
		LogLevel:  "debug",
		HTTPPort:  e2eBqHTTPPort,
		ProjectID: testProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: testPubsubSubscriptionID},
		BigQueryConfig: bqstore.BigQueryDatasetConfig{
			ProjectID: testProjectID,
			DatasetID: e2eBigQueryDatasetID,
			TableID:   e2eBigQueryTableID,
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
	bqClient, err := bigquery.NewClient(ctx, testProjectID, bqOptions...)
	defer bqClient.Close()

	bqConsumer, err := consumers.NewGooglePubSubConsumer(ctx, &consumers.GooglePubSubConsumerConfig{
		ProjectID:      testProjectID,
		SubscriptionID: testPubsubSubscriptionID,
	}, pubsubOptions, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new, single convenience constructor ***
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, &bqInitCfg.BatchProcessing.BatchInserterConfig, &bqInitCfg.BigQueryConfig, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new service constructor ***
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](bqInitCfg.BatchProcessing.NumWorkers, bqConsumer, batchInserter, types.ConsumedMessageTransformer, bqLogger)
	require.NoError(t, err)

	bqServer := bqinit.NewServer(bqInitCfg, processingService, bqLogger)

	go func() {
		if err := bqServer.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("BigQueryConfig Processing server failed: %v", err)
		}
	}()
	defer bqServer.Shutdown()

	time.Sleep(3 * time.Second) // Give services time to start and connect

	// --- 4. Publish a Test Message to MQTT ---
	mqttTestPublisher, err := emulators.CreateTestMqttPublisher(mqttBrokerURL, "e2e-test-publisher")
	require.NoError(t, err)
	defer mqttTestPublisher.Disconnect(250)

	const messageCount = 1
	testPayload := types.GardenMonitorReadings{
		DE:       testMqttDeviceUID,
		Sequence: 123,
		Battery:  98,
	}
	msgBytes, err := json.Marshal(types.GardenMonitorMessage{Payload: &testPayload})
	require.NoError(t, err)
	publishTopic := strings.Replace(testMqttTopicPattern, "+", testMqttDeviceUID, 1)
	token := mqttTestPublisher.Publish(publishTopic, 1, false, msgBytes)
	token.Wait()
	require.NoError(t, token.Error())
	log.Info().Msg("E2E: Published test message to MQTT.")

	// --- 5. Verify Data in BigQueryConfig using the correct polling method ---
	var receivedRows []types.GardenMonitorReadings
	var lastQueryErr error
	timeout := time.After(30 * time.Second)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-timeout:
			t.Fatalf("Test timed out waiting for BigQueryConfig results. Last error: %v", lastQueryErr)
		case <-tick.C:
			queryString := fmt.Sprintf("SELECT * FROM `%s.%s` WHERE uid = @uid", e2eBigQueryDatasetID, e2eBigQueryTableID)
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

			if len(currentRows) >= messageCount {
				receivedRows = currentRows
				log.Info().Int("count", len(receivedRows)).Msg("E2E: Successfully found rows, breaking verification loop.")
				break VerificationLoop // Success!
			}
			lastQueryErr = fmt.Errorf("still waiting for rows: got %d, want %d", len(currentRows), messageCount)
		}
	}

	// --- 6. Final Assertions ---
	require.Len(t, receivedRows, messageCount)
	assert.Equal(t, testPayload.DE, receivedRows[0].DE)
	assert.Equal(t, testPayload.Sequence, receivedRows[0].Sequence)
	assert.Equal(t, testPayload.Battery, receivedRows[0].Battery)
	log.Info().Msg("E2E: Verification successful!")
}
