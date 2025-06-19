//go:build integration

package managedload_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/bigquery/bqinit"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/ingestion/mqinit"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
)

// --- Test Flags ---
var (
	keepDataset = flag.Bool("keep-dataset", false, "If true, the BigQueryConfig dataset will not be deleted after the test, but will expire automatically after 1 day.")
)

// --- Test Constants ---
const (
	cloudTestGCPProjectID  = "gemini-power-test"
	loadTestDuration       = 30 * time.Second
	loadTestNumDevices     = 10
	loadTestRatePerDevice  = 2.0
	loadTestSuccessPercent = 0.98
	cloudLoadTestTimeout   = 10 * time.Minute
	testMqttTopicPattern   = "devices/+/data"
	testMqttClientIDPrefix = "ingestion-service-cloud-load"
	cloudTestMqttHTTPPort  = ":9092"
	cloudTestBqHTTPPort    = ":9093"
)

// buildTestServicesDefinition creates a complete TopLevelConfig in memory for the load test.
func buildTestServicesDefinition(runID, gcpProjectID string) *servicemanager.TopLevelConfig {
	dataflowName := "mqtt-to-bigquery-loadtest"
	topicID := fmt.Sprintf("processed-device-data-%s", runID)
	subscriptionID := fmt.Sprintf("analysis-service-sub-%s", runID)
	datasetID := fmt.Sprintf("device_data_analytics_%s", runID)
	tableID := fmt.Sprintf("monitor_payloads_%s", runID)

	return &servicemanager.TopLevelConfig{
		DefaultProjectID: gcpProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"loadtest": {ProjectID: gcpProjectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "ingestion-service", ServiceAccount: "ingestion-sa@your-gcp-project.iam.gserviceaccount.com"},
			{Name: "analysis-service", ServiceAccount: "analysis-sa@your-gcp-project.iam.gserviceaccount.com"},
		},
		Dataflows: []servicemanager.DataflowSpec{
			{
				Name:        dataflowName,
				Description: "Ephemeral dataflow for the cloud load test.",
				Services:    []string{"ingestion-service", "analysis-service"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy:          servicemanager.LifecycleStrategyEphemeral,
					KeepDatasetOnTest: *keepDataset, // Set from the command-line flag
				},
			},
		},
		Resources: servicemanager.ResourcesSpec{
			PubSubTopics: []servicemanager.PubSubTopic{
				{Name: topicID, ProducerService: "ingestion-service"},
			},
			PubSubSubscriptions: []servicemanager.PubSubSubscription{
				{Name: subscriptionID, Topic: topicID, ConsumerService: "analysis-service"},
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{Name: datasetID, Description: "Temp dataset for load test"},
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{
					Name:                   tableID,
					Dataset:                datasetID,
					AccessingServices:      []string{"analysis-service"},
					SchemaSourceType:       "go_struct",
					SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings",
				},
			},
		},
	}
}

// TestManagedCloudLoad subjects the entire pipeline to load, using the ServiceManager for setup and teardown.
func TestManagedCloudLoad(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	// --- 1. Test Setup & Authentication ---
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud load test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}
	t.Setenv("GOOGLE_CLOUD_PROJECT", projectID)

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		realPubSubClient, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
		realPubSubClient.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), cloudLoadTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	testLogger := log.Logger

	// --- 2. Create Dynamic Services Definition for this Test Run ---
	runID := uuid.New().String()[:8]
	servicesConfig := buildTestServicesDefinition(runID, projectID)

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	schemaRegistry := make(map[string]interface{})
	schemaRegistry["github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings"] = types.GardenMonitorReadings{}

	// --- 3. Use ServiceManager to Provision Infrastructure ---
	log.Info().Str("runID", runID).Msg("Initializing ServiceManager to provision dataflow...")
	manager, err := servicemanager.NewServiceManager(ctx, servicesDef, "loadtest", schemaRegistry, testLogger)
	require.NoError(t, err)

	dataflowName := servicesConfig.Dataflows[0].Name
	provisioned, err := manager.SetupDataflow(ctx, "loadtest", dataflowName)
	require.NoError(t, err, "ServiceManager.SetupDataflow failed")

	t.Cleanup(func() {
		// Use a new, independent context for cleanup to ensure it runs
		// even if the main test context times out.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		log.Info().Str("runID", runID).Msg("Test finished. Tearing down dataflow resources...")
		teardownErr := manager.TeardownDataflow(cleanupCtx, "loadtest", dataflowName)
		assert.NoError(t, teardownErr, "ServiceManager.TeardownDataflow failed")
	})

	// --- 4. Get Resource Names from the Provisioning Result ---
	log.Info().Msg("Getting resource names from the provisioned resources result...")
	require.NotEmpty(t, provisioned.PubSubTopics, "Provisioning must return a Pub/Sub topic")
	topicID := provisioned.PubSubTopics[0].Name

	require.NotEmpty(t, provisioned.PubSubSubscriptions, "Provisioning must return a Pub/Sub subscription")
	subscriptionID := provisioned.PubSubSubscriptions[0].Name

	require.NotEmpty(t, provisioned.BigQueryTables, "Provisioning must return a BigQueryConfig table")
	datasetID := provisioned.BigQueryTables[0].Dataset
	tableID := provisioned.BigQueryTables[0].Name

	log.Info().Str("topic", topicID).Str("subscription", subscriptionID).Msg("Using Pub/Sub resources")
	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("Using BigQueryConfig resources")

	// --- 5. Start Local Services & Load Generator ---
	log.Info().Msg("LoadTest: Setting up Mosquitto container...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	defer mosquittoCleanup()

	ingestionService, mqttServer := startIngestionService(t, ctx, projectID, mqttBrokerURL, topicID)
	defer mqttServer.Shutdown()

	var processingErrors []error
	var errMu sync.Mutex
	go func() {
		for err := range ingestionService.Err() {
			errMu.Lock()
			processingErrors = append(processingErrors, err)
			errMu.Unlock()
			log.Error().Err(err).Msg("Received processing error from ingestion service")
		}
	}()

	_, bqServer := startBQProcessingService(t, ctx, projectID, subscriptionID, datasetID, tableID)
	defer bqServer.Shutdown()

	log.Info().Msg("LoadTest: Pausing to allow services to start and connect...")
	time.Sleep(10 * time.Second)

	log.Info().Msg("LoadTest: Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()
	loadgenClient := loadgen.NewMqttClient(mqttBrokerURL, testMqttTopicPattern, 1, loadgenLogger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	deviceIDs := make([]string, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		deviceID := fmt.Sprintf("load-test-device-%d", i)
		deviceIDs[i] = deviceID
		payloadGenerator := &gardenMonitorPayloadGenerator{}
		devices[i] = &loadgen.Device{
			ID:               deviceID,
			MessageRate:      loadTestRatePerDevice,
			PayloadGenerator: payloadGenerator,
		}
	}

	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)
	err = loadGenerator.Run(ctx, loadTestDuration)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "Load generator returned an unexpected error")
	}

	// --- 6. Verification ---
	log.Info().Msg("Load generation complete. Verifying results in BigQueryConfig...")
	expectedMessages := int(float64(loadTestNumDevices) * loadTestRatePerDevice * loadTestDuration.Seconds())
	successThreshold := int(float64(expectedMessages) * loadTestSuccessPercent)

	bqClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer bqClient.Close()

	var lastRowCount int64 = -1 // Use -1 to indicate the first run
	var pollsWithNoChange int
	const maxPollsWithNoChange = 3 // Number of stable polls before we assume completion

	verificationCtx, verificationCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer verificationCancel()
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-verificationCtx.Done():
			t.Fatalf("Test timed out waiting for BigQueryConfig results. Last count: %d, Threshold: %d", lastRowCount, successThreshold)
		case <-tick.C:
			currentRowCount, err := getRowCount(verificationCtx, bqClient, datasetID, tableID)
			if err != nil {
				log.Warn().Err(err).Msg("Polling query for row count failed")
				continue
			}

			log.Info().Int64("current_count", currentRowCount).Int64("last_count", lastRowCount).Int("stable_polls", pollsWithNoChange).Msg("Polling BQ count")

			if currentRowCount == lastRowCount {
				pollsWithNoChange++
			} else {
				pollsWithNoChange = 0
				lastRowCount = currentRowCount
			}

			if pollsWithNoChange >= maxPollsWithNoChange {
				if lastRowCount >= int64(successThreshold) {
					log.Info().Msg("Row count has stabilized and meets the success threshold. Verification successful!")
					break VerificationLoop
				} else {
					t.Fatalf("Test failed: Row count stabilized at %d, which is below the success threshold of %d.", lastRowCount, successThreshold)
				}
			}
		}
	}

	// Final check outside the loop
	finalCount, err := getRowCount(verificationCtx, bqClient, datasetID, tableID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, finalCount, int64(successThreshold), "Final count in BQ (%d) is less than success threshold (%d)", finalCount, successThreshold)

	// --- 7. Post-Test Data Manipulation (Conditional) ---
	if *keepDataset {
		log.Info().Msg("'-keep-dataset' flag is set. Retaining dataset for demos and respacing timestamps...")
		respaceTimestampsForDemo(t, ctx, bqClient, projectID, datasetID, tableID, deviceIDs)
	}

	// --- 8. Final Error Check ---
	errMu.Lock()
	defer errMu.Unlock()
	assert.Empty(t, processingErrors, "Should be no processing errors from the ingestion service")
}

// --- Helper Functions ---

func getRowCount(ctx context.Context, client *bigquery.Client, datasetID, tableID string) (int64, error) {
	queryString := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s`", datasetID, tableID)
	query := client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return 0, err
	}
	var row struct {
		Count int64 `bigquery:"count"`
	}
	if err := it.Next(&row); err != nil {
		if errors.Is(err, iterator.Done) {
			return 0, nil // No rows found, which is a valid count of 0
		}
		return 0, err
	}
	return row.Count, nil
}

func startIngestionService(t *testing.T, ctx context.Context, projectID, mqttBrokerURL, topicID string) (*mqttconverter.IngestionService, *mqinit.Server) {
	t.Helper()
	mqttCfg := &mqinit.Config{
		LogLevel: "info", HTTPPort: cloudTestMqttHTTPPort, ProjectID: projectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{TopicID: topicID},
		MQTT:    mqttconverter.MQTTClientConfig{BrokerURL: mqttBrokerURL, Topic: testMqttTopicPattern, ClientIDPrefix: testMqttClientIDPrefix},
		Service: mqttconverter.IngestionServiceConfig{InputChanCapacity: 1000, NumProcessingWorkers: 20},
	}
	logger := log.With().Str("service", "mqtt-ingestion").Logger()
	publisher, err := mqttconverter.NewGooglePubsubPublisher(ctx,
		mqttconverter.GooglePubsubPublisherConfig{ProjectID: mqttCfg.ProjectID, TopicID: mqttCfg.Publisher.TopicID}, logger)
	require.NoError(t, err)
	service := mqttconverter.NewIngestionService(publisher, nil, logger, mqttCfg.Service, mqttCfg.MQTT)
	server := mqinit.NewServer(mqttCfg, service, logger)
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Ingestion server failed during test execution")
		}
	}()
	return service, server
}

func startBQProcessingService(t *testing.T, ctx context.Context, gcpProjectID, subID, datasetID, tableID string) (*consumers.ProcessingService[types.GardenMonitorReadings], *bqinit.Server) {
	t.Helper()
	bqCfg := &bqinit.Config{
		LogLevel:  "info",
		HTTPPort:  cloudTestBqHTTPPort,
		ProjectID: gcpProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: subID},
		BigQueryConfig: bqstore.BigQueryDatasetConfig{
			ProjectID: gcpProjectID,
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
	bqClient, err := bigquery.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)

	bqConsumer, err := consumers.NewGooglePubsubConsumer(ctx, &consumers.GooglePubsubConsumerConfig{
		ProjectID:      bqCfg.ProjectID,
		SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, nil, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new, single convenience constructor ***
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, &bqCfg.BatchProcessing.BatchInserterConfig, &bqCfg.BigQueryConfig, bqLogger)
	require.NoError(t, err)

	// *** REFACTORED PART: Use the new service constructor ***
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](bqCfg.BatchProcessing.NumWorkers, bqConsumer, batchInserter, types.ConsumedMessageTransformer, bqLogger)
	require.NoError(t, err)

	bqServer := bqinit.NewServer(bqCfg, processingService, bqLogger)
	require.NoError(t, err)
	go func() {
		if srvErr := bqServer.Start(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			t.Errorf("BQ Processing server failed: %v", srvErr)
		}
	}()
	return processingService, bqServer
}
