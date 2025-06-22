//go:build integration

package loade2e_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/icestore/icinit"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/ingestion/mqinit"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/icestore"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// --- Test Flags ---
var (
	keepResources = flag.Bool("keep-resources", false, "If true, ephemeral resources (like GCS buckets) will not be deleted after the test.")
)

// --- Test Constants ---
const (
	cloudTestGCPProjectID  = "gemini-power-test"
	loadTestDuration       = 30 * time.Second
	loadTestNumDevices     = 10
	loadTestRatePerDevice  = 2.0
	cloudLoadTestTimeout   = 10 * time.Minute
	testMqttTopicPattern   = "devices/+/data"
	testMqttClientIDPrefix = "ingestion-service-cloud-load-gcs"
	cloudTestMqttHTTPPort  = ":9094"
	cloudTestGCSHTTPPort   = ":9095"
	cloudGCSBucketPrefix   = "e2e-garden-monitor-loadtest-"
	maxBucketNameLength    = 63
)

// buildTestServicesDefinitionGCS creates a complete TopLevelConfig in memory for the GCS load test.
func buildTestServicesDefinitionGCS(runID, gcpProjectID string) *servicemanager.TopLevelConfig {
	dataflowName := "mqtt-to-gcs-loadtest"
	topicID := fmt.Sprintf("processed-device-data-gcs-%s", runID)
	subscriptionID := fmt.Sprintf("archival-service-sub-gcs-%s", runID)
	bucketName := fmt.Sprintf("%s%s", cloudGCSBucketPrefix, runID)

	bucketName = strings.ToLower(strings.ReplaceAll(bucketName, "_", "-"))
	if len(bucketName) > maxBucketNameLength {
		bucketName = bucketName[:maxBucketNameLength]
	}

	return &servicemanager.TopLevelConfig{
		DefaultProjectID: gcpProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"loadtest": {ProjectID: gcpProjectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "ingestion-service"},
			{Name: "archival-service"},
		},
		Dataflows: []servicemanager.DataflowSpec{
			{
				Name:     dataflowName,
				Services: []string{"ingestion-service", "archival-service"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy:            servicemanager.LifecycleStrategyEphemeral,
					KeepResourcesOnTest: *keepResources,
				},
			},
		},
		Resources: servicemanager.ResourcesSpec{
			MessagingTopics: []servicemanager.MessagingTopicConfig{
				{Name: topicID, ProducerService: "ingestion-service"},
			},
			MessagingSubscriptions: []servicemanager.MessagingSubscriptionConfig{
				{Name: subscriptionID, Topic: topicID, ConsumerService: "archival-service"},
			},
			GCSBuckets: []servicemanager.GCSBucket{
				{
					Name:              bucketName,
					AccessingServices: []string{"archival-service"},
				},
			},
		},
	}
}

// startGCSProcessingService has been updated to be more robust against propagation delays.
// startGCSProcessingService has been updated to be more robust against propagation delays.
func startGCSProcessingService(t *testing.T, ctx context.Context, gcpProjectID, subID, bucketName string) (*consumers.ProcessingService[icestore.ArchivalData], *icinit.Server) {
	t.Helper()
	icestoreCfg := &icinit.IceServiceConfig{
		LogLevel:  "info",
		HTTPPort:  cloudTestGCSHTTPPort,
		ProjectID: gcpProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: subID},
		IceStore: struct {
			CredentialsFile string `mapstructure:"credentials_file"`
			BucketName      string `mapstructure:"bucket_name"`
		}{BucketName: bucketName},
		BatchProcessing: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{NumWorkers: 5, BatchSize: 4, FlushTimeout: time.Second * 10},
	}

	gcsLogger := log.With().Str("service", "gcs-processor").Logger()

	// Create clients for GCS and Pub/Sub that this service needs to operate.
	gcsClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { gcsClient.Close() })

	pubsubClient, err := pubsub.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)
	t.Cleanup(func() { pubsubClient.Close() })

	// Add a retry loop to wait for the bucket to become available.
	bucketHandle := gcsClient.Bucket(bucketName)
	var bucketExists bool
	for i := 0; i < 5; i++ {
		_, err := bucketHandle.Attrs(ctx)
		if err == nil {
			bucketExists = true
			gcsLogger.Info().Str("bucket", bucketName).Msg("Successfully confirmed GCS bucket exists.")
			break
		}
		if !errors.Is(err, storage.ErrBucketNotExist) {
			require.NoError(t, err, "Failed to get bucket attributes with unexpected error")
		}
		gcsLogger.Warn().Str("bucket", bucketName).Int("attempt", i+1).Msg("Bucket not yet found, retrying...")
		time.Sleep(2 * time.Second)
	}
	require.True(t, bucketExists, "GCS bucket %s was not found after multiple retries", bucketName)

	// Add a similar retry loop for the Pub/Sub subscription.
	subscription := pubsubClient.Subscription(subID)
	var subExists bool
	for i := 0; i < 5; i++ {
		exists, err := subscription.Exists(ctx)
		if err != nil {
			require.NoError(t, err, "Failed to check for subscription existence with an unexpected error")
		}
		if exists {
			subExists = true
			gcsLogger.Info().Str("subscription", subID).Msg("Successfully confirmed Pub/Sub subscription exists.")
			break
		}
		gcsLogger.Warn().Str("subscription", subID).Int("attempt", i+1).Msg("Subscription not yet found, retrying...")
		time.Sleep(2 * time.Second)
	}
	require.True(t, subExists, "Pub/Sub subscription %s was not found after multiple retries", subID)

	// Now that resources are confirmed to exist, create the consumer.
	gcsConsumer, err := consumers.NewGooglePubsubConsumer(ctx, &consumers.GooglePubsubConsumerConfig{
		ProjectID:      icestoreCfg.ProjectID,
		SubscriptionID: icestoreCfg.Consumer.SubscriptionID,
	}, nil, gcsLogger) // Pass the existing client to the consumer
	require.NoError(t, err)

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{BatchSize: icestoreCfg.BatchProcessing.BatchSize, FlushTimeout: icestoreCfg.BatchProcessing.FlushTimeout},
		icestore.GCSBatchUploaderConfig{BucketName: bucketName, ObjectPrefix: "archived-data"},
		gcsLogger,
	)
	require.NoError(t, err)

	processingService, err := icestore.NewIceStorageService(icestoreCfg.BatchProcessing.NumWorkers, gcsConsumer, batcher, icestore.ArchivalTransformer, gcsLogger)
	require.NoError(t, err)

	gcsServer := icinit.NewServer(icestoreCfg, processingService, gcsLogger)
	go func() {
		if srvErr := gcsServer.Start(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			t.Errorf("GCS Processing server failed: %v", srvErr)
		}
	}()
	return processingService, gcsServer
}

// TestManagedCloudGCSSLoad subjects the entire pipeline to load, using the ServiceManager for setup and teardown.
func TestManagedCloudGCSSLoad(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud GCS load test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}
	t.Setenv("GOOGLE_CLOUD_PROJECT", projectID)

	ctx, cancel := context.WithTimeout(context.Background(), cloudLoadTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	testLogger := log.Logger

	runID := uuid.New().String()[:8]
	servicesConfig := buildTestServicesDefinitionGCS(runID, projectID)

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	schemaRegistry := make(map[string]interface{}) // Not used by GCS flow but required by manager
	schemaRegistry["github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings"] = types.GardenMonitorReadings{}

	log.Info().Str("runID", runID).Msg("Initializing ServiceManager to provision dataflow...")
	manager, err := servicemanager.NewServiceManager(ctx, servicesDef, "loadtest", schemaRegistry, testLogger)
	require.NoError(t, err)

	dataflowName := servicesConfig.Dataflows[0].Name
	provisioned, err := manager.SetupDataflow(ctx, "loadtest", dataflowName)
	require.NoError(t, err, "ServiceManager.SetupDataflow failed")

	//seems to take a while for subscriptions to form
	time.Sleep(time.Second * 10)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		log.Info().Str("runID", runID).Msg("Test finished. Tearing down dataflow resources...")
		teardownErr := manager.TeardownDataflow(cleanupCtx, "loadtest", dataflowName)
		assert.NoError(t, teardownErr, "ServiceManager.TeardownDataflow failed")
	})

	require.NotEmpty(t, provisioned.GCSBuckets, "Provisioning must return a GCS bucket")
	bucketName := provisioned.GCSBuckets[0].Name
	require.NotEmpty(t, provisioned.PubSubTopics, "Provisioning must return a Pub/Sub topic")
	topicID := provisioned.PubSubTopics[0].Name
	require.NotEmpty(t, provisioned.PubSubSubscriptions, "Provisioning must return a Pub/Sub subscription")
	subscriptionID := provisioned.PubSubSubscriptions[0].Name

	log.Info().Str("bucket", bucketName).Str("topic", topicID).Str("subscription", subscriptionID).Msg("Using provisioned resources")

	log.Info().Msg("LoadTest: Setting up Mosquitto container...")
	mqttBrokerURL, mosquittoCleanup := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	defer mosquittoCleanup()

	_, mqttServer := startIngestionService(t, ctx, projectID, mqttBrokerURL, topicID)
	defer mqttServer.Shutdown()

	_, gcsServer := startGCSProcessingService(t, ctx, projectID, subscriptionID, bucketName)
	defer gcsServer.Shutdown()

	log.Info().Msg("LoadTest: Pausing to allow services to start and connect...")
	time.Sleep(10 * time.Second)

	log.Info().Msg("LoadTest: Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()
	loadgenClient := loadgen.NewMqttClient(mqttBrokerURL, testMqttTopicPattern, 1, loadgenLogger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{
			ID:               fmt.Sprintf("load-test-device-%d", i),
			MessageRate:      loadTestRatePerDevice,
			PayloadGenerator: &gardenMonitorPayloadGenerator{},
		}
	}

	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)
	err = loadGenerator.Run(ctx, loadTestDuration)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "Load generator returned an unexpected error")
	}

	log.Info().Msg("Load generation complete. Verifying results in GCS...")
	verifyGCSResults(t, ctx, bucketName)
}

// verifyGCSResults polls the GCS bucket until the object count stabilizes, then asserts success.
func verifyGCSResults(t *testing.T, ctx context.Context, bucketName string) {
	t.Helper()
	expectedMinGCSObjects := 1 // At least one file should be created

	gcsClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	defer gcsClient.Close()
	bucketHandle := gcsClient.Bucket(bucketName)

	var lastObjectCount = -1
	var pollsWithNoChange int
	const maxPollsWithNoChange = 3

	verificationCtx, verificationCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer verificationCancel()
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-verificationCtx.Done():
			t.Fatalf("Test timed out waiting for GCS objects. Last count: %d, Expected Min: %d", lastObjectCount, expectedMinGCSObjects)
		case <-tick.C:
			currentObjects, err := listGCSObjectAttrs(t, verificationCtx, bucketHandle)
			if err != nil {
				log.Warn().Err(err).Msg("Polling GCS objects failed")
				continue
			}
			currentObjectCount := len(currentObjects)
			log.Info().Int("current_count", currentObjectCount).Int("last_count", lastObjectCount).Int("stable_polls", pollsWithNoChange).Msg("Polling GCS object count")

			if currentObjectCount == lastObjectCount {
				pollsWithNoChange++
			} else {
				pollsWithNoChange = 0
			}
			lastObjectCount = currentObjectCount

			if pollsWithNoChange >= maxPollsWithNoChange {
				require.GreaterOrEqual(t, lastObjectCount, expectedMinGCSObjects, "GCS object count stabilized at %d, which is below the success threshold of %d.", lastObjectCount, expectedMinGCSObjects)
				log.Info().Msg("GCS object count has stabilized and meets the success threshold. Verification successful!")
				return // Exit verification
			}
		}
	}
}

// listGCSObjectAttrs is a helper function to list objects in a GCS bucket.
func listGCSObjectAttrs(t *testing.T, ctx context.Context, bucket *storage.BucketHandle) ([]*storage.ObjectAttrs, error) {
	t.Helper()
	var attrs []*storage.ObjectAttrs
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		attrs = append(attrs, objAttrs)
	}
	return attrs, nil
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

type gardenMonitorPayloadGenerator struct {
	mu            sync.Mutex
	isInitialized bool
	sequence      *loadgen.Sequence
	// ... other state fields
}

func (g *gardenMonitorPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isInitialized {
		g.sequence = loadgen.NewSequence(0)
		// ... initial values
		g.isInitialized = true
	}
	// ... generate payload logic
	payload := types.GardenMonitorReadings{
		DE:        device.ID,
		Sequence:  int(g.sequence.Next()),
		Timestamp: time.Now().UTC(),
		// ... other fields
	}
	return json.Marshal(payload)
}
