package servicemanager

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// ProvisionedPubSubTopic holds details of a created Pub/Sub topic.
type ProvisionedPubSubTopic struct {
	Name string
}

// ProvisionedPubSubSubscription holds details of a created Pub/Sub subscription.
type ProvisionedPubSubSubscription struct {
	Name  string
	Topic string
}

// ProvisionedGCSBucket holds details of a created GCS bucket.
type ProvisionedGCSBucket struct {
	Name string
}

// ProvisionedBigQueryDataset holds details of a created BigQuery dataset.
type ProvisionedBigQueryDataset struct {
	Name string
}

// ProvisionedBigQueryTable holds details of a created BigQuery table.
type ProvisionedBigQueryTable struct {
	Dataset string
	Name    string
}

// ProvisionedResources contains the details of all resources created by a setup operation.
type ProvisionedResources struct {
	PubSubTopics        []ProvisionedPubSubTopic
	PubSubSubscriptions []ProvisionedPubSubSubscription
	GCSBuckets          []ProvisionedGCSBucket
	BigQueryDatasets    []ProvisionedBigQueryDataset
	BigQueryTables      []ProvisionedBigQueryTable
}

// ServiceManager coordinates all resource-specific managers.
type ServiceManager struct {
	pubsub      *PubSubManager
	storage     *StorageManager
	bigquery    *BigQueryManager
	servicesDef ServicesDefinition // Use the ServicesDefinition interface
	logger      zerolog.Logger
}

// NewServiceManager creates a new central manager, initializing all sub-managers.
// It accepts a schemaRegistry to configure the BigQueryManager.
func NewServiceManager(ctx context.Context, servicesDef ServicesDefinition, env string, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceManager, error) {
	projectID, err := servicesDef.GetProjectID(env)
	if err != nil {
		return nil, err
	}

	// Create real GCP clients (or mocks for testing)
	gcsClient, err := CreateGoogleGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	bqClient, err := CreateGoogleBigQueryClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}
	psClient, err := CreateGooglePubSubClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create PubSub client: %w", err)
	}

	// Initialize individual managers
	psManager, err := NewPubSubManager(psClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub manager: %w", err)
	}
	gcsManager, err := NewStorageManager(gcsClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage manager: %w", err)
	}
	// Pass the schema registry to the BigQueryManager
	bqManager, err := NewBigQueryManager(bqClient, logger, schemaRegistry)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery manager: %w", err)
	}

	return &ServiceManager{
		pubsub:      psManager,
		storage:     gcsManager,
		bigquery:    bqManager,
		servicesDef: servicesDef,
		logger:      logger,
	}, nil
}

// SetupAll runs the setup process for all resource types in the provided config.
// It now returns a summary of the provisioned resources.
func (sm *ServiceManager) SetupAll(ctx context.Context, cfg *TopLevelConfig, environment string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment setup...")
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error { return sm.pubsub.Setup(gCtx, cfg, environment) })
	g.Go(func() error { return sm.storage.Setup(gCtx, cfg, environment) })
	g.Go(func() error { return sm.bigquery.Setup(gCtx, cfg, environment) })

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed during parallel setup: %w", err)
	}

	// Ideally, the individual managers (pubsub, storage, bigquery) would return
	// the details of the resources they created. For now, we'll construct the
	// result from the input configuration. This establishes the new API contract.
	provResources := &ProvisionedResources{}
	for _, topic := range cfg.Resources.PubSubTopics {
		provResources.PubSubTopics = append(provResources.PubSubTopics, ProvisionedPubSubTopic{Name: topic.Name})
	}
	for _, sub := range cfg.Resources.PubSubSubscriptions {
		provResources.PubSubSubscriptions = append(provResources.PubSubSubscriptions, ProvisionedPubSubSubscription{Name: sub.Name, Topic: sub.Topic})
	}
	for _, bucket := range cfg.Resources.GCSBuckets {
		provResources.GCSBuckets = append(provResources.GCSBuckets, ProvisionedGCSBucket{Name: bucket.Name})
	}
	for _, dataset := range cfg.Resources.BigQueryDatasets {
		provResources.BigQueryDatasets = append(provResources.BigQueryDatasets, ProvisionedBigQueryDataset{Name: dataset.Name})
	}
	for _, table := range cfg.Resources.BigQueryTables {
		provResources.BigQueryTables = append(provResources.BigQueryTables, ProvisionedBigQueryTable{Dataset: table.Dataset, Name: table.Name})
	}

	sm.logger.Info().Str("environment", environment).Msg("Full environment setup completed successfully.")
	return provResources, nil
}

// SetupDataflow provisions all resources associated with a specific dataflow.
func (sm *ServiceManager) SetupDataflow(ctx context.Context, environment string, dataflowName string) (*ProvisionedResources, error) {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting setup for specific dataflow")

	targetDataflow, err := sm.servicesDef.GetDataflow(dataflowName)
	if err != nil {
		return nil, err
	}
	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return nil, err
	}

	serviceSet := make(map[string]struct{})
	for _, serviceName := range targetDataflow.Services {
		serviceSet[serviceName] = struct{}{}
	}

	dataflowConfig := sm.filterConfigForServices(fullConfig, serviceSet)

	sm.logger.Info().
		Str("dataflow", dataflowName).
		Int("topics", len(dataflowConfig.Resources.PubSubTopics)).
		Int("subscriptions", len(dataflowConfig.Resources.PubSubSubscriptions)).
		Int("gcs_buckets", len(dataflowConfig.Resources.GCSBuckets)).
		Int("bq_tables", len(dataflowConfig.Resources.BigQueryTables)).
		Msg("Filtered resources for dataflow. Proceeding with setup.")

	return sm.SetupAll(ctx, dataflowConfig, environment)
}

// TeardownAll runs the teardown process for all resource types defined in the provided config.
func (sm *ServiceManager) TeardownAll(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	sm.logger.Info().Str("environment", environment).Msg("Starting full environment teardown...")
	if err := sm.bigquery.Teardown(ctx, cfg, environment); err != nil {
		return err
	}
	if err := sm.storage.Teardown(ctx, cfg, environment); err != nil {
		return err
	}
	if err := sm.pubsub.Teardown(ctx, cfg, environment); err != nil {
		return err
	}
	sm.logger.Info().Str("environment", environment).Msg("Full environment teardown completed successfully.")
	return nil
}

// TeardownDataflow tears down all resources associated with a specific dataflow.
func (sm *ServiceManager) TeardownDataflow(ctx context.Context, environment string, dataflowName string) error {
	sm.logger.Info().Str("dataflow", dataflowName).Str("environment", environment).Msg("Starting teardown for specific dataflow")

	targetDataflow, err := sm.servicesDef.GetDataflow(dataflowName)
	if err != nil {
		return err
	}

	if targetDataflow.Lifecycle == nil || targetDataflow.Lifecycle.Strategy != LifecycleStrategyEphemeral {
		sm.logger.Warn().
			Str("dataflow", dataflowName).
			Msg("Teardown skipped: Dataflow is not marked with an 'ephemeral' lifecycle strategy.")
		return nil
	}

	fullConfig, err := sm.servicesDef.GetTopLevelConfig()
	if err != nil {
		return err
	}

	serviceSet := make(map[string]struct{})
	for _, serviceName := range targetDataflow.Services {
		serviceSet[serviceName] = struct{}{}
	}

	dataflowConfig := sm.filterConfigForServices(fullConfig, serviceSet)

	if targetDataflow.Lifecycle.KeepDatasetOnTest {
		sm.logger.Info().
			Str("dataflow", dataflowName).
			Msg("'KeepDatasetOnTest' is true. BigQuery datasets will be excluded from teardown.")
		dataflowConfig.Resources.BigQueryDatasets = nil
	}

	sm.logger.Info().
		Str("dataflow", dataflowName).
		Str("lifecycle_strategy", string(targetDataflow.Lifecycle.Strategy)).
		Msg("Filtered resources for dataflow. Proceeding with teardown.")

	return sm.TeardownAll(ctx, dataflowConfig, environment)
}

// filterConfigForServices is a helper to generate a temporary config containing
// only the resources needed by a given set of services.
func (sm *ServiceManager) filterConfigForServices(fullConfig *TopLevelConfig, serviceSet map[string]struct{}) *TopLevelConfig {
	dataflowConfig := &TopLevelConfig{
		DefaultProjectID: fullConfig.DefaultProjectID,
		DefaultLocation:  fullConfig.DefaultLocation,
		Environments:     fullConfig.Environments,
		Services:         fullConfig.Services,
		Dataflows:        fullConfig.Dataflows,
		Resources:        ResourcesSpec{}, // Start with empty resources
	}

	for _, topic := range fullConfig.Resources.PubSubTopics {
		if _, ok := serviceSet[topic.ProducerService]; ok {
			dataflowConfig.Resources.PubSubTopics = append(dataflowConfig.Resources.PubSubTopics, topic)
		}
	}
	for _, sub := range fullConfig.Resources.PubSubSubscriptions {
		if _, ok := serviceSet[sub.ConsumerService]; ok {
			dataflowConfig.Resources.PubSubSubscriptions = append(dataflowConfig.Resources.PubSubSubscriptions, sub)
		}
	}
	for _, bucket := range fullConfig.Resources.GCSBuckets {
		for _, accessingService := range bucket.AccessingServices {
			if _, ok := serviceSet[accessingService]; ok {
				dataflowConfig.Resources.GCSBuckets = append(dataflowConfig.Resources.GCSBuckets, bucket)
				break
			}
		}
	}
	for _, table := range fullConfig.Resources.BigQueryTables {
		for _, accessingService := range table.AccessingServices {
			if _, ok := serviceSet[accessingService]; ok {
				dataflowConfig.Resources.BigQueryTables = append(dataflowConfig.Resources.BigQueryTables, table)
				break
			}
		}
	}

	requiredDatasets := make(map[string]struct{})
	for _, table := range dataflowConfig.Resources.BigQueryTables {
		requiredDatasets[table.Dataset] = struct{}{}
	}
	for _, dataset := range fullConfig.Resources.BigQueryDatasets {
		if _, ok := requiredDatasets[dataset.Name]; ok {
			dataflowConfig.Resources.BigQueryDatasets = append(dataflowConfig.Resources.BigQueryDatasets, dataset)
		}
	}

	return dataflowConfig
}
