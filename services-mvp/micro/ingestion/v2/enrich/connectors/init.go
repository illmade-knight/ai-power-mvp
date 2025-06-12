package connectors

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
)

// EnrichmentServiceDependencies holds all initialized dependencies for the enrichment service.
type EnrichmentServiceDependencies struct {
	Service               *EnrichmentService // Changed from IngestionService
	Logger                zerolog.Logger
	InputSubscriptionCfg  *PubSubSubscriptionConfig // New: Config for the input subscription
	EnrichedPublisher     *GooglePubSubPublisher
	UnidentifiedPublisher *GooglePubSubPublisher
	MetadataFetcher       *GoogleDeviceMetadataFetcher
	ServiceConfig         EnrichmentServiceConfig // Holds worker counts, channel capacities etc.
}

// InitializeEnrichmentServiceDependencies sets up all necessary configurations and components
// for the new enrichment service.
func InitializeEnrichmentServiceDependencies(ctx context.Context, baseLogger zerolog.Logger) (*EnrichmentServiceDependencies, error) {
	logger := baseLogger.With().Str("component_setup", "enrichment_service_dependencies").Logger()
	logger.Info().Msg("Initializing enrichment service dependencies...")

	var err error
	dependencies := &EnrichmentServiceDependencies{Logger: logger}

	// 1. Load configurations
	// Input Pub/Sub Subscription Configuration
	// Using generic env var names here, replace with your actual env var names
	dependencies.InputSubscriptionCfg, err = LoadInputPubSubSubscriptionConfigFromEnv("INPUT_PUBSUB_PROJECT_ID", "INPUT_PUBSUB_SUBSCRIPTION_ID")
	if err != nil {
		return nil, fmt.Errorf("failed to load input Pub/Sub subscription config: %w", err)
	}
	logger.Info().Str("input_project_id", dependencies.InputSubscriptionCfg.ProjectID).Str("input_subscription_id", dependencies.InputSubscriptionCfg.SubscriptionID).Msg("Input Pub/Sub subscription config loaded.")

	// Output Publisher Configurations (reusing existing functions)
	enrichedPubCfg, err := LoadGooglePubSubPublisherConfigFromEnv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES")
	if err != nil {
		return nil, fmt.Errorf("failed to load enriched publisher config: %w", err)
	}
	logger.Info().Str("topic_id", enrichedPubCfg.TopicID).Msg("Enriched publisher config loaded.")

	unidentifiedPubCfg, err := LoadGooglePubSubPublisherConfigFromEnv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES")
	if err != nil {
		return nil, fmt.Errorf("failed to load unidentified publisher config: %w", err)
	}
	logger.Info().Str("topic_id", unidentifiedPubCfg.TopicID).Msg("Unidentified publisher config loaded.")

	// Firestore Fetcher Configuration
	firestoreFetcherCfg, err := LoadFirestoreFetcherConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to load Firestore fetcher config: %w", err)
	}
	logger.Info().Msg("Firestore fetcher config loaded.")

	// Enrichment Service Worker/Channel Configuration
	numWorkers, chanCap, maxOutstanding, loadErr := LoadEnrichmentServiceWorkerConfigFromEnv()
	if loadErr != nil {
		logger.Warn().Err(loadErr).Msg("Error loading some worker/channel configs, using defaults for problematic ones.")
		// Continue with defaults for parts that failed to load if desired, or return error
	}
	dependencies.ServiceConfig = DefaultEnrichmentServiceConfig()                           // Start with defaults
	dependencies.ServiceConfig.InputSubscriptionConfig = *dependencies.InputSubscriptionCfg // Already loaded
	dependencies.ServiceConfig.NumProcessingWorkers = numWorkers
	dependencies.ServiceConfig.InputChanCapacity = chanCap
	dependencies.ServiceConfig.MaxOutstandingMessages = maxOutstanding
	logger.Info().
		Int("num_workers", dependencies.ServiceConfig.NumProcessingWorkers).
		Int("input_chan_capacity", dependencies.ServiceConfig.InputChanCapacity).
		Int("max_outstanding_msgs", dependencies.ServiceConfig.MaxOutstandingMessages).
		Msg("Enrichment service worker/channel config prepared.")

	// 2. Create concrete instances of dependencies
	dependencies.EnrichedPublisher, err = NewGooglePubSubPublisher(ctx, enrichedPubCfg, logger.With().Str("publisher_type", "enriched").Logger())
	if err != nil {
		return nil, fmt.Errorf("failed to create enriched publisher: %w", err)
	}
	logger.Info().Msg("Enriched publisher created.")

	dependencies.UnidentifiedPublisher, err = NewGooglePubSubPublisher(ctx, unidentifiedPubCfg, logger.With().Str("publisher_type", "unidentified").Logger())
	if err != nil {
		dependencies.EnrichedPublisher.Stop()
		return nil, fmt.Errorf("failed to create unidentified publisher: %w", err)
	}
	logger.Info().Msg("Unidentified publisher created.")

	dependencies.MetadataFetcher, err = NewGoogleDeviceMetadataFetcher(ctx, firestoreFetcherCfg, logger.With().Str("component", "FirestoreFetcher").Logger())
	if err != nil {
		dependencies.EnrichedPublisher.Stop()
		dependencies.UnidentifiedPublisher.Stop()
		return nil, fmt.Errorf("failed to create metadata fetcher: %w", err)
	}
	logger.Info().Msg("Metadata fetcher created.")

	// 3. Create the EnrichmentService instance
	dependencies.Service, err = NewEnrichmentService(
		ctx,
		dependencies.ServiceConfig,
		*dependencies.InputSubscriptionCfg, // Pass the loaded input subscription config
		dependencies.MetadataFetcher.Fetch, // The Fetch method satisfies DeviceMetadataFetcher
		dependencies.EnrichedPublisher,
		dependencies.UnidentifiedPublisher,
		logger.With().Str("service_type", "enrichment").Logger(),
	)
	if err != nil {
		// Cleanup already created components if service creation fails
		dependencies.EnrichedPublisher.Stop()
		dependencies.UnidentifiedPublisher.Stop()
		if dependencies.MetadataFetcher != nil {
			dependencies.MetadataFetcher.Close()
		}
		return nil, fmt.Errorf("failed to create EnrichmentService: %w", err)
	}
	logger.Info().Msg("EnrichmentService instance created.")

	return dependencies, nil
}
