package connectors // Changed package to connectors

import (
	"context"
	"fmt"
	// "os" // Assuming Load...FromEnv functions handle os.Getenv internally

	"github.com/rs/zerolog"
	// No need to import "your_module_path/ingestion/connectors" as this file is now part of it.
)

// CoreServiceDependencies holds all the initialized dependencies for the core ingestion service.
// This struct can be returned by the setup function to provide access to individual components
// if needed for direct interaction or cleanup in tests or main.
type CoreServiceDependencies struct {
	Service               *IngestionService // Now directly IngestionService from this package
	Logger                zerolog.Logger
	MQTTConfig            *MQTTClientConfig // Directly from this package
	EnrichedPublisher     *GooglePubSubPublisher
	UnidentifiedPublisher *GooglePubSubPublisher
	MetadataFetcher       *GoogleDeviceMetadataFetcher
}

// InitializeAndGetCoreService sets up all necessary configurations and components
// for the core ingestion service. It's designed to be used by both main.go
// for the actual service run and by integration tests.
// This function is now part of the 'connectors' package.
func InitializeAndGetCoreService(ctx context.Context, baseLogger zerolog.Logger) (*CoreServiceDependencies, error) {
	logger := baseLogger.With().Str("component_setup", "core_dependencies").Logger()
	logger.Info().Msg("Initializing core service dependencies (from connectors package)...")

	var err error
	dependencies := &CoreServiceDependencies{Logger: logger}

	// 1. Load configurations using functions from this 'connectors' package
	dependencies.MQTTConfig, err = LoadMQTTClientConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to load MQTT config: %w", err)
	}
	logger.Info().Msg("MQTT config loaded.")

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

	firestoreFetcherCfg, err := LoadFirestoreFetcherConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to load Firestore fetcher config: %w", err)
	}
	logger.Info().Msg("Firestore fetcher config loaded.")

	// 2. Create concrete instances of dependencies (types are now directly from this package)
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

	// 3. Create core IngestionService config
	serviceCfg := DefaultIngestionServiceConfig() // Assumes this is in the connectors package
	logger.Info().Msg("Core service config prepared.")

	// 4. Create the core IngestionService instance
	dependencies.Service = NewIngestionService( // Assumes NewIngestionService is in the connectors package
		dependencies.MetadataFetcher.Fetch,
		dependencies.EnrichedPublisher,
		dependencies.UnidentifiedPublisher,
		logger,
		serviceCfg,
		dependencies.MQTTConfig,
	)
	logger.Info().Msg("Core IngestionService instance created.")

	return dependencies, nil
}
