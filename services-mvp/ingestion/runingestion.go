package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	// Replace "your_module_path/ingestion/connectors" with the actual import path
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/connectors"
	// We are in package main, in the ingestionservice directory.
	// Types from server.go (which is package ingestionservice) need to be
	// referenced as if they are in a different package if server.go is not also package main.
	// For this to work cleanly, server.go should be package ingestionservice,
	// and we'd need to import our own package if main was truly outside.
	// A common pattern is for main.go to be in the root of the service directory
	// and other .go files in that directory share its package or are sub-packages.

	// Let's assume 'server.go' defines types in 'package ingestionservice'
	// and 'main.go' is 'package main' in the same directory.
	// To use types from 'server.go', we'd typically make 'server.go' part of 'package main'
	// OR import 'ingestionservice' if 'main.go' is in a different directory.

	// For this example, we'll assume server.go's types (Config, Server, CoreService)
	// are accessible as if they were in this package. This implies either:
	// 1. server.go is also `package main` (less common for library-style code).
	// 2. Or, more likely, you'd have a structure where `main.go` imports `ingestionservice`.
	// Let's proceed assuming you'll adjust imports based on your actual package structure.
	// For now, I will use "is." as a placeholder for the ingestionservice package alias.
	// You would replace "is." with your actual import alias or remove it if main is in the same package.
	is "github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/ingestionservice" // Placeholder for actual import
)

// loadAppConfig loads the IngestionService server wrapper configuration.
// This function would ideally be part of the 'ingestionservice' package as is.LoadConfig().
func loadAppConfig() (*is.Config, error) { // Changed to return *is.Config
	httpPortStr := os.Getenv("HTTP_PORT")
	if httpPortStr == "" {
		httpPortStr = "8080"
	}
	httpPort, err := strconv.Atoi(httpPortStr)
	if err != nil {
		return nil, fmt.Errorf("invalid HTTP_PORT value: %s - %w", httpPortStr, err)
	}

	shutdownTimeoutStr := os.Getenv("SHUTDOWN_TIMEOUT_SECONDS")
	if shutdownTimeoutStr == "" {
		shutdownTimeoutStr = "30"
	}
	shutdownTimeoutSec, err := strconv.Atoi(shutdownTimeoutStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SHUTDOWN_TIMEOUT_SECONDS value: %s - %w", shutdownTimeoutStr, err)
	}

	cfg := &is.Config{ // Changed to is.Config
		ServiceName:                 os.Getenv("INGESTION_SERVICE_NAME"),
		Environment:                 os.Getenv("INGESTION_ENVIRONMENT"),
		ServiceManagerAPIURL:        os.Getenv("SERVICE_MANAGER_API_URL"),
		HTTPPort:                    httpPort,
		ShutdownTimeout:             time.Duration(shutdownTimeoutSec) * time.Second,
		ExpectedEnrichedTopicID:     os.Getenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES"),
		ExpectedUnidentifiedTopicID: os.Getenv("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES"),
	}

	// Validation for required fields
	if cfg.ServiceName == "" {
		return nil, fmt.Errorf("INGESTION_SERVICE_NAME environment variable is required")
	}
	if cfg.Environment == "" {
		return nil, fmt.Errorf("INGESTION_ENVIRONMENT environment variable is required")
	}
	if cfg.ServiceManagerAPIURL == "" {
		return nil, fmt.Errorf("SERVICE_MANAGER_API_URL environment variable is required")
	}
	if cfg.ExpectedEnrichedTopicID == "" {
		return nil, fmt.Errorf("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES environment variable for expected topic ID is required")
	}
	if cfg.ExpectedUnidentifiedTopicID == "" {
		return nil, fmt.Errorf("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES environment variable for expected topic ID is required")
	}
	// Add check for HTTPPort being non-zero if it's critical.
	if cfg.HTTPPort <= 0 {
		log.Warn().Int("port", cfg.HTTPPort).Msg("HTTP_PORT is zero or negative, using default 8080 if not overridden by server logic")
		// Or return an error: return nil, fmt.Errorf("HTTP_PORT must be a positive integer")
	}

	return cfg, nil
}

func main() {
	logLevelStr := os.Getenv("LOG_LEVEL")
	if logLevelStr == "" {
		logLevelStr = "info"
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	level, err := zerolog.ParseLevel(logLevelStr)
	if err != nil {
		log.Warn().Str("level_str", logLevelStr).Msg("Invalid LOG_LEVEL, defaulting to info")
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(level)
	}

	log.Info().Msg("Starting Ingestion Service application (main.go)...")

	appCfg, err := loadAppConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load application configuration")
	}
	log.Info().Interface("app_config", appCfg).Msg("Application configuration loaded")

	ctx := context.Background()
	coreDependencies, err := connectors.InitializeAndGetCoreService(ctx, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize core service dependencies")
	}
	log.Info().Msg("Core service dependencies initialized successfully.")

	defer func() {
		log.Info().Msg("Cleaning up core dependencies in main...")
		if coreDependencies.EnrichedPublisher != nil {
			coreDependencies.EnrichedPublisher.Stop()
		}
		if coreDependencies.UnidentifiedPublisher != nil {
			coreDependencies.UnidentifiedPublisher.Stop()
		}
		if coreDependencies.MetadataFetcher != nil {
			if errClose := coreDependencies.MetadataFetcher.Close(); errClose != nil {
				log.Error().Err(errClose).Msg("Error closing metadata fetcher")
			}
		}
		log.Info().Msg("Core dependencies cleanup finished from main.")
	}()

	// Assuming connectors.IngestionService implements is.CoreService interface
	// defined in your ingestionservice package (server.go)
	server, err := is.NewServer(appCfg, log.Logger, coreDependencies.Service)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create IngestionService server")
	}

	if err := server.Run(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("IngestionService server run failed")
	}

	log.Info().Msg("IngestionService application shut down gracefully.")
}
