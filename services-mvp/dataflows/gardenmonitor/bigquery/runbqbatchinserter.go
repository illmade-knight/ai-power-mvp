package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"bigquery/bqinit"

	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// --- 1. Load Configuration (Unchanged) ---
	cfg, err := bqinit.LoadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// --- 2. Set up Logger (Unchanged) ---
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
		log.Warn().Str("log_level", cfg.LogLevel).Msg("Invalid log level, defaulting to 'info'")
	}
	zerolog.SetGlobalLevel(level)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: zerolog.TimeFieldFormat})
	log.Info().Msg("Logger configured.")
	log.Info().Interface("cfg", cfg).Msg("Configuration")

	// --- 3. Build Service Components (Updated) ---
	ctx := context.Background()

	// Create the BigQuery client.
	// NOTE: This assumes a `NewProductionBigQueryClient` function exists in your bqstore package.
	bqClient, err := bqstore.NewProductionBigQueryClient(ctx, &bqstore.BigQueryInserterConfig{
		ProjectID:       cfg.ProjectID,
		CredentialsFile: cfg.BigQuery.CredentialsFile,
	}, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create BigQuery client")
	}
	defer bqClient.Close()

	// Create the Pub/Sub consumer using the shared consumers package.
	consumerCfg := &consumers.GooglePubSubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := consumers.NewGooglePubSubConsumer(ctx, consumerCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub consumer")
	}

	// Create the bqstore-specific components.
	bqInserterCfg := &bqstore.BigQueryInserterConfig{
		ProjectID: cfg.ProjectID,
		DatasetID: cfg.BigQuery.DatasetID,
		TableID:   cfg.BigQuery.TableID,
	}
	bigQueryInserter, err := bqstore.NewBigQueryInserter[types.GardenMonitorPayload](ctx, bqClient, bqInserterCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create BigQuery inserter")
	}

	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    cfg.BatchProcessing.BatchSize,
		FlushTimeout: cfg.BatchProcessing.FlushTimeout,
	}
	batchInserter := bqstore.NewBatchInserter[types.GardenMonitorPayload](batcherCfg, bigQueryInserter, log.Logger)

	// Get the application-specific decoder.
	decoder := types.NewGardenMonitorDecoder()

	// Assemble the final service using the new, clean constructor.
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorPayload](
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batchInserter,
		decoder,
		log.Logger,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create processing service")
	}

	// --- 4. Create and Run the Server (Unchanged) ---
	server := bqinit.NewServer(cfg, processingService, log.Logger)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Start(); err != nil {
			log.Fatal().Err(err).Msg("Server failed to start")
		}
	}()

	<-stop
	log.Warn().Msg("Shutdown signal received")
	server.Shutdown()
	log.Info().Msg("Server shut down gracefully.")
}
