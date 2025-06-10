package main

import (
	"bigquery/bqinit"
	"context"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	// Import the generic pipeline library
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
)

func main() {
	// --- 1. Load Configuration ---
	cfg, err := bqinit.LoadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// --- 2. Set up Logger ---
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
		log.Warn().Str("log_level", cfg.LogLevel).Msg("Invalid log level, defaulting to 'info'")
	}
	zerolog.SetGlobalLevel(level)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: zerolog.TimeFieldFormat})
	log.Info().Msg("Logger configured.")

	// --- 3. Build Pipeline Components ---
	ctx := context.Background()

	// Create a BigQuery client (reused by the inserter).
	// The bqstore library provides helpers, but you can also create clients manually.
	bqClient, err := bqstore.NewProductionBigQueryClient(ctx, &bqstore.BigQueryInserterConfig{
		ProjectID:       cfg.ProjectID,
		CredentialsFile: cfg.BigQuery.CredentialsFile,
	}, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create BigQuery client")
	}
	defer bqClient.Close()

	// Create the Pub/Sub consumer.
	consumerCfg := &bqstore.GooglePubSubConsumerConfig{
		ProjectID:       cfg.ProjectID,
		SubscriptionID:  cfg.Consumer.SubscriptionID,
		CredentialsFile: cfg.Consumer.CredentialsFile,
	}
	consumer, err := bqstore.NewGooglePubSubConsumer(ctx, consumerCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub consumer")
	}

	// Create the generic BigQuery inserter, specifying our concrete type.
	bqInserterCfg := &bqstore.BigQueryInserterConfig{
		ProjectID: cfg.ProjectID,
		DatasetID: cfg.BigQuery.DatasetID,
		TableID:   cfg.BigQuery.TableID,
	}
	bigQueryInserter, err := bqstore.NewBigQueryInserter[types.GardenMonitorPayload](ctx, bqClient, bqInserterCfg, log.Logger)
	if err != nil {
		// Attempt to provide more specific error info if table creation fails.
		if _, ok := err.(*bigquery.Error); ok {
			log.Fatal().Err(err).Msg("A BigQuery API error occurred. Check permissions and if the dataset exists.")
		}
		log.Fatal().Err(err).Msg("Failed to create BigQuery inserter")
	}

	// Create the generic batch inserter.
	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    cfg.Pipeline.BatchSize,
		FlushTimeout: cfg.Pipeline.FlushTimeout,
	}
	batchInserter := bqstore.NewBatchInserter[types.GardenMonitorPayload](batcherCfg, bigQueryInserter, log.Logger)

	// Create the generic processing service, providing our specific decoder.
	serviceCfg := &bqstore.ServiceConfig{
		NumProcessingWorkers: cfg.Pipeline.NumWorkers,
	}
	processingService, err := bqstore.NewProcessingService[types.GardenMonitorPayload](
		serviceCfg,
		consumer,
		batchInserter,
		types.GardenMonitorDecoder, // Our specific decoder function
		log.Logger,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create processing service")
	}

	// --- 4. Create and Run the Server ---
	server := bqinit.NewServer(cfg, processingService, log.Logger)

	// Set up graceful shutdown.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Run the server in a separate goroutine.
	go func() {
		if err := server.Start(); err != nil {
			log.Fatal().Err(err).Msg("Server failed to start")
		}
	}()

	// Block until a shutdown signal is received.
	<-stop
	log.Warn().Msg("Shutdown signal received")

	// Perform graceful shutdown.
	server.Shutdown()
	log.Info().Msg("Server shut down gracefully.")
}
