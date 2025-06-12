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

	log.Info().Interface("cfg", cfg).Msg("Configuration")

	// --- 3. Build BatchProcessing Components ---
	ctx := context.Background()

	bqClient, err := bqstore.NewProductionBigQueryClient(ctx, &bqstore.BigQueryInserterConfig{
		ProjectID:       cfg.ProjectID,
		CredentialsFile: cfg.BigQuery.CredentialsFile,
	}, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create BigQuery client")
	}
	defer bqClient.Close()

	consumerCfg := &bqstore.GooglePubSubConsumerConfig{
		ProjectID:       cfg.ProjectID,
		SubscriptionID:  cfg.Consumer.SubscriptionID,
		CredentialsFile: cfg.Consumer.CredentialsFile,
	}

	log.Info().Interface("consumerCfg", consumerCfg).Msg("Created Pubsub consumer")
	consumer, err := bqstore.NewGooglePubSubConsumer(ctx, consumerCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub consumer")
	}

	bqInserterCfg := &bqstore.BigQueryInserterConfig{
		ProjectID: cfg.ProjectID,
		DatasetID: cfg.BigQuery.DatasetID,
		TableID:   cfg.BigQuery.TableID,
	}
	bigQueryInserter, err := bqstore.NewBigQueryInserter[types.GardenMonitorPayload](ctx, bqClient, bqInserterCfg, log.Logger)
	if err != nil {
		if _, ok := err.(*bigquery.Error); ok {
			log.Fatal().Err(err).Msg("A BigQuery API error occurred. Check permissions and if the dataset exists.")
		}
		log.Fatal().Err(err).Msg("Failed to create BigQuery inserter")
	}

	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    cfg.BatchProcessing.BatchSize,
		FlushTimeout: cfg.BatchProcessing.FlushTimeout,
	}
	batchInserter := bqstore.NewBatchInserter[types.GardenMonitorPayload](batcherCfg, bigQueryInserter, log.Logger)

	serviceCfg := &bqstore.ServiceConfig{
		NumProcessingWorkers: cfg.BatchProcessing.NumWorkers,
	}
	// Corrected function name from NewBatchingService to NewProcessingService
	processingService, err := bqstore.NewBatchingService[types.GardenMonitorPayload](
		serviceCfg,
		consumer,
		batchInserter,
		types.GardenMonitorDecoder,
		log.Logger,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create processing service")
	}

	// --- 4. Create and Run the Server ---
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
