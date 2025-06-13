package main

import (
	"bigquery/icinit"
	"cloud.google.com/go/storage"
	"context"
	"github.com/illmade-knight/ai-power-mpv/pkg/icestore"
	"google.golang.org/api/option"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// --- 1. Load Configuration (Unchanged) ---
	cfg, err := icinit.LoadConfig()
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

	iceLogger := log.Logger.With().Str("service", "ice-processor").Logger()
	// Create the Icestore client.
	// NOTE: This assumes a `NewProductionBigQueryClient` function exists in your bqstore package.
	gcsClient, err := storage.NewClient(ctx, option.WithoutAuthentication(), option.WithEndpoint(os.Getenv("STORAGE_EMULATOR_HOST")))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create storage client")
	}
	// Create the bucket in the GCS emulator
	err = gcsClient.Bucket(cfg.IceStore.BucketName).Create(ctx, cfg.ProjectID, nil)

	// Create the Pub/Sub consumer using the shared consumers package.
	consumerCfg := &consumers.GooglePubSubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := consumers.NewGooglePubSubConsumer(ctx, consumerCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub consumer")
	}

	// REFACTORED: Use the new single constructor for the batch processor.
	// The generic type is now icestore.ArchivalData.
	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{
			BatchSize:    3,
			FlushTimeout: 5 * time.Second,
		},
		icestore.GCSBatchUploaderConfig{
			BucketName:   cfg.IceStore.BucketName,
			ObjectPrefix: "archived-data",
		},
		iceLogger,
	)

	// Assemble the final service using the new, clean constructor.
	processingService, err := icestore.NewIceStorageService(
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batcher,
		icestore.ArchivalTransformer,
		log.Logger,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create processing service")
	}

	// --- 4. Create and Run the Server (Unchanged) ---
	server := icinit.NewServer(cfg, processingService, iceLogger)

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
