package main

import (
	"bigquery/bqinit"
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
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
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

	gcsAdapter := icestore.NewGCSClientAdapter(gcsClient)
	uploader, err := icestore.NewGCSBatchUploader(gcsAdapter, icestore.GCSBatchUploaderConfig{
		BucketName:   cfg.IceStore.BucketName,
		ObjectPrefix: "archived-data",
	}, iceLogger)

	// Create the Pub/Sub consumer using the shared consumers package.
	consumerCfg := &consumers.GooglePubSubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := consumers.NewGooglePubSubConsumer(ctx, consumerCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub consumer")
	}

	// Get the application-specific decoder.
	decoder := types.NewGardenMonitorDecoder()

	batcher := icestore.NewBatcher[types.GardenMonitorPayload](&icestore.BatcherConfig{
		BatchSize:    3,
		FlushTimeout: 5 * time.Second,
	}, uploader, iceLogger)

	// Assemble the final service using the new, clean constructor.
	processingService, err := icestore.NewIceStorageService[types.GardenMonitorPayload](
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batcher,
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
