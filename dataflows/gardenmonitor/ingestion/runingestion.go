package main

import (
	"context"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/ingestion/mqinit"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
)

func main() {
	// --- 1. Load Configuration ---
	cfg, err := mqinit.LoadConfig()
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

	// --- 3. Build Service Components ---
	ctx := context.Background()

	// Create the Google Pub/Sub publisher.
	publisherCfg := &mqttconverter.GooglePubSubPublisherConfig{
		ProjectID:       cfg.ProjectID,
		TopicID:         cfg.Publisher.TopicID,
		CredentialsFile: cfg.Publisher.CredentialsFile,
	}
	publisher, err := mqttconverter.NewGooglePubSubPublisher(ctx, publisherCfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Google Pub/Sub publisher")
	}

	// For a production bridge, we don't need a special attribute extractor.
	var extractor mqttconverter.AttributeExtractor = nil

	// Create the ingestion service, passing the MQTT config by value.
	ingestionService := mqttconverter.NewIngestionService(
		publisher,
		extractor,
		log.Logger,
		cfg.Service,
		cfg.MQTT, // Pass by value to prevent pointer issues
	)

	// --- 4. Create and Run the Server ---
	server := mqinit.NewServer(cfg, ingestionService, log.Logger)

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
