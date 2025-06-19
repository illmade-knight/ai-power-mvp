package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/mqtttopubsub/converter"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	// --- Configuration Flags ---
	// MQTT Config (reusing from your converter.MQTTClientConfig loading)
	// The LoadMQTTClientConfigFromEnv in your package already loads these.
	// We just need to ensure those ENV VARS are set or provide flags if we want to override.
	// For simplicity, relying on ENV for MQTT config as per your converter.LoadMQTTClientConfigFromEnv

	// PubSub Config
	gcpProjectID := flag.String("gcp-project-id", getEnv("GCP_PROJECT_ID", ""), "Google Cloud Project ID (required)")
	// The target Pub/Sub topic ID for the dumper
	targetPubSubTopicIDEnvVar := "DUMPER_PUBSUB_TOPIC_ID" // ENV var name for the dumper's target topic
	pubsubTopicID := flag.String("pubsub-topic-id", getEnv(targetPubSubTopicIDEnvVar, ""), "Google Cloud Pub/Sub Topic ID to publish to (required)")
	//pubsubEmulatorHost := flag.String("pubsub-emulator-host", getEnv("PUBSUB_EMULATOR_HOST", ""), "Pub/Sub emulator host (e.g., localhost:8085)")

	// Service Config
	inputChanCapacity := flag.Int("input-chan-capacity", 100, "Capacity of the internal raw message channel")
	numWorkers := flag.Int("num-workers", 5, "Number of concurrent message processing workers")
	logLevel := flag.String("log-level", getEnv("LOG_LEVEL", "info"), "Log level (debug, info, warn, error, fatal, panic)")
	flag.Parse()

	// --- Logger Setup ---
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	parsedLevel, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		log.Warn().Str("log_level_input", *logLevel).Msg("Invalid log level provided, defaulting to 'info'")
		parsedLevel = zerolog.InfoLevel
	}
	logger := zerolog.New(os.Stderr).Level(parsedLevel).With().Timestamp().Str("service", "mqtt-dumper").Logger()
	log.Logger = logger // Set global logger

	// --- Load Configurations ---
	mqttCfg, err := converter.LoadMQTTClientConfigFromEnv()
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to load MQTT client configuration from environment")
	}
	// Override with flags if provided (example, you might not need this if ENV is king)
	// if *mqttBrokerURLFromFlag != "" { mqttCfg.BrokerURL = *mqttBrokerURLFromFlag }
	// etc. for other MQTT flags if you add them.

	pubsubCfg := &converter.GooglePubSubPublisherConfig{
		ProjectID: *gcpProjectID,
		TopicID:   *pubsubTopicID,
		// CredentialsFile could also be a flag if needed
		CredentialsFile: os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"),
	}
	if pubsubCfg.ProjectID == "" {
		logger.Fatal().Msg("GCP Project ID is required (-gcp-project-id or GCP_PROJECT_ID env)")
	}
	if pubsubCfg.TopicID == "" {
		logger.Fatal().Msg("Pub/Sub Topic ID is required (-pubsub-topic-id or DUMPER_PUBSUB_TOPIC_ID env)")
	}

	serviceCfg := converter.IngestionServiceConfig{
		InputChanCapacity:    *inputChanCapacity,
		NumProcessingWorkers: *numWorkers,
	}

	// --- Create Dependencies ---
	ctxApp, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	publisher, err := converter.NewGooglePubSubPublisher(ctxApp, pubsubCfg, logger.With().Str("component", "pubsub-publisher").Logger())
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create GooglePubSubPublisher")
	}

	// --- Create and Start Service ---
	dumperService := converter.NewIngestionService(
		publisher,
		logger.With().Str("component", "dumper-core").Logger(),
		serviceCfg,
		mqttCfg,
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var serviceErr error
	go func() {
		logger.Info().Msg("Attempting to start Dumper Service...")
		// The Start method of IngestionService is blocking until its context is done or an error occurs
		// Its internal context (s.cancelCtx) is derived from context.Background() and controlled by s.cancelFunc()
		// We don't pass ctxApp directly to Start() because IngestionService manages its own lifecycle
		// via its Stop() method which calls s.cancelFunc().
		// The main goroutine will wait for OS signal or its own ctxApp.Done().
		if err := dumperService.Start(); err != nil {
			// This error is from the blocking Start method if it returns an error not related to graceful shutdown.
			serviceErr = fmt.Errorf("dumper service Start() error: %w", err)
			logger.Error().Err(serviceErr).Msg("Dumper service exited with error")
		} else {
			logger.Info().Msg("Dumper service Start() exited cleanly.")
		}
		cancelApp() // Signal main to exit if service stops for any reason
	}()

	// Wait for shutdown signal or service error/completion
	select {
	case sig := <-sigChan:
		logger.Info().Str("signal", sig.String()).Msg("Received OS signal, initiating shutdown...")
	case <-ctxApp.Done(): // Triggered by service exiting (error or normal if Start wasn't blocking indefinitely)
		if serviceErr != nil {
			logger.Info().Msg("Service context done due to internal error, shutting down.")
		} else {
			logger.Info().Msg("Service context done, initiating shutdown (likely normal completion or explicit cancel).")
		}
	}

	logger.Info().Msg("Shutting down dumper service...")
	dumperService.Stop() // Call graceful stop

	if serviceErr != nil && !errors.Is(serviceErr, context.Canceled) {
		logger.Error().Err(serviceErr).Msg("Service run failed")
		os.Exit(1) // Exit with error code if service had an unhandled issue
	}
	logger.Info().Msg("MQTT to Pub/Sub Dumper Service shut down gracefully.")
}
