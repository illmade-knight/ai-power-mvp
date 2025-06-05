package main

import (
	"context"
	"flag"
	"load_test/apps/verifier/loadtestverifier"
	"os"
	"time"

	// Adjust this import path based on your Go module structure.
	// For example, if your module is "mycompany.com/loadtester" and the
	// loadtestverifier package is in "mycompany.com/loadtester/internal/loadtestverifier",
	// the import path would be "mycompany.com/loadtester/internal/loadtestverifier".
	// For this example, we'll assume a placeholder path.
	//"/loadtestverifier" // <<< --- IMPORTANT: Replace with your actual module path

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log" // Using global logger for main, verifier uses its own instance
)

func main() {
	// --- Command-line flags ---
	projectID := flag.String("project", "", "GCP Project MessageID (required)")
	enrichedTopic := flag.String("enriched-topic", "", "Short MessageID of the enriched messages Pub/Sub topic (required)")
	unidentifiedTopic := flag.String("unidentified-topic", "", "Short MessageID of the unidentified messages Pub/Sub topic (required)")
	durationStr := flag.String("duration", "1m", "Duration for the verifier to run (e.g., 30s, 5m, 1h)")
	outputFile := flag.String("output", "verifier_results.json", "Output file for results")
	testRunID := flag.String("run-id", uuid.NewString(), "Unique MessageID for this test run")
	machineInfo := flag.String("machine-info", "", "Information about the machine running the test (optional)")
	envName := flag.String("env-name", "", "Name of the test environment (optional)")
	logLevelStr := flag.String("log-level", "info", "Log level (trace, debug, info, warn, error, fatal, panic)")
	emulatorHost := flag.String("emulator-host", "", "Pub/Sub emulator host (e.g., localhost:8085). If set, credentials are not used.")

	flag.Parse()

	// --- Logger Setup ---
	// Configure the global logger for the main application.
	// The Verifier instance will get its own logger based on this or its config.
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	baseLogger := zerolog.New(consoleWriter).With().Timestamp().Logger() // Create a base logger instance

	parsedLevel, err := zerolog.ParseLevel(*logLevelStr)
	if err != nil {
		baseLogger.Warn().Str("level_str", *logLevelStr).Msg("Invalid log level from flag, defaulting to info for base logger")
		baseLogger = baseLogger.Level(zerolog.InfoLevel)
	} else {
		baseLogger = baseLogger.Level(parsedLevel)
	}
	log.Logger = baseLogger // Set the global logger used by log.Info(), log.Fatal(), etc.

	// --- Validate Flags & Create Config for Verifier ---
	if *projectID == "" {
		log.Fatal().Msg("-project flag is required")
	}
	if *enrichedTopic == "" {
		log.Fatal().Msg("-enriched-topic flag is required")
	}
	if *unidentifiedTopic == "" {
		log.Fatal().Msg("-unidentified-topic flag is required")
	}

	duration, err := time.ParseDuration(*durationStr)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid -duration format")
	}
	if duration <= 0 {
		log.Fatal().Msg("-duration must be positive")
	}

	cfg := loadtestverifier.VerifierConfig{
		ProjectID:           *projectID,
		EnrichedTopicID:     *enrichedTopic,
		UnidentifiedTopicID: *unidentifiedTopic,
		TestDuration:        duration,
		OutputFile:          *outputFile,
		TestRunID:           *testRunID,
		MachineInfo:         *machineInfo,
		EnvironmentName:     *envName,
		EmulatorHost:        *emulatorHost, // Pass emulator host from flag
		LogLevel:            *logLevelStr,  // Pass log level string for verifier's internal logger setup
		ClientOpts:          nil,           // ClientOpts can be further configured if needed, e.g., for credentials
	}

	// --- Create and Run Verifier ---
	// Pass the baseLogger to NewVerifier. The verifier will create its own
	// instance logger based on this and its configuration.
	verifier, err := loadtestverifier.NewVerifier(cfg, baseLogger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create verifier")
	}

	// The verifier's Run method manages its own main context based on TestDuration
	// and OS signal handling. We pass context.Background() as the parent.
	// The verifier.Run method will also handle saving results to the output file if specified.
	results, err := verifier.Run(context.Background())
	if err != nil {
		// Verifier.Run itself logs errors internally. Main can log a summary error.
		log.Error().Err(err).Msg("Verifier run encountered an error during execution")
		// Depending on the severity or type of error, you might os.Exit(1) here.
		// For now, we'll let it proceed to log whatever results were gathered.
	}

	if results != nil {
		log.Info().
			Int("total_verified", results.TotalMessagesVerified).
			Int("enriched", results.EnrichedMessages).
			Int("unidentified", results.UnidentifiedMessages).
			Str("actual_duration", results.ActualDuration).
			Msg("Verification complete. Results summary above.")
		// Results are saved by verifier.Run() if OutputFile is set in config.
	} else {
		log.Warn().Msg("Verifier run did not produce results (possibly due to an early or critical error).")
	}

	log.Info().Msg("Verifier application finished.")
}
