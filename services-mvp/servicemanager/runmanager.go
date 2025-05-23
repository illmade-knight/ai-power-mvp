package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log" // Using global logger for convenience

	// Replace "your_module_path/servicemanager" with your actual module path
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/apiserver"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/initialization"
)

func main() {
	// Setup structured logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Define command-line flags
	configPath := flag.String("config", "", "Path to the master YAML configuration file (required)")
	port := flag.Int("port", 8080, "Port for the configuration access server")
	flag.Parse()

	if *configPath == "" {
		log.Fatal().Msg("-config flag is required")
	}

	// Load and validate the main configuration file
	// This uses LoadAndValidateConfig from your initialization package
	cfg, err := initialization.LoadAndValidateConfig(*configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load or validate master configuration")
	}
	log.Info().Str("path", *configPath).Msg("Master configuration loaded successfully")

	// Create the ConfigServer instance
	// It takes the loaded config and a logger
	configAPIServer := apiserver.NewConfigServer(cfg, log.Logger) // Pass the global logger or a specific one

	// Setup HTTP routes
	// The ConfigServer provides the handler for the /config endpoint
	http.HandleFunc("/config", configAPIServer.GetServiceConfigHandler)
	// You could add other handlers here, e.g., /healthz
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	// Start the HTTP server
	serverAddr := fmt.Sprintf(":%d", *port)
	log.Info().Str("address", serverAddr).Msg("Starting Service Manager API server")

	// Configure the server with timeouts
	server := &http.Server{
		Addr:              serverAddr,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("Failed to start HTTP server")
	}
	log.Info().Msg("Service Manager API server shut down gracefully")
}
