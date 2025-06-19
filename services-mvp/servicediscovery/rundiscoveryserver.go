package main

import (
	"flag"
	"github.com/go-chi/chi/v5"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"net/http"
	"os"
	"servicediscovery/discoveryserver"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// --- Flag Definitions ---
	// Define command-line flags to make the server configurable.
	configFile := flag.String("config", "config.yaml", "Path to the services definition YAML file.")
	listenAddr := flag.String("listen-addr", ":8080", "The HTTP address and port to listen on.")
	flag.Parse()

	// --- Logger Setup ---
	// Use a structured logger for clear, machine-readable logs.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- Services Definition Initialization ---
	// Create the services definition repository from the YAML file.
	// This is the single source of truth for the API server.
	servicesDef, err := servicemanager.NewYAMLServicesDefinition(*configFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load services definition")
	}
	log.Info().Str("path", *configFile).Msg("Services definition loaded successfully")

	// --- API Server Initialization ---
	// Create the main API server, injecting the services definition and logger.
	apiServer := discoveryserver.NewServer(servicesDef, log.Logger)

	// --- HTTP Router Setup ---
	// Use a capable router like chi for features like middleware.
	router := chi.NewRouter()

	// Add middleware for logging, panic recovery, and request IDs.
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(60 * time.Second))

	// Register the handler for the main API endpoint.
	router.Get("/config", apiServer.GetServiceConfigHandler)

	// Add a simple health check endpoint.
	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	log.Info().Str("address", *listenAddr).Msg("Starting API server")

	// --- Start Server ---
	// Start the HTTP server and listen for requests.
	if err := http.ListenAndServe(*listenAddr, router); err != nil {
		log.Fatal().Err(err).Msg("Failed to start API server")
	}
}
