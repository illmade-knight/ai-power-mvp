package bqinit

import (
	"context"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	// Import the generic batchProcessing library
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
)

// --- Application Server ---

// Server holds all the components of our microservice.
type Server struct {
	logger          zerolog.Logger
	config          *Config
	batchProcessing *bqstore.ProcessingService[types.GardenMonitorPayload]
	httpServer      *http.Server
}

// NewServer creates and configures a new Server instance.
func NewServer(cfg *Config, b *bqstore.ProcessingService[types.GardenMonitorPayload], logger zerolog.Logger) *Server {
	return &Server{
		logger:          logger,
		config:          cfg,
		batchProcessing: b,
	}
}

func (s *Server) GetHTTPPort() string {
	return s.config.HTTPPort
}

// Start runs the main application logic.
func (s *Server) Start() error {
	s.logger.Info().Msg("Starting server...")

	// Start the data processing batchProcessing.
	if err := s.batchProcessing.Start(); err != nil {
		return fmt.Errorf("failed to start batchProcessing: %w", err)
	}
	s.logger.Info().Msg("Data batchProcessing started.")

	// Set up and start the HTTP server for health checks.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", s.healthzHandler)
	s.httpServer = &http.Server{
		Addr:    s.config.HTTPPort,
		Handler: mux,
	}

	s.logger.Info().Str("address", s.config.HTTPPort).Msg("Starting health check server.")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("health check server failed: %w", err)
	}

	return nil
}

// Shutdown gracefully stops all components of the service.
func (s *Server) Shutdown() {
	s.logger.Info().Msg("Shutting down server...")

	// 1. Stop the data batchProcessing first to ensure all messages are flushed.
	s.batchProcessing.Stop()
	s.logger.Info().Msg("Data batchProcessing stopped.")

	// 2. Shut down the HTTP server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Error during health check server shutdown.")
	} else {
		s.logger.Info().Msg("Health check server stopped.")
	}
}

// healthzHandler responds to health check probes.
func (s *Server) healthzHandler(w http.ResponseWriter, r *http.Request) {
	// A simple health check that returns 200 OK.
	// This can be expanded to check dependencies (e.g., Pub/Sub connection).
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
