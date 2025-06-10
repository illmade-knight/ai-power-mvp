package mqinit

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	"github.com/illmade-knight/ai-power-mpv/pkg/mqttconverter"
)

// --- Application Server ---

// Server holds all the components of our MQTT ingestion microservice.
type Server struct {
	logger     zerolog.Logger
	config     *Config
	service    *mqttconverter.IngestionService
	httpServer *http.Server
}

// NewServer creates and configures a new Server instance.
func NewServer(cfg *Config, service *mqttconverter.IngestionService, logger zerolog.Logger) *Server {
	return &Server{
		logger:  logger,
		config:  cfg,
		service: service,
	}
}

// Start runs the main application logic.
func (s *Server) Start() error {
	s.logger.Info().Msg("Starting MQTT ingestion server...")

	// Start the ingestion service. This will connect to MQTT.
	if err := s.service.Start(); err != nil {
		return fmt.Errorf("failed to start ingestion service: %w", err)
	}
	s.logger.Info().Msg("MQTT ingestion service started.")

	// Set up and start the HTTP server for health checks.
	mux := http.NewServeMux()
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
	s.logger.Info().Msg("Shutting down MQTT ingestion server...")

	// 1. Stop the ingestion service. This disconnects from MQTT and flushes messages.
	s.service.Stop()
	s.logger.Info().Msg("Ingestion service stopped.")

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
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}
