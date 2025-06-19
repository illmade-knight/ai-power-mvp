package icinit

import (
	"context"
	"fmt"
	"github.com/illmade-knight/go-iot/pkg/icestore"
	"net/http"
	"time"

	// Import the new generic consumers package
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/rs/zerolog"
)

// --- Application Server ---

// Server holds all the components of our microservice.
type Server struct {
	logger zerolog.Logger
	config *IceServiceConfig
	// The server now holds a pointer to the generic ProcessingService.
	batchProcessing *consumers.ProcessingService[icestore.ArchivalData]
	httpServer      *http.Server
}

// NewServer creates and configures a new Server instance.
// Its signature is updated to accept the generic service type.
func NewServer(cfg *IceServiceConfig, b *consumers.ProcessingService[icestore.ArchivalData], logger zerolog.Logger) *Server {
	return &Server{
		logger:          logger,
		config:          cfg,
		batchProcessing: b,
	}
}

func (s *Server) GetHTTPPort() string {
	return s.config.HTTPPort
}

// GetHandler returns the server's HTTP handler.
// This method is added to make the server testable with the standard httptest package,
// as it exposes the routing logic without blocking on ListenAndServe.
func (s *Server) GetHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", s.healthzHandler)
	return mux
}

// Start runs the main application logic.
func (s *Server) Start() error {
	s.logger.Info().Msg("Starting server...")

	// The Start() method on the service remains the same.
	if err := s.batchProcessing.Start(); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")

	// Set up and start the HTTP server for health checks.
	// The handler is now created by the GetHandler method for consistency.
	s.httpServer = &http.Server{
		Addr:    s.config.HTTPPort,
		Handler: s.GetHandler(),
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

	// The Stop() method on the service remains the same.
	s.batchProcessing.Stop()
	s.logger.Info().Msg("Data processing service stopped.")

	// Shut down the HTTP server.
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
	w.Write([]byte("OK"))
}
