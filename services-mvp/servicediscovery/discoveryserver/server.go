package discoveryserver

import (
	"encoding/json"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"net/http"

	"github.com/rs/zerolog"
)

// Server serves configuration information to microservices at runtime.
type Server struct {
	servicesDef servicemanager.ServicesDefinition // Use the interface
	logger      zerolog.Logger
}

// NewServer creates a new API server.
func NewServer(def servicemanager.ServicesDefinition, logger zerolog.Logger) *Server {
	return &Server{
		servicesDef: def,
		logger:      logger.With().Str("component", "APIServer").Logger(),
	}
}

// GetServiceConfigHandler is an HTTP handler that calls the core logic
// and wraps the result in an HTTP response.
func (s *Server) GetServiceConfigHandler(w http.ResponseWriter, r *http.Request) {
	serviceName := r.URL.Query().Get("serviceName")
	environment := r.URL.Query().Get("env")

	if serviceName == "" {
		http.Error(w, "query parameter 'serviceName' is required", http.StatusBadRequest)
		return
	}
	if environment == "" {
		http.Error(w, "query parameter 'env' is required", http.StatusBadRequest)
		return
	}

	// Call the reusable library function to get the configuration.
	response, err := servicemanager.GetConfigurationForService(s.servicesDef, serviceName, environment)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to get service configuration")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Error().Err(err).Str("service", serviceName).Str("environment", environment).Msg("Failed to encode and write response")
	}
}
