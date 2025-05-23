package ingestionservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io" // Added for io.ReadAll
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	// Assuming your IngestionService and related types are now in a 'connectors' package
	// within your 'ingestion' directory.
	// Replace "your_module_path/ingestion/connectors" with the actual import path.
	//"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/connectors"
)

// CoreService defines the interface for the core ingestion logic
// that the Server wrapper interacts with.
type CoreService interface {
	Start() error
	Stop() // Assuming Stop does not return an error based on previous discussion
}

// Config holds the configuration for the IngestionService server wrapper.
type Config struct {
	ServiceName                 string        `json:"serviceName"` // e.g., "ingestion-service"
	Environment                 string        `json:"environment"` // e.g., "test", "prod"
	ServiceManagerAPIURL        string        `json:"serviceManagerApiUrl"`
	HTTPPort                    int           `json:"httpPort"`
	ShutdownTimeout             time.Duration `json:"shutdownTimeout"`
	ExpectedEnrichedTopicID     string        `json:"expectedEnrichedTopicId"`     // Short ID, e.g., "enriched-device-messages"
	ExpectedUnidentifiedTopicID string        `json:"expectedUnidentifiedTopicId"` // Short ID, e.g., "unidentified-device-messages"
}

// ServiceManagerResourceInfo defines the structure for individual resources
// as expected from the ServiceManager API. (Mirrors apiserver.ServiceResourceInfo)
type ServiceManagerResourceInfo struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
}

// ServiceManagerSubscriptionInfo defines subscription info from ServiceManager.
// (Mirrors apiserver.ServiceSubscriptionInfo)
type ServiceManagerSubscriptionInfo struct {
	Name               string            `json:"name"`
	Topic              string            `json:"topic"`
	AckDeadlineSeconds int               `json:"ackDeadlineSeconds,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"`
}

// ServiceManagerConfiguration is the expected response structure from the ServiceManager API.
// (Mirrors apiserver.ServiceConfigurationResponse)
type ServiceManagerConfiguration struct {
	ServiceName               string                           `json:"serviceName"`
	Environment               string                           `json:"environment"`
	GCPProjectID              string                           `json:"gcpProjectId"`
	PublishesToTopics         []ServiceManagerResourceInfo     `json:"publishesToTopics"`
	ConsumesFromSubscriptions []ServiceManagerSubscriptionInfo `json:"consumesFromSubscriptions"`
	// Add GCSBuckets and BigQueryTables if this service needs to validate them
}

// Server represents the runnable IngestionService application.
// It wraps the core connectors.IngestionService to add HTTP capabilities,
// signal handling, and ServiceManager integration.
type Server struct {
	config               *Config
	logger               zerolog.Logger
	httpServer           *http.Server
	coreService          CoreService // Using the CoreService interface
	serviceManagerConfig *ServiceManagerConfiguration
}

// NewServer creates a new IngestionService server wrapper.
// It takes the server's own configuration, a logger, and the pre-initialized core IngestionService.
func NewServer(
	cfg *Config,
	logger zerolog.Logger,
	coreSvc CoreService, // Accepts the CoreService interface
) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("server config cannot be nil")
	}
	if cfg.ServiceName == "" {
		return nil, errors.New("server config: ServiceName is required")
	}
	if cfg.Environment == "" {
		return nil, errors.New("server config: Environment is required")
	}
	if cfg.ServiceManagerAPIURL == "" {
		return nil, errors.New("server config: ServiceManagerAPIURL is required")
	}
	if cfg.ExpectedEnrichedTopicID == "" {
		return nil, errors.New("server config: ExpectedEnrichedTopicID is required")
	}
	if cfg.ExpectedUnidentifiedTopicID == "" {
		return nil, errors.New("server config: ExpectedUnidentifiedTopicID is required")
	}
	if coreSvc == nil {
		return nil, errors.New("coreService cannot be nil")
	}

	return &Server{
		config:      cfg,
		logger:      logger.With().Str("service_wrapper", cfg.ServiceName).Str("environment", cfg.Environment).Logger(),
		coreService: coreSvc,
	}, nil
}

// Start initializes and starts the IngestionService application.
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting IngestionService application...")

	// 1. Fetch configuration from ServiceManager API
	err := s.fetchAndValidateServiceManagerConfig(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to fetch or validate configuration from ServiceManager. This may impact service behavior or validation.")
		// Depending on strictness, you might return err here to prevent startup.
	} else {
		s.logger.Info().Msg("Successfully fetched and validated configuration with ServiceManager.")
	}

	// 2. Start the core IngestionService logic
	s.logger.Info().Msg("Starting core IngestionService logic...")
	if err := s.coreService.Start(); err != nil {
		return fmt.Errorf("failed to start core IngestionService: %w", err)
	}
	s.logger.Info().Msg("Core IngestionService logic started.")

	// 3. Setup HTTP Server for health checks
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthzHandler) // Using "healthz" by convention

	s.httpServer = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.config.HTTPPort),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	go func() {
		s.logger.Info().Str("address", s.httpServer.Addr).Msg("HTTP server listening")
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Fatal().Err(err).Msg("HTTP server ListenAndServe error")
		}
		s.logger.Info().Msg("HTTP server shut down.")
	}()

	s.logger.Info().Msg("IngestionService application started successfully.")
	return nil
}

func (s *Server) fetchAndValidateServiceManagerConfig(ctx context.Context) error {
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("%s/config?serviceName=%s&env=%s", s.config.ServiceManagerAPIURL, s.config.ServiceName, s.config.Environment)

	s.logger.Info().Str("url", url).Msg("Fetching configuration from ServiceManager API")

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request to ServiceManager API: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call ServiceManager API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, readErr := io.ReadAll(resp.Body) // Corrected to io.ReadAll
		if readErr != nil {
			s.logger.Error().Err(readErr).Msg("Failed to read error response body from ServiceManager API")
			return fmt.Errorf("ServiceManager API returned non-OK status: %d (and failed to read body)", resp.StatusCode)
		}
		return fmt.Errorf("ServiceManager API returned non-OK status: %d, Body: %s", resp.StatusCode, string(bodyBytes))
	}

	var smConfig ServiceManagerConfiguration
	if err := json.NewDecoder(resp.Body).Decode(&smConfig); err != nil {
		return fmt.Errorf("failed to decode response from ServiceManager API: %w", err)
	}

	s.serviceManagerConfig = &smConfig
	s.logger.Info().Interface("config_from_servicemanager", smConfig).Msg("Received configuration from ServiceManager")

	if smConfig.GCPProjectID == "" {
		s.logger.Warn().Msg("ServiceManager response missing GCPProjectID. Cannot validate full topic names.")
	}

	// Validate Enriched Topic
	foundEnriched := false
	if smConfig.GCPProjectID != "" {
		expectedEnrichedFullTopicName := fmt.Sprintf("projects/%s/topics/%s", smConfig.GCPProjectID, s.config.ExpectedEnrichedTopicID)
		for _, topicInfo := range smConfig.PublishesToTopics {
			if topicInfo.Name == expectedEnrichedFullTopicName {
				foundEnriched = true
				s.logger.Info().Str("topic", expectedEnrichedFullTopicName).Msg("Confirmed: Service is expected to publish to ENRICHED topic by ServiceManager.")
				break
			}
		}
		if !foundEnriched {
			s.logger.Warn().Str("expected_topic_short_id", s.config.ExpectedEnrichedTopicID).Str("sm_project_id", smConfig.GCPProjectID).Msg("Warning: ServiceManager does not list this service as a producer for its configured ENRICHED output topic.")
		}
	}

	// Validate Unidentified Topic
	foundUnidentified := false
	if smConfig.GCPProjectID != "" {
		expectedUnidentifiedFullTopicName := fmt.Sprintf("projects/%s/topics/%s", smConfig.GCPProjectID, s.config.ExpectedUnidentifiedTopicID)
		for _, topicInfo := range smConfig.PublishesToTopics {
			if topicInfo.Name == expectedUnidentifiedFullTopicName {
				foundUnidentified = true
				s.logger.Info().Str("topic", expectedUnidentifiedFullTopicName).Msg("Confirmed: Service is expected to publish to UNIDENTIFIED topic by ServiceManager.")
				break
			}
		}
		if !foundUnidentified {
			s.logger.Warn().Str("expected_topic_short_id", s.config.ExpectedUnidentifiedTopicID).Str("sm_project_id", smConfig.GCPProjectID).Msg("Warning: ServiceManager does not list this service as a producer for its configured UNIDENTIFIED output topic.")
		}
	}
	return nil
}

func (s *Server) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
	s.logger.Debug().Msg("Health check successful")
}

// Stop gracefully shuts down the IngestionService application.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down IngestionService application...")
	var firstErr error

	if s.httpServer != nil {
		shutdownHttpCtx, cancelHttp := context.WithTimeout(ctx, s.config.ShutdownTimeout)
		defer cancelHttp()
		s.logger.Info().Msg("Shutting down HTTP server...")
		if err := s.httpServer.Shutdown(shutdownHttpCtx); err != nil {
			s.logger.Error().Err(err).Msg("HTTP server shutdown error")
			if firstErr == nil {
				firstErr = err
			}
		} else {
			s.logger.Info().Msg("HTTP server gracefully stopped.")
		}
	}

	if s.coreService != nil {
		s.logger.Info().Msg("Stopping core IngestionService logic...")
		s.coreService.Stop() // Assumes connectors.IngestionService.Stop() does not return an error
		s.logger.Info().Msg("Core IngestionService logic stopped.")
	}

	s.logger.Info().Msg("IngestionService application shut down process completed.")
	return firstErr
}

// Run starts the server and waits for a shutdown signal.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stopSignal:
		s.logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal.")
	case <-ctx.Done():
		s.logger.Info().Str("error", ctx.Err().Error()).Msg("Context cancelled, initiating shutdown.")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	return s.Stop(shutdownCtx)
}
