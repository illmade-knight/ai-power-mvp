package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// --- ConsumedMessage & MessageConsumer Interface (Ideally in a shared package) ---

// ConsumedMessage represents a message received from a message broker.
type ConsumedMessage struct {
	ID          string    // Message ID from the broker
	Payload     []byte    // The raw message payload
	PublishTime time.Time // Timestamp when the message was published to the broker
	Ack         func()    // Function to call to acknowledge the message
	Nack        func()    // Function to call to negative-acknowledge the message
}

// MessageConsumer defines an interface for consuming messages from a broker.
type MessageConsumer interface {
	Messages() <-chan ConsumedMessage
	Start(ctx context.Context) error
	Stop() error
	Done() <-chan struct{}
}

// --- Device Processing Service --- (Renamed from XDevice Processing Service)

// ServiceConfig holds configuration for the ProcessingService.
// Renamed from ProcessingServiceConfig
type ServiceConfig struct {
	// Env var name for the subscription MessageID for messages from the ingestion service
	UpstreamSubscriptionEnvVar string
	NumProcessingWorkers       int
}

// LoadServiceConfigFromEnv loads service configuration.
// Renamed from LoadProcessingServiceConfigFromEnv
func LoadServiceConfigFromEnv() (*ServiceConfig, error) {
	cfg := &ServiceConfig{
		UpstreamSubscriptionEnvVar: "PUBSUB_SUBSCRIPTION_ID_XDEVICE_INPUT", // Example env var name, might need to be more generic if service handles multiple device types
		NumProcessingWorkers:       5,                                      // Default
	}
	// Renamed XDEVICE_UPSTREAM_SUB_ENV_VAR to PROCESSING_UPSTREAM_SUB_ENV_VAR for more generic approach
	if subEnvVar := os.Getenv("PROCESSING_UPSTREAM_SUB_ENV_VAR"); subEnvVar != "" {
		cfg.UpstreamSubscriptionEnvVar = subEnvVar
	}
	// TODO: Add parsing for NumProcessingWorkers from env if needed
	return cfg, nil
}

// DecodedDataInserter defines an interface for inserting decoded meter readings.
type DecodedDataInserter interface {
	Insert(ctx context.Context, reading GardenMonitorPayload) error // Now uses MeterReading
	Close() error
}

// ProcessingService consumes messages from Pub/Sub, decodes device-specific payloads,
// and stores them using a DecodedDataInserter.
// Renamed from XDeviceProcessingService
type ProcessingService struct {
	config       *ServiceConfig // Updated type
	consumer     MessageConsumer
	inserter     DecodedDataInserter
	logger       zerolog.Logger
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewProcessingService creates a new ProcessingService.
// Renamed from NewXDeviceProcessingService
func NewProcessingService(
	ctx context.Context, // For initializing consumer
	config *ServiceConfig, // Updated type
	inserter DecodedDataInserter,
	logger zerolog.Logger,
) (*ProcessingService, error) {
	// The subscription MessageID here is for "XDevice" input. If this service becomes more generic,
	// it might need multiple consumers or a way to filter messages by device type.
	// Using the renamed LoadGooglePubSubConsumerConfigFromEnv
	consumerCfg, err := LoadGooglePubSubConsumerConfigFromEnv(config.UpstreamSubscriptionEnvVar, "default-device-input-sub")
	if err != nil {
		return nil, fmt.Errorf("failed to load device input consumer config: %w", err)
	}
	consumer, err := NewGooglePubSubConsumer(ctx, consumerCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create device input message consumer: %w", err)
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	return &ProcessingService{ // Updated struct name
		config:       config,
		consumer:     consumer,
		inserter:     inserter,
		logger:       logger.With().Str("service", "ProcessingService").Logger(), // Updated service name in log
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}, nil
}

// Start begins consuming messages and processing them.
func (s *ProcessingService) Start() error { // Updated receiver type
	s.logger.Info().Msg("Starting ProcessingService...") // Updated log message

	if err := s.consumer.Start(s.shutdownCtx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Debug().Int("worker_id", workerID).Msg("Device processing worker started.") // Updated log
			for {
				select {
				case <-s.shutdownCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Device processing worker shutting down.") // Updated log
					return
				case msg, ok := <-s.consumer.Messages():
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("Consumer channel closed, worker exiting.")
						return
					}
					s.processConsumedMessage(msg, workerID)
				}
			}
		}(i)
	}
	s.logger.Info().Msg("ProcessingService started with workers.") // Updated log
	return nil
}

func (s *ProcessingService) processConsumedMessage(msg ConsumedMessage, workerID int) { // Updated receiver type
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Processing consumed message")

	var upstreamMsg GardenMonitorMessage
	if err := json.Unmarshal(msg.Payload, &upstreamMsg); err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to unmarshal Pub/Sub payload into ConsumedUpstreamMessage, Nacking.")
		msg.Nack()
		return
	}

	if upstreamMsg.Payload == nil {
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Consumed message has empty RawPayload for device, Acking and skipping.")
		msg.Ack()
		return
	}

	insertCtx, cancelInsert := context.WithTimeout(s.shutdownCtx, 30*time.Second)
	defer cancelInsert()

	payload := *upstreamMsg.Payload

	if err := s.inserter.Insert(insertCtx, payload); err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Str("uid", payload.UID).Msg("Failed to insert meter reading, Nacking.")
		msg.Nack()
	} else {
		s.logger.Debug().Str("msg_id", msg.ID).Str("uid", payload.UID).Msg("Meter reading processed and inserted, Acking.")
		msg.Ack()
	}
}

// Stop gracefully shuts down the ProcessingService.
func (s *ProcessingService) Stop() { // Updated receiver type
	s.logger.Info().Msg("Stopping ProcessingService...") // Updated log
	s.shutdownFunc()

	s.logger.Info().Msg("Stopping message consumer...")
	if err := s.consumer.Stop(); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping message consumer")
	}
	<-s.consumer.Done()
	s.logger.Info().Msg("Message consumer stopped.")

	s.logger.Info().Msg("Waiting for processing workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All processing workers completed.")

	if s.inserter != nil {
		s.logger.Info().Msg("Closing data inserter...")
		if err := s.inserter.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing data inserter")
		} else {
			s.logger.Info().Msg("Data inserter closed.")
		}
	}
	s.logger.Info().Msg("ProcessingService stopped.") // Updated log
}
