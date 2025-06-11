package bqstore

import (
	"context"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"sync"
)

// DefaultConfig provides sensible defaults.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		NumProcessingWorkers: 20,
	}
}

// --- Generic Interfaces & Types ---

// MessageConsumer defines the interface for a message source (e.g., Pub/Sub, Kafka).
type MessageConsumer interface {
	Messages() <-chan types.ConsumedMessage
	Start(ctx context.Context) error
	Stop() error
	Done() <-chan struct{}
}

// PayloadDecoder is a generic function type.
type PayloadDecoder[T any] func(payload []byte) (*T, error)

// --- Generic Processing Service ---

// ServiceConfig holds configuration for the ProcessingService.
type ServiceConfig struct {
	NumProcessingWorkers int
}

// ProcessingService is the generic orchestrator for the data pipeline.
type ProcessingService[T any] struct {
	config        *ServiceConfig
	consumer      MessageConsumer
	batchInserter *BatchInserter[T]
	decoder       PayloadDecoder[T]
	logger        zerolog.Logger
	wg            sync.WaitGroup
	shutdownCtx   context.Context
	shutdownFunc  context.CancelFunc
	errChan       chan error // NEW: Channel to broadcast processing errors
}

// NewBatchingService creates a new, generic ProcessingService.
func NewBatchingService[T any](
	config *ServiceConfig,
	consumer MessageConsumer,
	batchInserter *BatchInserter[T],
	decoder PayloadDecoder[T],
	logger zerolog.Logger,
) (*ProcessingService[T], error) {
	if consumer == nil {
		return nil, fmt.Errorf("MessageConsumer cannot be nil")
	}
	if batchInserter == nil {
		return nil, fmt.Errorf("BatchInserter cannot be nil")
	}
	if decoder == nil {
		return nil, fmt.Errorf("PayloadDecoder cannot be nil")
	}
	if config.NumProcessingWorkers <= 0 {
		defaultCfg := DefaultServiceConfig()
		logger.Warn().
			Int("provided_workers", config.NumProcessingWorkers).
			Int("default_workers", defaultCfg.NumProcessingWorkers).
			Msg("NumProcessingWorkers was zero or negative, applying default value.")
		config.NumProcessingWorkers = defaultCfg.NumProcessingWorkers
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	return &ProcessingService[T]{
		config:        config,
		consumer:      consumer,
		batchInserter: batchInserter,
		decoder:       decoder,
		logger:        logger.With().Str("service", "ProcessingService").Logger(),
		shutdownCtx:   shutdownCtx,
		shutdownFunc:  shutdownFunc,
		// Create a buffered channel to prevent workers from blocking if the listener is slow.
		errChan: make(chan error, config.NumProcessingWorkers),
	}, nil
}

// Err returns a read-only channel of processing errors. The owner of this
// service can listen to this channel to be notified of any non-fatal errors
// that occur during message processing.
func (s *ProcessingService[T]) Err() <-chan error {
	return s.errChan
}

// Start begins the service operation.
func (s *ProcessingService[T]) Start() error {
	s.logger.Info().Msg("Starting generic ProcessingService...")
	s.batchInserter.Start()

	if err := s.consumer.Start(s.shutdownCtx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	s.logger.Info().Int("worker_count", s.config.NumProcessingWorkers).Msg("Starting processing workers...")
	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Debug().Int("worker_id", workerID).Msg("Processing worker started.")
			for {
				select {
				case <-s.shutdownCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down.")
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
	s.logger.Info().Msg("ProcessingService started successfully.")
	return nil
}

// processConsumedMessage is the core logic for each worker.
func (s *ProcessingService[T]) processConsumedMessage(msg types.ConsumedMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Processing consumed message")

	decodedPayload, err := s.decoder(msg.Payload)
	if err != nil {
		// Log the error as before.
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to decode payload, Nacking message.")
		msg.Nack()

		// NEW: Send the error to the error channel for external monitoring.
		select {
		case s.errChan <- fmt.Errorf("worker %d failed to decode msg %s: %w", workerID, msg.ID, err):
		case <-s.shutdownCtx.Done(): // Don't block if we're shutting down.
		}
		return
	}

	if decodedPayload == nil {
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Decoder returned nil payload, Acking and skipping.")
		msg.Ack()
		return
	}

	batchedMsg := &BatchedMessage[T]{
		OriginalMessage: msg,
		Payload:         decodedPayload,
	}

	select {
	case s.batchInserter.Input() <- batchedMsg:
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Payload sent to batch inserter.")
	case <-s.shutdownCtx.Done():
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Shutdown in progress, Nacking message.")
		msg.Nack()
	}
}

// Stop gracefully shuts down the ProcessingService.
func (s *ProcessingService[T]) Stop() {
	s.logger.Info().Msg("Stopping ProcessingService...")
	s.shutdownFunc()

	s.logger.Info().Msg("Waiting for message consumer to stop...")
	<-s.consumer.Done()
	s.logger.Info().Msg("Message consumer stopped.")

	s.logger.Info().Msg("Waiting for processing workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All processing workers completed.")

	s.batchInserter.Stop()

	// NEW: Close the error channel to signal that no more errors will be sent.
	close(s.errChan)

	s.logger.Info().Msg("ProcessingService stopped gracefully.")
}
