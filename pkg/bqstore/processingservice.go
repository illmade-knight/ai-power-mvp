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
// This interface is a good abstraction and remains unchanged.
type MessageConsumer interface {
	Messages() <-chan types.ConsumedMessage
	Start(ctx context.Context) error
	Stop() error
	Done() <-chan struct{}
}

// PayloadDecoder is a generic function type. An implementation of this function
// is responsible for decoding the raw byte payload from a message queue
// into a pointer to a specific struct of type T.
type PayloadDecoder[T any] func(payload []byte) (*T, error)

// --- Generic Processing Service ---

// ServiceConfig holds configuration for the ProcessingService.
// Batching-related configs have been moved to BatchInserterConfig.
type ServiceConfig struct {
	NumProcessingWorkers int
}

// ProcessingService is the generic orchestrator for the data pipeline.
// It consumes messages, uses a pool of workers to decode them using the
// injected decoder, and forwards them to the generic batch inserter.
type ProcessingService[T any] struct {
	config        *ServiceConfig
	consumer      MessageConsumer
	batchInserter *BatchInserter[T] // Uses the generic BatchInserter
	decoder       PayloadDecoder[T] // The function to decode a message payload
	logger        zerolog.Logger
	wg            sync.WaitGroup
	shutdownCtx   context.Context
	shutdownFunc  context.CancelFunc
}

// NewProcessingService creates a new, generic ProcessingService.
// It follows Inversion of Control (IoC) by taking its dependencies
// (consumer, inserter, decoder) as arguments.
func NewProcessingService[T any](
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
	}, nil
}

// Start begins the service operation: it starts the batch inserter, the message consumer,
// and the processing workers.
func (s *ProcessingService[T]) Start() error {
	s.logger.Info().Msg("Starting generic ProcessingService...")
	s.batchInserter.Start()

	// The shutdown context is passed to the consumer so it can be stopped by the service.
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

// processConsumedMessage is the core logic for each worker. It uses the injected
// decoder to process the message and sends the result to the batch inserter.
func (s *ProcessingService[T]) processConsumedMessage(msg types.ConsumedMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Processing consumed message")

	// Use the injected decoder function to parse the payload.
	decodedPayload, err := s.decoder(msg.Payload)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to decode payload, Nacking message.")
		msg.Nack()
		return
	}

	// The payload might be valid but empty (e.g. a message with no data), so we can skip it.
	if decodedPayload == nil {
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Decoder returned nil payload, Acking and skipping.")
		msg.Ack()
		return
	}

	// Create the generic batched message.
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

// Stop gracefully shuts down the ProcessingService in the correct order.
func (s *ProcessingService[T]) Stop() {
	s.logger.Info().Msg("Stopping ProcessingService...")

	// 1. Signal all workers and the consumer to stop.
	s.shutdownFunc()

	// 2. Wait for the consumer to fully stop. This closes the `Messages()` channel.
	s.logger.Info().Msg("Waiting for message consumer to stop...")
	<-s.consumer.Done()
	s.logger.Info().Msg("Message consumer stopped.")

	// 3. Wait for all processing workers to finish. They will exit because the consumer
	// channel is now closed, ensuring any in-flight messages are processed.
	s.logger.Info().Msg("Waiting for processing workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All processing workers completed.")

	// 4. Now that all messages are in the batcher, stop it to trigger a final flush.
	s.batchInserter.Stop()

	s.logger.Info().Msg("ProcessingService stopped gracefully.")
}
