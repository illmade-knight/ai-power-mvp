package consumers

import (
	"context"
	"fmt"
	"sync"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file contains a generic, reusable service for processing messages from any
// MessageConsumer and handing them off to any MessageProcessor.
// ====================================================================================

// ProcessingService orchestrates the pipeline of consuming, decoding, and processing messages.
// It is generic and can be used with any combination of consumers and processors.
type ProcessingService[T any] struct {
	numWorkers   int
	consumer     MessageConsumer
	processor    MessageProcessor[T] // Depends on the new generic interface
	decoder      PayloadDecoder[T]
	logger       zerolog.Logger
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewProcessingService creates a new, generic ProcessingService.
// It requires a consumer to get messages, a decoder to transform them, and a processor
// to handle the transformed data.
func NewProcessingService[T any](
	numWorkers int,
	consumer MessageConsumer,
	processor MessageProcessor[T],
	decoder PayloadDecoder[T],
	logger zerolog.Logger,
) (*ProcessingService[T], error) {
	// In a production library, you would add nil checks for the parameters here.
	if numWorkers <= 0 {
		numWorkers = 5 // Default to 5 workers if an invalid number is provided.
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	return &ProcessingService[T]{
		numWorkers:   numWorkers,
		consumer:     consumer,
		processor:    processor,
		decoder:      decoder,
		logger:       logger.With().Str("service", "ProcessingService").Logger(),
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}, nil
}

// Start begins the service operation. It starts the processor and the consumer,
// then spins up a pool of workers to process messages.
func (s *ProcessingService[T]) Start() error {
	s.logger.Info().Msg("Starting generic ProcessingService...")

	// Start the processor first, so it's ready to receive items.
	s.processor.Start()

	// Start the consumer, passing the service's shutdown context to it.
	if err := s.consumer.Start(s.shutdownCtx); err != nil {
		// If the consumer fails to start, stop the processor to clean up.
		s.processor.Stop()
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	// Start a pool of workers to process messages concurrently.
	s.logger.Info().Int("worker_count", s.numWorkers).Msg("Starting processing workers...")
	for i := 0; i < s.numWorkers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}

	s.logger.Info().Msg("Generic ProcessingService started successfully.")
	return nil
}

// worker is the main loop for each concurrent worker.
func (s *ProcessingService[T]) worker(workerID int) {
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
}

// processConsumedMessage contains the core logic for each worker.
// It decodes a message and, upon success, sends it to the processor.
func (s *ProcessingService[T]) processConsumedMessage(msg types.ConsumedMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Processing message")

	decodedPayload, err := s.decoder(msg.Payload)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to decode payload, Nacking message.")
		msg.Nack()
		return
	}

	if decodedPayload == nil {
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Decoder returned nil payload, Acking and skipping.")
		msg.Ack()
		return
	}

	batchedMsg := &types.BatchedMessage[T]{
		OriginalMessage: msg,
		Payload:         decodedPayload,
	}

	select {
	case s.processor.Input() <- batchedMsg:
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Payload sent to processor.")
	case <-s.shutdownCtx.Done():
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Shutdown in progress, Nacking message.")
		msg.Nack()
	}
}

// Stop gracefully shuts down the entire service in the correct order.
func (s *ProcessingService[T]) Stop() {
	s.logger.Info().Msg("Stopping generic ProcessingService...")

	// 1. Signal all workers and the consumer to begin shutting down.
	s.shutdownFunc()

	// 2. Wait for the consumer to fully stop.
	s.logger.Info().Msg("Waiting for message consumer to stop...")
	<-s.consumer.Done()
	s.logger.Info().Msg("Message consumer stopped.")

	// 3. Wait for all processing workers to finish their current tasks.
	s.logger.Info().Msg("Waiting for processing workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All processing workers completed.")

	// 4. Stop the processor. This will flush any remaining buffered items.
	s.processor.Stop()

	s.logger.Info().Msg("Generic ProcessingService stopped gracefully.")
}
