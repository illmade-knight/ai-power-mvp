package bqstore

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/types" // Uses the shared types package
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file contains the bqstore-specific BatchInserter.
// It has been updated to use the shared types and to implicitly satisfy the
// consumers.MessageProcessor interface, making it compatible with the generic
// ProcessingService.
// ====================================================================================

// DataBatchInserter is a generic interface for inserting a batch of items of any type T.
// It abstracts the destination data store (e.g., BigQuery, Postgres, etc.).
type DataBatchInserter[T any] interface {
	InsertBatch(ctx context.Context, items []*T) error
	Close() error
}

// BatchInserterConfig holds configuration for the BatchInserter.
type BatchInserterConfig struct {
	BatchSize    int
	FlushTimeout time.Duration
}

// BatchInserter manages batching and insertion for items of type T.
// It now implicitly implements the consumers.MessageProcessor[T] interface.
type BatchInserter[T any] struct {
	config       *BatchInserterConfig
	inserter     DataBatchInserter[T]
	logger       zerolog.Logger
	inputChan    chan *types.BatchedMessage[T] // Channel uses the shared type
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewBatcher creates a new generic BatchInserter for a given type T.
func NewBatcher[T any](
	config *BatchInserterConfig,
	inserter DataBatchInserter[T],
	logger zerolog.Logger,
) *BatchInserter[T] {
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	return &BatchInserter[T]{
		config:       config,
		inserter:     inserter,
		logger:       logger.With().Str("component", "BatchInserter").Logger(),
		inputChan:    make(chan *types.BatchedMessage[T], config.BatchSize*2),
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}
}

// Start begins the batching worker. This is part of the MessageProcessor interface.
func (b *BatchInserter[T]) Start() {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_timeout", b.config.FlushTimeout).
		Msg("Starting BatchInserter worker...")
	b.wg.Add(1)
	go b.worker()
}

// Stop gracefully shuts down the BatchInserter. This is part of the MessageProcessor interface.
func (b *BatchInserter[T]) Stop() {
	b.logger.Info().Msg("Stopping BatchInserter...")
	b.shutdownFunc()
	close(b.inputChan)
	b.wg.Wait()
	if err := b.inserter.Close(); err != nil {
		b.logger.Error().Err(err).Msg("Error closing underlying data inserter")
	}
	b.logger.Info().Msg("BatchInserter stopped.")
}

// Input returns the channel to which payloads should be sent.
// This is part of the MessageProcessor interface.
func (b *BatchInserter[T]) Input() chan<- *types.BatchedMessage[T] {
	return b.inputChan
}

// worker is the core logic that collects items into a batch and flushes it.
func (b *BatchInserter[T]) worker() {
	defer b.wg.Done()
	batch := make([]*types.BatchedMessage[T], 0, b.config.BatchSize)
	ticker := time.NewTicker(b.config.FlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-b.shutdownCtx.Done():
			b.flush(batch)
			return

		case msg, ok := <-b.inputChan:
			if !ok {
				b.flush(batch)
				return
			}
			batch = append(batch, msg)
			if len(batch) >= b.config.BatchSize {
				b.flush(batch)
				batch = make([]*types.BatchedMessage[T], 0, b.config.BatchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.flush(batch)
				batch = make([]*types.BatchedMessage[T], 0, b.config.BatchSize)
			}
		}
	}
}

// flush sends the current batch to the inserter and handles Ack/Nack logic.
func (b *BatchInserter[T]) flush(batch []*types.BatchedMessage[T]) {
	if len(batch) == 0 {
		return
	}

	payloads := make([]*T, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := b.inserter.InsertBatch(ctx, payloads); err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to insert batch, Nacking messages.")
		for _, msg := range batch {
			// Add a nil check to prevent panics in tests or other scenarios
			// where Ack/Nack handlers might not be provided.
			if msg.OriginalMessage.Nack != nil {
				msg.OriginalMessage.Nack()
			}
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Msg("Successfully flushed batch, Acking messages.")
		for _, msg := range batch {
			// Add a nil check to prevent panics.
			if msg.OriginalMessage.Ack != nil {
				msg.OriginalMessage.Ack()
			}
		}
	}
}
