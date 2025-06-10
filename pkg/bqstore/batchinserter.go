package bqstore

import (
	"context"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// DataBatchInserter is a generic interface for inserting a batch of items of any type T.
// It abstracts the destination data store (e.g., BigQuery, Postgres, etc.).
type DataBatchInserter[T any] interface {
	// InsertBatch saves a slice of items of type T.
	InsertBatch(ctx context.Context, items []*T) error
	Close() error
}

// --- Generic Batch Inserter Implementation ---

// BatchedMessage is a generic wrapper that pairs the original consumed message
// with its decoded payload of type T. This allows ack/nack operations to be
// handled after the batch insert result is known.
type BatchedMessage[T any] struct {
	OriginalMessage types.ConsumedMessage
	Payload         *T
}

// BatchInserterConfig holds configuration for the BatchInserter.
// This remains unchanged as it's not dependent on a specific type.
type BatchInserterConfig struct {
	BatchSize    int
	FlushTimeout time.Duration
}

// BatchInserter is now a generic struct that manages batching and insertion for items of type T.
// It uses the generic DataBatchInserter[T] interface to delegate the actual insertion.
type BatchInserter[T any] struct {
	config       *BatchInserterConfig
	inserter     DataBatchInserter[T] // Uses the generic interface
	logger       zerolog.Logger
	inputChan    chan *BatchedMessage[T] // The channel is now generic
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewBatchInserter creates a new generic BatchInserter for a given type T.
func NewBatchInserter[T any](
	config *BatchInserterConfig,
	inserter DataBatchInserter[T], // Accepts the generic interface
	logger zerolog.Logger,
) *BatchInserter[T] {
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	return &BatchInserter[T]{
		config:       config,
		inserter:     inserter,
		logger:       logger.With().Str("component", "BatchInserter").Logger(),
		inputChan:    make(chan *BatchedMessage[T], config.BatchSize*2), // Generic channel
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}
}

// Start begins the batching worker. This method's logic is unchanged.
func (b *BatchInserter[T]) Start() {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_timeout", b.config.FlushTimeout).
		Msg("Starting generic BatchInserter worker...")

	b.wg.Add(1)
	go b.worker()
}

// Stop gracefully shuts down the BatchInserter. This method's logic is unchanged.
func (b *BatchInserter[T]) Stop() {
	b.logger.Info().Msg("Stopping generic BatchInserter...")
	b.shutdownFunc()
	close(b.inputChan) // Close the input channel to signal the worker to flush and exit.
	b.wg.Wait()
	b.logger.Info().Msg("Generic BatchInserter stopped.")
}

// Input returns the channel to which payloads should be sent.
// The channel type is now generic.
func (b *BatchInserter[T]) Input() chan<- *BatchedMessage[T] {
	return b.inputChan
}

// worker is the core logic that collects items into a batch and flushes it.
func (b *BatchInserter[T]) worker() {
	defer b.wg.Done()
	defer b.logger.Info().Msg("BatchInserter worker stopped.")

	// The batch slice is now generic.
	batch := make([]*BatchedMessage[T], 0, b.config.BatchSize)
	ticker := time.NewTicker(b.config.FlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-b.shutdownCtx.Done():
			b.logger.Info().Msg("Shutdown signal received. Flushing final batch...")
			b.flush(batch)
			return

		case msg, ok := <-b.inputChan:
			if !ok {
				b.logger.Info().Msg("Input channel closed. Flushing final batch...")
				b.flush(batch)
				return // Exit the worker goroutine.
			}
			batch = append(batch, msg)
			if len(batch) >= b.config.BatchSize {
				b.logger.Debug().Int("current_batch_size", len(batch)).Msg("Batch size reached. Flushing batch.")
				b.flush(batch)
				batch = make([]*BatchedMessage[T], 0, b.config.BatchSize) // Reset batch
			}

		case <-ticker.C:
			if len(batch) > 0 {
				b.logger.Debug().Int("current_batch_size", len(batch)).Msg("Flush timeout reached. Flushing batch.")
				b.flush(batch)
				batch = make([]*BatchedMessage[T], 0, b.config.BatchSize) // Reset batch
			}
		}
	}
}

// flush sends the current batch to the inserter and handles Ack/Nack logic.
// This function now operates on a generic batch.
func (b *BatchInserter[T]) flush(batch []*BatchedMessage[T]) {
	if len(batch) == 0 {
		return
	}

	// Extract payloads of type T for the generic inserter.
	payloads := make([]*T, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Call the generic InsertBatch method.
	err := b.inserter.InsertBatch(ctx, payloads)
	if err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to insert batch, Nacking messages.")
		// Nack all messages in the batch on failure.
		for _, msg := range batch {
			msg.OriginalMessage.Nack()
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Msg("Successfully flushed batch, Acking messages.")
		// Ack all messages in the batch on success.
		for _, msg := range batch {
			msg.OriginalMessage.Ack()
		}
	}
}
