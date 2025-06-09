package bigquery

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// BatchedMessage is a wrapper that pairs the original message with its decoded payload.
// This allows ack/nack operations to be handled after the batch insert result is known.
type BatchedMessage struct {
	OriginalMessage ConsumedMessage
	Payload         *GardenMonitorPayload
}

// BatchInserterConfig holds configuration for the BatchInserter.
type BatchInserterConfig struct {
	BatchSize    int
	FlushTimeout time.Duration
}

// DecodedDataBatchInserter defines an interface for inserting a batch of decoded readings.
type DecodedDataBatchInserter interface {
	InsertBatch(ctx context.Context, readings []*GardenMonitorPayload) error
	Close() error
}

// BatchInserter manages batching and insertion of meter readings.
type BatchInserter struct {
	config       *BatchInserterConfig
	inserter     DecodedDataBatchInserter
	logger       zerolog.Logger
	inputChan    chan *BatchedMessage // Changed to accept the new wrapper type
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewBatchInserter creates a new BatchInserter.
func NewBatchInserter(
	config *BatchInserterConfig,
	inserter DecodedDataBatchInserter,
	logger zerolog.Logger,
) *BatchInserter {
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	return &BatchInserter{
		config:       config,
		inserter:     inserter,
		logger:       logger.With().Str("component", "BatchInserter").Logger(),
		inputChan:    make(chan *BatchedMessage, config.BatchSize*2), // Uses the new type
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}
}

// Start begins the batching worker.
func (b *BatchInserter) Start() {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_timeout", b.config.FlushTimeout).
		Msg("Starting BatchInserter worker...")

	b.wg.Add(1)
	go b.worker()
}

// Stop gracefully shuts down the BatchInserter.
func (b *BatchInserter) Stop() {
	b.logger.Info().Msg("Stopping BatchInserter...")
	b.shutdownFunc()
	b.wg.Wait()
	b.logger.Info().Msg("BatchInserter stopped.")
}

// Input returns the channel to which payloads should be sent.
func (b *BatchInserter) Input() chan<- *BatchedMessage { // Updated return type
	return b.inputChan
}

// worker is the core logic that collects items into a batch and flushes it.
func (b *BatchInserter) worker() {
	defer b.wg.Done()
	defer b.logger.Info().Msg("BatchInserter worker stopped.")

	batch := make([]*BatchedMessage, 0, b.config.BatchSize) // Batch of wrapped messages
	ticker := time.NewTicker(b.config.FlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-b.shutdownCtx.Done():
			b.logger.Info().Msg("Shutdown signal received. Flushing final batch...")
			b.flush(batch)
			return
		case reading, ok := <-b.inputChan:
			if !ok {
				b.logger.Info().Msg("Input channel closed. Flushing final batch...")
				b.flush(batch)
				return
			}
			batch = append(batch, reading)
			if len(batch) >= b.config.BatchSize {
				b.logger.Debug().Msg("Batch size reached. Flushing batch.")
				b.flush(batch)
				batch = make([]*BatchedMessage, 0, b.config.BatchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				b.logger.Debug().Msg("Flush timeout reached. Flushing batch.")
				b.flush(batch)
				batch = make([]*BatchedMessage, 0, b.config.BatchSize)
			}
		}
	}
}

// flush sends the current batch to the inserter and handles Ack/Nack logic.
func (b *BatchInserter) flush(batch []*BatchedMessage) {
	if len(batch) == 0 {
		return
	}

	// Extract payloads for the inserter
	payloads := make([]*GardenMonitorPayload, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := b.inserter.InsertBatch(ctx, payloads)
	if err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to insert batch, Nacking messages.")
		// Nack all messages in the batch on failure
		for _, msg := range batch {
			msg.OriginalMessage.Nack()
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Msg("Successfully flushed batch, Acking messages.")
		// Ack all messages in the batch on success
		for _, msg := range batch {
			msg.OriginalMessage.Ack()
		}
	}
}
