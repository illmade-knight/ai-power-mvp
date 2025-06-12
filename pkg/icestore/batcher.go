package icestore

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file implements a generic batching worker for the icestore library.
// It is analogous to bqstore.BatchInserter but is designed to work with a
// DataUploader for file-based storage.
// ====================================================================================

// BatcherConfig holds configuration for the Batcher.
type BatcherConfig struct {
	BatchSize    int
	FlushTimeout time.Duration
}

// Batcher manages the batching of items of type T.
// It receives messages, collects their payloads into a batch, and periodically
// flushes the batch to a DataUploader. It implicitly implements the
// consumers.MessageProcessor[T] interface.
type Batcher[T any] struct {
	config       *BatcherConfig
	uploader     DataUploader[T]
	logger       zerolog.Logger
	inputChan    chan *types.BatchedMessage[T]
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewBatcher creates a new generic Batcher for a given type T.
func NewBatcher[T any](
	config *BatcherConfig,
	uploader DataUploader[T],
	logger zerolog.Logger,
) *Batcher[T] {
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	return &Batcher[T]{
		config:       config,
		uploader:     uploader,
		logger:       logger.With().Str("component", "IceStoreBatcher").Logger(),
		inputChan:    make(chan *types.BatchedMessage[T], config.BatchSize*2),
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}
}

// Start begins the batching worker goroutine.
func (b *Batcher[T]) Start() {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_timeout", b.config.FlushTimeout).
		Msg("Starting icestore Batcher worker...")
	b.wg.Add(1)
	go b.worker()
}

// Stop gracefully shuts down the Batcher, ensuring any pending items are flushed.
func (b *Batcher[T]) Stop() {
	b.logger.Info().Msg("Stopping icestore Batcher...")
	b.shutdownFunc()
	close(b.inputChan)
	b.wg.Wait()
	if err := b.uploader.Close(); err != nil {
		b.logger.Error().Err(err).Msg("Error closing underlying data uploader")
	}
	b.logger.Info().Msg("IceStore Batcher stopped.")
}

// Input returns the write-only channel to which batched messages should be sent.
func (b *Batcher[T]) Input() chan<- *types.BatchedMessage[T] {
	return b.inputChan
}

// worker contains the core batching logic. It runs in a separate goroutine.
func (b *Batcher[T]) worker() {
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

// flush sends the current batch to the uploader and handles the Ack/Nack logic.
func (b *Batcher[T]) flush(batch []*types.BatchedMessage[T]) {
	if len(batch) == 0 {
		return
	}

	payloads := make([]*T, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // Longer timeout for file uploads
	defer cancel()

	if err := b.uploader.UploadBatch(ctx, payloads); err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to upload batch, Nacking messages.")
		for _, msg := range batch {
			if msg.OriginalMessage.Nack != nil {
				msg.OriginalMessage.Nack()
			}
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Msg("Successfully uploaded batch, Acking messages.")
		for _, msg := range batch {
			if msg.OriginalMessage.Ack != nil {
				msg.OriginalMessage.Ack()
			}
		}
	}
}
