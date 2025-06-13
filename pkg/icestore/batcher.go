package icestore

import (
	"context"
	"sync"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
)

// BatcherConfig holds configuration for the Batcher.
type BatcherConfig struct {
	BatchSize    int
	FlushTimeout time.Duration
}

// Batcher manages the batching of ArchivalData items.
// It receives messages, groups them by their BatchKey, and flushes groups
// when they are full or a timeout occurs.
type Batcher struct {
	config       *BatcherConfig
	uploader     DataUploader
	logger       zerolog.Logger
	inputChan    chan *types.BatchedMessage[ArchivalData]
	wg           sync.WaitGroup
	shutdownCtx  context.Context
	shutdownFunc context.CancelFunc
}

// NewBatcher creates a new Batcher for ArchivalData.
func NewBatcher(
	config *BatcherConfig,
	uploader DataUploader,
	logger zerolog.Logger,
) *Batcher {
	if config.BatchSize <= 0 {
		logger.Warn().Int("provided_batch_size", config.BatchSize).Msg("BatchSize must be positive, defaulting to 100.")
		config.BatchSize = 100
	}
	if config.FlushTimeout <= 0 {
		logger.Warn().Dur("provided_flush_timeout", config.FlushTimeout).Msg("FlushTimeout must be positive, defaulting to 1 minute.")
		config.FlushTimeout = 1 * time.Minute
	}
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	return &Batcher{
		config:       config,
		uploader:     uploader,
		logger:       logger.With().Str("component", "IceStoreBatcher").Logger(),
		inputChan:    make(chan *types.BatchedMessage[ArchivalData], config.BatchSize*2),
		shutdownCtx:  shutdownCtx,
		shutdownFunc: shutdownFunc,
	}
}

// Start begins the batching worker goroutine.
func (b *Batcher) Start() {
	b.logger.Info().
		Int("batch_size", b.config.BatchSize).
		Dur("flush_timeout", b.config.FlushTimeout).
		Msg("Starting icestore Batcher worker...")
	b.wg.Add(1)
	go b.worker()
}

// Stop gracefully shuts down the Batcher, ensuring any pending items are flushed.
func (b *Batcher) Stop() {
	b.logger.Info().Msg("Stopping icestore Batcher...")
	b.shutdownFunc() // Signal worker to stop.
	// Close the input channel to unblock the worker if it's waiting for messages.
	// This must happen after shutdownFunc to avoid panics on closed channel writes.
	close(b.inputChan)
	b.wg.Wait() // Wait for the worker to finish flushing.
	if err := b.uploader.Close(); err != nil {
		b.logger.Error().Err(err).Msg("Error closing underlying data uploader")
	}
	b.logger.Info().Msg("IceStore Batcher stopped.")
}

// Input returns the write-only channel for ArchivalData messages.
func (b *Batcher) Input() chan<- *types.BatchedMessage[ArchivalData] {
	return b.inputChan
}

// worker contains the core key-aware batching logic.
func (b *Batcher) worker() {
	defer b.wg.Done()
	// batches now holds a map of lists, one for each batch key.
	batches := make(map[string][]*types.BatchedMessage[ArchivalData])
	ticker := time.NewTicker(b.config.FlushTimeout)
	defer ticker.Stop()

	// flushAll is a helper to drain all pending batches.
	flushAll := func() {
		if len(batches) == 0 {
			return
		}
		b.logger.Info().Int("key_count", len(batches)).Msg("Flushing all pending batches due to timeout or shutdown.")
		for key, batchToFlush := range batches {
			b.flush(batchToFlush)
			// After flushing, remove the key from the map.
			delete(batches, key)
		}
	}

	for {
		select {
		case <-b.shutdownCtx.Done():
			// Shutdown signal received, flush everything and exit.
			flushAll()
			return

		case msg, ok := <-b.inputChan:
			if !ok {
				// Channel closed, means Stop() was called. Flush everything and exit.
				b.logger.Info().Msg("Input channel closed, flushing remaining batches.")
				flushAll()
				return
			}

			key := msg.Payload.GetBatchKey()
			batches[key] = append(batches[key], msg)
			b.logger.Debug().Str("batch_key", key).Int("current_size", len(batches[key])).Msg("Appended message to batch.")

			// If the batch for this specific key is full, flush it immediately.
			if len(batches[key]) >= b.config.BatchSize {
				b.logger.Info().Str("batch_key", key).Msg("Batch is full, flushing.")
				b.flush(batches[key])
				delete(batches, key)
			}

		case <-ticker.C:
			// Timer ticked, flush all pending, non-empty batches.
			flushAll()
		}
	}
}

// flush sends the current batch to the uploader and handles the Ack/Nack logic.
func (b *Batcher) flush(batch []*types.BatchedMessage[ArchivalData]) {
	if len(batch) == 0 {
		return
	}

	// The uploader is responsible for grouping, but since this batcher now
	// only sends homogeneous batches (all items have the same key), the uploader's
	// work will be simpler. This is perfectly fine.
	payloads := make([]*ArchivalData, len(batch))
	for i, msg := range batch {
		payloads[i] = msg.Payload
	}

	// Use a background context for the upload itself to ensure it completes
	// even if the service shutdown context is canceled.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := b.uploader.UploadBatch(ctx, payloads); err != nil {
		b.logger.Error().Err(err).Int("batch_size", len(batch)).Msg("Failed to upload batch, Nacking messages.")
		for _, msg := range batch {
			// CORRECTED: The original message holds the Ack/Nack functions.
			// We must check if the Nack function itself is nil before calling it to prevent panics.
			if msg.OriginalMessage.Nack != nil {
				msg.OriginalMessage.Nack()
			}
		}
	} else {
		b.logger.Info().Int("batch_size", len(batch)).Str("batch_key", payloads[0].GetBatchKey()).Msg("Successfully uploaded batch, Acking messages.")
		for _, msg := range batch {
			// CORRECTED: Similarly, check if the Ack function is available before calling it.
			if msg.OriginalMessage.Ack != nil {
				msg.OriginalMessage.Ack()
			}
		}
	}
}
