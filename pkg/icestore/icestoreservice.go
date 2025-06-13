package icestore

import (
	"fmt"

	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file provides a convenience constructor for creating an icestore-specific
// processing service. It leverages the generic ProcessingService from the
// shared consumers package.
// REFACTORED: All generic types have been removed. This package is now specific
// to archiving ArchivalData.
// ====================================================================================

// NewIceStorageService is a constructor function that assembles and returns a fully configured
// GCS archival pipeline. It is now specific to processing ArchivalData.
func NewIceStorageService(
	numWorkers int,
	consumer consumers.MessageConsumer,
	batcher *Batcher, // Non-generic
	transformer consumers.MessageTransformer[ArchivalData], // Explicit type
	logger zerolog.Logger,
) (*consumers.ProcessingService[ArchivalData], error) {

	// The consumers.ProcessingService is still generic, so we specify that it
	// will be handling the ArchivalData type.
	processingService, err := consumers.NewProcessingService[ArchivalData](
		numWorkers,
		consumer,
		batcher, // Pass the non-generic Batcher
		transformer,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing service for icestore: %w", err)
	}

	return processingService, nil
}

// NewGCSBatchProcessor is a high-level convenience constructor that creates a
// complete GCS archival pipeline component (uploader + batcher) that satisfies the
// consumers.MessageProcessor interface.
func NewGCSBatchProcessor(
	gcsClient GCSClient,
	batchCfg *BatcherConfig,
	uploaderCfg GCSBatchUploaderConfig,
	logger zerolog.Logger,
) (*Batcher, error) {
	// 1. Create the underlying GCS-specific data uploader.
	gcsUploader, err := NewGCSBatchUploader(gcsClient, uploaderCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS uploader: %w", err)
	}

	// 2. Wrap the GCS uploader with the batching logic.
	batcher := NewBatcher(batchCfg, gcsUploader, logger)

	return batcher, nil
}
