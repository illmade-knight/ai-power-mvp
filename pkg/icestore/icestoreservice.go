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
// ====================================================================================

// NewIceStorageService is a constructor function that assembles and returns a fully configured
// GCS archival pipeline.
//
// It takes the icestore-specific Batcher and wires it into the generic
// ProcessingService from the shared consumers package. This demonstrates how the
// shared pipeline can be easily adapted for the icestore backend.
func NewIceStorageService[T any](
	numWorkers int,
	consumer consumers.MessageConsumer,
	batcher *Batcher[T], // The icestore-specific processor
	decoder consumers.PayloadDecoder[T],
	logger zerolog.Logger,
) (*consumers.ProcessingService[T], error) {

	// The icestore.Batcher already satisfies the consumers.MessageProcessor interface,
	// so we can pass it directly to the generic service constructor.
	genericService, err := consumers.NewProcessingService[T](
		numWorkers,
		consumer,
		batcher, // Pass the Batcher as the MessageProcessor
		decoder,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic processing service for icestore: %w", err)
	}

	return genericService, nil
}
