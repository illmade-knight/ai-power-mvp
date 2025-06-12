package bqstore

import (
	"fmt"

	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file provides a convenience constructor for creating a bqstore-specific
// processing service. It leverages the generic ProcessingService from the
// shared consumers package.
// ====================================================================================

// NewBigQueryService is a constructor function that assembles and returns a fully configured
// BigQuery processing pipeline.
//
// It takes the bqstore-specific BatchInserter and wires it into the generic
// ProcessingService from the shared consumers package. The function returns a pointer
// to the generic service, making the types explicit and avoiding linter warnings
// about copying locks. The function's name provides the necessary domain context.
func NewBigQueryService[T any](
	numWorkers int,
	consumer consumers.MessageConsumer,
	batchInserter *BatchInserter[T], // The bqstore-specific processor
	decoder consumers.PayloadDecoder[T],
	logger zerolog.Logger,
) (*consumers.ProcessingService[T], error) {

	// The bqstore.BatchInserter already satisfies the consumers.MessageProcessor interface,
	// so we can pass it directly to the generic service constructor.
	genericService, err := consumers.NewProcessingService[T](
		numWorkers,
		consumer,
		batchInserter, // Pass the BatchInserter as the MessageProcessor
		decoder,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic processing service for bqstore: %w", err)
	}

	// Return the generic service pointer directly. No type alias or casting is needed.
	return genericService, nil
}
