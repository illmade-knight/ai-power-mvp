package bqstore

import (
	"cloud.google.com/go/bigquery"
	"context"
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
// REFACTORED: This function now accepts a `MessageTransformer` instead of the legacy
// `PayloadDecoder`. This aligns it with the updated consumers.ProcessingService,
// allowing transformation logic to access the full `ConsumedMessage` and its metadata.
func NewBigQueryService[T any](
	numWorkers int,
	consumer consumers.MessageConsumer,
	batchInserter *BatchInserter[T], // The bqstore-specific processor
	transformer consumers.MessageTransformer[T],
	logger zerolog.Logger,
) (*consumers.ProcessingService[T], error) {

	// The bqstore.BatchInserter already satisfies the consumers.MessageProcessor interface,
	// so we can pass it directly to the generic service constructor.
	genericService, err := consumers.NewProcessingService[T](
		numWorkers,
		consumer,
		batchInserter, // Pass the BatchInserter as the MessageProcessor
		transformer,   // Pass the new MessageTransformer
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create generic processing service for bqstore: %w", err)
	}

	return genericService, nil
}

// NewBigQueryBatchProcessor is a high-level convenience constructor that creates and
// wires together a BigQueryInserter and a BatchInserter. This simplifies service
// initialization by providing a single entry point for creating a complete BigQuery
// batch processing pipeline that satisfies the consumers.MessageProcessor interface.
func NewBigQueryBatchProcessor[T any](
	ctx context.Context,
	client *bigquery.Client,
	batchCfg *BatchInserterConfig,
	dsCfg *BigQueryDatasetConfig,
	logger zerolog.Logger,
) (*BatchInserter[T], error) {
	// 1. Create the underlying BigQuery-specific data inserter.
	bigQueryInserter, err := NewBigQueryInserter[T](ctx, client, dsCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery inserter: %w", err)
	}

	// 2. Wrap the BigQuery inserter with the generic batching logic.
	batchInserter := NewBatcher[T](batchCfg, bigQueryInserter, logger)

	return batchInserter, nil
}
