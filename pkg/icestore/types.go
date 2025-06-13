package icestore

import (
	"context"
	"encoding/json"
	"time"
)

// ====================================================================================
// This file defines the core types for the icestore library.
// ====================================================================================

// DataUploader is a generic interface for uploading a batch of processed items.
// It is the `icestore` equivalent of the `bqstore.DataBatchInserter` interface.
type DataUploader interface {
	// UploadBatch uploads a batch of items of type T.
	UploadBatch(ctx context.Context, items []*ArchivalData) error
	// Close performs any necessary cleanup, such as waiting for pending uploads.
	Close() error
}

// ArchivalData is the final, structured representation of a message that will be
// serialized into a file in Google Cloud Storage.
type ArchivalData struct {
	// BatchKey determines the destination path within the GCS bucket. It is
	// derived from message content (e.g., date and location) by the decoder.
	// The `json:"-"` tag prevents it from being serialized into the output file itself.
	BatchKey string `json:"-"`

	ID string `json:"id"`

	// OriginalPubSubPayload stores the complete, raw payload of the source message
	// as valid JSON. This ensures a full audit trail.
	OriginalPubSubPayload json.RawMessage `json:"original_pubsub_payload"`

	// ArchivedAt is a timestamp indicating when the message was processed by this service.
	ArchivedAt time.Time `json:"archived_at"`
}

// GetBatchKey allows ArchivalData to satisfy the Batchable interface.
func (a ArchivalData) GetBatchKey() string {
	return a.BatchKey
}
