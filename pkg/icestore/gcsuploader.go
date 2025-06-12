package icestore

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// ====================================================================================
// This file contains the generic Google Cloud Storage uploader.
// It has been refactored to work with any data type that satisfies the Batchable
// interface, making it truly reusable.
// ====================================================================================

// Batchable is an interface that any type must implement to be processed by the GCSBatchUploader.
// It provides a way for the uploader to determine the destination file path for an item.
type Batchable interface {
	GetBatchKey() string
}

// GCSBatchUploaderConfig holds configuration specific to the GCS uploader.
type GCSBatchUploaderConfig struct {
	BucketName   string
	ObjectPrefix string // e.g., "raw-audit-logs/daily-aggregates"
}

// GCSBatchUploader is a generic struct that implements the DataUploader interface.
// It is constrained to work only with types that implement the Batchable interface.
type GCSBatchUploader[T Batchable] struct {
	client GCSClient
	config GCSBatchUploaderConfig
	logger zerolog.Logger
	wg     sync.WaitGroup
}

// NewGCSBatchUploader creates a new generic uploader configured for Google Cloud Storage.
func NewGCSBatchUploader[T Batchable](
	gcsClient GCSClient,
	config GCSBatchUploaderConfig,
	logger zerolog.Logger,
) (*GCSBatchUploader[T], error) {
	if gcsClient == nil {
		return nil, errors.New("GCS client cannot be nil")
	}
	if config.BucketName == "" {
		return nil, errors.New("GCS bucket name is required")
	}
	return &GCSBatchUploader[T]{
		client: gcsClient,
		config: config,
		logger: logger.With().Str("component", "GCSBatchUploader").Logger(),
	}, nil
}

// UploadBatch takes a batch of Batchable items, groups them by their key,
// and uploads each group to a separate, compressed GCS object.
func (u *GCSBatchUploader[T]) UploadBatch(ctx context.Context, items []*T) error {
	if len(items) == 0 {
		return nil
	}

	groupedBatches := make(map[string][]*T)
	for _, item := range items {
		if item == nil {
			continue
		}
		key := (*item).GetBatchKey() // Use the interface method to get the path
		if key == "" {
			u.logger.Warn().Msg("item has an empty BatchKey, skipping.")
			continue
		}
		groupedBatches[key] = append(groupedBatches[key], item)
	}

	if len(groupedBatches) == 0 {
		return nil
	}

	var uploadWg sync.WaitGroup
	errs := make(chan error, len(groupedBatches))

	for key, batchData := range groupedBatches {
		uploadWg.Add(1)
		u.wg.Add(1)

		go func(batchKey string, dataToUpload []*T) {
			defer uploadWg.Done()
			defer u.wg.Done()
			if err := u.uploadSingleGroup(ctx, batchKey, dataToUpload); err != nil {
				errs <- err
			}
		}(key, batchData)
	}

	uploadWg.Wait()
	close(errs)

	var combinedErr error
	for err := range errs {
		if combinedErr == nil {
			combinedErr = err
		} else {
			combinedErr = fmt.Errorf("%v; %w", combinedErr, err)
		}
	}
	return combinedErr
}

// uploadSingleGroup handles writing one group of records to a GCS object.
// This version uses a more robust io.Pipe pattern that relies on the pipe's
// built-in error propagation, removing the need for a separate sync channel.
func (u *GCSBatchUploader[T]) uploadSingleGroup(ctx context.Context, batchKey string, batchData []*T) error {
	if len(batchData) == 0 {
		return nil
	}
	batchFileID := uuid.New().String()
	objectName := path.Join(u.config.ObjectPrefix, batchKey, fmt.Sprintf("%s.jsonl.gz", batchFileID))
	u.logger.Info().Str("object_name", objectName).Int("message_count", len(batchData)).Msg("Starting upload for grouped batch")

	objHandle := u.client.Bucket(u.config.BucketName).Object(objectName)
	gcsWriter := objHandle.NewWriter(ctx)
	pr, pw := io.Pipe()

	// This goroutine produces the compressed data and writes it to the pipe.
	go func() {
		// This deferred function ensures the pipe writer is always closed,
		// with an error if one occurred during processing. This is what
		// unblocks the `io.Copy` in the main goroutine.
		var err error
		defer func() {
			pw.CloseWithError(err)
		}()

		gz := gzip.NewWriter(pw)
		enc := json.NewEncoder(gz)

		for _, rec := range batchData {
			if err = enc.Encode(rec); err != nil {
				err = fmt.Errorf("json encoding failed for %s: %w", objectName, err)
				return // The defer will close the pipe with this error.
			}
		}

		// It is critical to close the gzip writer to flush its buffer to the pipe.
		if err = gz.Close(); err != nil {
			err = fmt.Errorf("gzip writer close failed for %s: %w", objectName, err)
			return // The defer will close the pipe with this error.
		}
	}()

	// This call blocks, streaming data from the pipe reader to the GCS writer.
	// It will return nil on success (when the pipe is closed cleanly) or the
	// error passed to `CloseWithError` by the producer goroutine.
	bytesWritten, pipeReadErr := io.Copy(gcsWriter, pr)

	// Per GCS client library documentation, gcsWriter.Close() MUST be called to
	// finalize or abort the upload. The error from Close() indicates the final status.
	closeErr := gcsWriter.Close()

	// Prioritize reporting the error from the data production/pipe first, as it's the root cause.
	if pipeReadErr != nil {
		return fmt.Errorf("failed to stream data for GCS object %s: %w", objectName, pipeReadErr)
	}

	// If the pipe was clean but closing the GCS writer failed, the upload failed.
	if closeErr != nil {
		return fmt.Errorf("failed to close GCS object writer for %s: %w", objectName, closeErr)
	}

	u.logger.Info().
		Str("object_name", objectName).
		Int64("bytes_written", bytesWritten).
		Msg("Successfully uploaded grouped batch to GCS")
	return nil
}

// Close waits for any pending upload goroutines to complete.
func (u *GCSBatchUploader[T]) Close() error {
	u.logger.Info().Msg("Waiting for all pending GCS uploads to complete...")
	u.wg.Wait()
	u.logger.Info().Msg("All GCS uploads completed.")
	return nil
}
