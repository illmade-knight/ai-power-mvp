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
// This file contains the Google Cloud Storage uploader.
// REFACTORED: It is no longer generic and works specifically with ArchivalData.
// ====================================================================================

// GCSBatchUploaderConfig holds configuration specific to the GCS uploader.
type GCSBatchUploaderConfig struct {
	BucketName   string
	ObjectPrefix string
}

// GCSBatchUploader implements the DataUploader interface for ArchivalData.
// It is no longer generic, simplifying its structure.
type GCSBatchUploader struct {
	client GCSClient
	config GCSBatchUploaderConfig
	logger zerolog.Logger
	wg     sync.WaitGroup
}

// NewGCSBatchUploader creates a new uploader configured for Google Cloud Storage.
// The generic constraint has been removed.
func NewGCSBatchUploader(
	gcsClient GCSClient,
	config GCSBatchUploaderConfig,
	logger zerolog.Logger,
) (*GCSBatchUploader, error) {
	if gcsClient == nil {
		return nil, errors.New("GCS client cannot be nil")
	}
	if config.BucketName == "" {
		return nil, errors.New("GCS bucket name is required")
	}
	return &GCSBatchUploader{
		client: gcsClient,
		config: config,
		logger: logger.With().Str("component", "GCSBatchUploader").Logger(),
	}, nil
}

// UploadBatch takes a batch of ArchivalData items, groups them by their key,
// and uploads each group to a separate, compressed GCS object.
// The type is now explicitly []*ArchivalData.
func (u *GCSBatchUploader) UploadBatch(ctx context.Context, items []*ArchivalData) error {
	if len(items) == 0 {
		return nil
	}

	groupedBatches := make(map[string][]*ArchivalData)
	for _, item := range items {
		if item == nil {
			continue
		}
		key := item.GetBatchKey() // Directly call the method on ArchivalData.
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

		go func(batchKey string, dataToUpload []*ArchivalData) {
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
func (u *GCSBatchUploader) uploadSingleGroup(ctx context.Context, batchKey string, batchData []*ArchivalData) error {
	if len(batchData) == 0 {
		return nil
	}
	batchFileID := uuid.New().String()
	objectName := path.Join(u.config.ObjectPrefix, batchKey, fmt.Sprintf("%s.jsonl.gz", batchFileID))
	u.logger.Info().Str("object_name", objectName).Int("message_count", len(batchData)).Msg("Starting upload for grouped batch")

	objHandle := u.client.Bucket(u.config.BucketName).Object(objectName)
	gcsWriter := objHandle.NewWriter(ctx)
	pr, pw := io.Pipe()

	go func() {
		var err error
		defer func() {
			pw.CloseWithError(err)
		}()

		gz := gzip.NewWriter(pw)
		enc := json.NewEncoder(gz)

		for _, rec := range batchData {
			if err = enc.Encode(rec); err != nil {
				err = fmt.Errorf("json encoding failed for %s: %w", objectName, err)
				return
			}
		}

		if err = gz.Close(); err != nil {
			err = fmt.Errorf("gzip writer close failed for %s: %w", objectName, err)
			return
		}
	}()

	bytesWritten, pipeReadErr := io.Copy(gcsWriter, pr)
	closeErr := gcsWriter.Close()

	if pipeReadErr != nil {
		return fmt.Errorf("failed to stream data for GCS object %s: %w", objectName, pipeReadErr)
	}
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
func (u *GCSBatchUploader) Close() error {
	u.logger.Info().Msg("Waiting for all pending GCS uploads to complete...")
	u.wg.Wait()
	u.logger.Info().Msg("All GCS uploads completed.")
	return nil
}
