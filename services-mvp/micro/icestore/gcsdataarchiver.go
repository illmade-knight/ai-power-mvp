package icestore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid" // For unique batch IDs
	"github.com/rs/zerolog"
)

// GCSDataArchiverConfig (remains the same)
type GCSDataArchiverConfig struct {
	BucketName          string
	ObjectPrefix        string
	BatchSizeThreshold  int
	DefaultStorageClass string
}

// --- GCS Client Abstraction Interfaces ---
type GCSClient interface {
	Bucket(name string) GCSBucketHandle
}
type GCSBucketHandle interface {
	Object(name string) GCSObjectHandle
}
type GCSObjectHandle interface {
	NewWriter(ctx context.Context) GCSWriter
}
type GCSWriter interface {
	io.WriteCloser
	// SetChunkSize was removed
	Attrs() *storage.ObjectAttrs
}

// --- Adapters for Google Cloud Storage Client ---
type gcsClientAdapter struct{ client *storage.Client }

func (a *gcsClientAdapter) Bucket(name string) GCSBucketHandle {
	return &storageBucketHandleAdapter{BucketHandle: a.client.Bucket(name)}
}

type storageBucketHandleAdapter struct{ *storage.BucketHandle }

func (a *storageBucketHandleAdapter) Object(name string) GCSObjectHandle {
	return &storageObjectHandleAdapter{ObjectHandle: a.BucketHandle.Object(name)}
}

type storageObjectHandleAdapter struct{ *storage.ObjectHandle }

func (a *storageObjectHandleAdapter) NewWriter(ctx context.Context) GCSWriter {
	storageWriter := a.ObjectHandle.NewWriter(ctx)
	return &gcsWriterAdapter{Writer: storageWriter}
}

type gcsWriterAdapter struct{ *storage.Writer }

func (gwa *gcsWriterAdapter) Attrs() *storage.ObjectAttrs { return &gwa.Writer.ObjectAttrs }

// SetChunkSize is not needed here if not in GCSWriter interface

var _ GCSClient = &gcsClientAdapter{}
var _ GCSBucketHandle = &storageBucketHandleAdapter{}
var _ GCSObjectHandle = &storageObjectHandleAdapter{}
var _ GCSWriter = &gcsWriterAdapter{}

// RawDataForArchival
type RawDataForArchival struct {
	DeviceEUI               string          `json:"device_eui"`
	MessageTimestampForPath time.Time       `json:"-"`
	OriginalPubSubPayload   json.RawMessage `json:"original_pubsub_payload"` // Stores the original Pub/Sub message payload as valid JSON
	ArchivedAt              time.Time       `json:"archived_at"`
}

// GCSDataArchiver
type GCSDataArchiver struct {
	client     GCSClient
	config     GCSDataArchiverConfig
	logger     zerolog.Logger
	mu         sync.Mutex
	batchMap   map[string][]RawDataForArchival
	wg         sync.WaitGroup
	shutdownCh chan struct{}
}

func NewGCSDataArchiver(
	ctx context.Context,
	gcsClient *storage.Client,
	config GCSDataArchiverConfig,
	logger zerolog.Logger,
) (*GCSDataArchiver, error) {
	if gcsClient == nil {
		return nil, errors.New("GCS client cannot be nil")
	}
	if config.BucketName == "" {
		return nil, errors.New("GCS bucket name is required")
	}
	if config.BatchSizeThreshold <= 0 {
		config.BatchSizeThreshold = 100
	}
	if config.ObjectPrefix == "" {
		config.ObjectPrefix = "raw-audit-logs/daily-aggregates"
	}
	clientAdapter := &gcsClientAdapter{client: gcsClient}
	return &GCSDataArchiver{
		client:     clientAdapter,
		config:     config,
		logger:     logger.With().Str("component", "GCSDataArchiver").Logger(),
		batchMap:   make(map[string][]RawDataForArchival),
		shutdownCh: make(chan struct{}),
	}, nil
}

func (a *GCSDataArchiver) Archive(ctx context.Context, pubSubMessagePayload []byte, pathTimestamp time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var combinedMsg CombinedMessagePayload
	if err := json.Unmarshal(pubSubMessagePayload, &combinedMsg); err != nil {
		a.logger.Error().Err(err).Str("payload_snippet", string(pubSubMessagePayload[:min(100, len(pubSubMessagePayload))])).Msg("Failed to unmarshal Pub/Sub message into CombinedMessagePayload. Message will not be archived by this service.")
		return fmt.Errorf("%w: unmarshal error: %v", ErrMsgProcessingError, err)
	}

	// If unmarshal was successful, proceed to determine batch key and archive.
	// The OriginalPubSubPayload should store the raw pubSubMessagePayload directly,
	// as it's already the JSON representation we want to keep.
	var batchKeyLocationPart string
	if combinedMsg.LocationID != "" && combinedMsg.ProcessingError == "" {
		batchKeyLocationPart = combinedMsg.LocationID
	} else {
		batchKeyLocationPart = "deadletter"
		if combinedMsg.ProcessingError != "" {
			a.logger.Info().Str("device_eui", combinedMsg.DeviceEUI).Str("upstream_error", combinedMsg.ProcessingError).Msg("Message routed to deadletter path due to upstream processing error.")
		} else if combinedMsg.LocationID == "" {
			a.logger.Warn().Str("device_eui", combinedMsg.DeviceEUI).Msg("Message missing LocationID (and no upstream error), routing to deadletter path.")
		} else {
			a.logger.Warn().Str("device_eui", combinedMsg.DeviceEUI).Msg("Message routed to deadletter path for other reasons (e.g. unexpected state).")
		}
	}

	year, month, day := pathTimestamp.Date()
	batchKey := fmt.Sprintf("%d/%02d/%02d/%s", year, int(month), day, batchKeyLocationPart)

	archivalData := RawDataForArchival{
		DeviceEUI:               combinedMsg.DeviceEUI,
		MessageTimestampForPath: pathTimestamp,
		OriginalPubSubPayload:   json.RawMessage(pubSubMessagePayload), // Store the original Pub/Sub payload directly
		ArchivedAt:              time.Now().UTC(),
	}

	a.batchMap[batchKey] = append(a.batchMap[batchKey], archivalData)

	if len(a.batchMap[batchKey]) >= a.config.BatchSizeThreshold {
		batchToFlush := a.batchMap[batchKey]
		delete(a.batchMap, batchKey)
		a.mu.Unlock()
		a.flushBatch(ctx, batchKey, batchToFlush)
		a.mu.Lock()
	}
	return nil
}

func (a *GCSDataArchiver) flushBatch(ctx context.Context, batchKey string, batchData []RawDataForArchival) {
	if len(batchData) == 0 {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		batchFileID := uuid.New().String()
		objectName := path.Join(a.config.ObjectPrefix, batchKey, fmt.Sprintf("%s.jsonl.gz", batchFileID))
		a.logger.Info().Str("object_name", objectName).Int("message_count", len(batchData)).Msg("Flushing batch to GCS")
		var compressedData bytes.Buffer
		gzWriter := gzip.NewWriter(&compressedData)
		jsonEncoder := json.NewEncoder(gzWriter)
		for _, record := range batchData {
			if err := jsonEncoder.Encode(record); err != nil {
				a.logger.Error().Err(err).Str("batch_key", batchKey).Str("device_eui_sample", record.DeviceEUI).Msg("Failed to encode single record to JSON for batch")
			}
		}
		if err := gzWriter.Close(); err != nil {
			a.logger.Error().Err(err).Str("batch_key", batchKey).Msg("Failed to finalize gzip compression")
			return
		}
		objHandle := a.client.Bucket(a.config.BucketName).Object(objectName)
		writer := objHandle.NewWriter(ctx)
		attrs := writer.Attrs()
		if attrs == nil {
			a.logger.Error().Str("object_name", objectName).Msg("Failed to get ObjectAttrs from writer")
			writer.Close()
			return
		}
		if a.config.DefaultStorageClass != "" {
			attrs.StorageClass = a.config.DefaultStorageClass
		}
		attrs.ContentType = "application/x-jsonlines"
		attrs.ContentEncoding = "gzip"
		if len(batchData) > 0 {
			attrs.Metadata = map[string]string{
				"batch_key": batchKey, "message_count": fmt.Sprintf("%d", len(batchData)),
				"first_message_eui":         batchData[0].DeviceEUI,
				"first_message_ts_archived": batchData[0].ArchivedAt.Format(time.RFC3339),
				"last_message_ts_archived":  batchData[len(batchData)-1].ArchivedAt.Format(time.RFC3339),
			}
		}
		if _, err := io.Copy(writer, &compressedData); err != nil {
			a.logger.Error().Err(err).Str("object_name", objectName).Msg("Failed to write data to GCS object writer")
			writer.Close()
			return
		}
		if err := writer.Close(); err != nil {
			a.logger.Error().Err(err).Str("object_name", objectName).Msg("Failed to close GCS object writer (commit upload)")
			return
		}
		a.logger.Info().Str("object_name", objectName).Msg("Successfully uploaded batch to GCS")
	}()
}

func (a *GCSDataArchiver) Stop() error {
	a.logger.Info().Msg("Stopping GCSDataArchiver, flushing pending batches...")
	if a.shutdownCh != nil {
		select {
		case <-a.shutdownCh:
		default:
			close(a.shutdownCh)
		}
	}
	a.mu.Lock()
	batchesToFlush := make(map[string][]RawDataForArchival)
	for key, data := range a.batchMap {
		batchesToFlush[key] = data
	}
	a.batchMap = make(map[string][]RawDataForArchival)
	a.mu.Unlock()
	for key, data := range batchesToFlush {
		if len(data) > 0 {
			a.flushBatch(context.Background(), key, data)
		}
	}
	a.logger.Info().Msg("Waiting for all pending GCS uploads to complete...")
	a.wg.Wait()
	a.logger.Info().Msg("All GCS uploads completed. GCSDataArchiver stopped.")
	return nil
}
