package icestore

import (
	"context"
	"errors"
	"time"
)

// --- Error Definitions for this Package ---
var (
	// ErrMsgProcessingError indicates a generic error during message processing within the archiver.
	ErrMsgProcessingError = errors.New("error processing message for archival")
	// String constant for matching the error string from the ingestion service
	// This assumes the ingestion service sets ProcessingError to "device metadata not found"
	// when its local ErrMetadataNotFound occurs.
	OriginalErrMetadataNotFoundString = "device metadata not found"
)

// CombinedMessagePayload is a mix of EnrichedMessage and UnidentifiedDeviceMessage
type CombinedMessagePayload struct {
	DeviceEUI          string    `json:"device_eui"`
	RawPayload         string    `json:"raw_payload"`
	OriginalMQTTTime   time.Time `json:"original_mqtt_time,omitempty"`
	LoRaWANReceivedAt  time.Time `json:"lorawan_received_at,omitempty"`
	IngestionTimestamp time.Time `json:"ingestion_timestamp"`
	ClientID           string    `json:"client_id,omitempty"`
	LocationID         string    `json:"location_id,omitempty"`
	DeviceCategory     string    `json:"device_category,omitempty"`
	ProcessingError    string    `json:"processing_error,omitempty"`
}

// RawDataArchiver interface (remains the same signature for Archive)
type RawDataArchiver interface {
	Archive(ctx context.Context, pubSubMessagePayload []byte, pathTimestamp time.Time) error
	Stop() error
}
