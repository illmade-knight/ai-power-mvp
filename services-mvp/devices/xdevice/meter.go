package xdevice

import (
	"context"
	"time"
)

// --- ConsumedUpstreamMessage (from ingestion service, for context) ---
// This represents the structure of messages this xdevice service might consume.
type ConsumedUpstreamMessage struct {
	DeviceEUI          string    `json:"device_eui"`
	RawPayload         string    `json:"raw_payload"` // The hex string for the specific device
	OriginalMQTTTime   time.Time `json:"original_mqtt_time,omitempty"`
	LoRaWANReceivedAt  time.Time `json:"lorawan_received_at,omitempty"` // From EnrichedMessage
	IngestionTimestamp time.Time `json:"ingestion_timestamp"`           // Timestamp from the previous (ingestion) service
	ClientID           string    `json:"client_id,omitempty"`
	LocationID         string    `json:"location_id,omitempty"`
	DeviceCategory     string    `json:"device_category,omitempty"` // General category from ingestion
	ProcessingError    string    `json:"processing_error,omitempty"`
}

// MeterReading represents the structured meter reading data after decoding.
// This struct is generic and can be used by various storage backends.
// BigQuery struct tags are included for the BigQuery-specific implementation.
// Renamed from XDeviceMeterReading.
type MeterReading struct {
	// Fields from a specific device's decoded payload (e.g., DecodedPayload)
	// These will be common fields, or you might use a nested struct or JSON for device-specific parts.
	// For XDevice:
	UID            string  `json:"uid" bigquery:"uid"` // Specific device's UID from its payload
	Reading        float32 `json:"reading" bigquery:"reading"`
	AverageCurrent float32 `json:"average_current,omitempty" bigquery:"average_current"` // Omit if not applicable to all devices
	MaxCurrent     float32 `json:"max_current,omitempty" bigquery:"max_current"`
	MaxVoltage     float32 `json:"max_voltage,omitempty" bigquery:"max_voltage"`
	AverageVoltage float32 `json:"average_voltage,omitempty" bigquery:"average_voltage"`

	// Common metadata fields from the upstream message
	DeviceEUI                  string    `json:"device_eui" bigquery:"device_eui"` // Network-level EUI
	ClientID                   string    `json:"client_id" bigquery:"client_id"`
	LocationID                 string    `json:"location_id" bigquery:"location_id"`
	DeviceCategory             string    `json:"device_category" bigquery:"device_category"` // General category
	OriginalMQTTTime           time.Time `json:"original_mqtt_time" bigquery:"original_mqtt_time"`
	UpstreamIngestionTimestamp time.Time `json:"upstream_ingestion_timestamp" bigquery:"upstream_ingestion_timestamp"`

	// Timestamp for this specific device decoder service's processing
	ProcessedTimestamp time.Time `json:"processed_timestamp" bigquery:"processed_timestamp"`
	DeviceType         string    `json:"device_type" bigquery:"device_type"` // Added to distinguish device types in BQ
}

// NewMeterReading creates a MeterReading from a specific device's decoded payload
// (e.g., DecodedPayload) and metadata from the consumed upstream message.
// Renamed from NewXDeviceMeterReading.
func NewMeterReading(
	// Parameters for XDevice specific fields
	uid string,
	reading float32,
	avgCurrent float32,
	maxCurrent float32,
	maxVoltage float32,
	avgVoltage float32,
	// Common metadata
	upstreamMsgMeta ConsumedUpstreamMessage,
	deviceType string, // e.g., "XDevice", "YDevice"
) MeterReading {
	return MeterReading{
		UID:                        uid,
		Reading:                    reading,
		AverageCurrent:             avgCurrent,
		MaxCurrent:                 maxCurrent,
		MaxVoltage:                 maxVoltage,
		AverageVoltage:             avgVoltage,
		DeviceEUI:                  upstreamMsgMeta.DeviceEUI,
		ClientID:                   upstreamMsgMeta.ClientID,
		LocationID:                 upstreamMsgMeta.LocationID,
		DeviceCategory:             upstreamMsgMeta.DeviceCategory,
		OriginalMQTTTime:           upstreamMsgMeta.OriginalMQTTTime,
		UpstreamIngestionTimestamp: upstreamMsgMeta.IngestionTimestamp,
		ProcessedTimestamp:         time.Now().UTC(),
		DeviceType:                 deviceType,
	}
}

// DecodedDataInserter defines an interface for inserting decoded meter readings.
type DecodedDataInserter interface {
	Insert(ctx context.Context, reading MeterReading) error // Now uses MeterReading
	Close() error
}
