package xdevice

import (
	"context"
	telemetry "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// NewMeterReading creates a MeterReading from a specific device's decoded payload
// (e.g., DecodedPayload) and metadata from the consumed upstream message.
// Renamed from NewXDeviceMeterReading.
func NewMeterReading(u ConsumedUpstreamMessage, d DecodedPayload) *telemetry.MeterReading {
	return &telemetry.MeterReading{
		Uid:                        d.UID,
		Reading:                    d.Reading,
		AverageCurrent:             d.AverageCurrent,
		MaxCurrent:                 d.MaxCurrent,
		MaxVoltage:                 d.MaxVoltage,
		AverageVoltage:             d.AverageVoltage,
		DeviceEui:                  u.DeviceEUI,
		ClientId:                   u.ClientID,
		LocationId:                 u.LocationID,
		DeviceCategory:             u.DeviceCategory,
		OriginalMqttTime:           timestamppb.New(u.OriginalMQTTTime),
		UpstreamIngestionTimestamp: timestamppb.New(u.IngestionTimestamp),
		ProcessedTimestamp:         timestamppb.New(time.Now()),
		DeviceType:                 "XDevice",
	}
}

// DecodedDataInserter defines an interface for inserting decoded meter readings.
type DecodedDataInserter interface {
	Insert(ctx context.Context, reading *telemetry.MeterReading) error // Now uses MeterReading
	Close() error
}
