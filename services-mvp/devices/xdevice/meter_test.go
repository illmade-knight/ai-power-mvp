package xdevice

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewMeterReading(t *testing.T) {
	decodedPayload := DecodedPayload{
		UID:            "DEVXYZ123",
		Reading:        123.45,
		AverageCurrent: 1.23,
		MaxCurrent:     2.34,
		MaxVoltage:     230.5,
		AverageVoltage: 229.5,
	}
	upstreamTime := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	ingestionTime := time.Now().Add(-5 * time.Minute).UTC().Truncate(time.Second)

	upstreamMeta := ConsumedUpstreamMessage{
		DeviceEUI:          "EUI_NETWORK_123",
		ClientID:           "ClientABC",
		LocationID:         "LocationXYZ",
		DeviceCategory:     "HVAC",
		OriginalMQTTTime:   upstreamTime,
		IngestionTimestamp: ingestionTime,
	}
	deviceType := "XDevice"

	meterReading := NewMeterReading(
		decodedPayload.UID,
		decodedPayload.Reading,
		decodedPayload.AverageCurrent,
		decodedPayload.MaxCurrent,
		decodedPayload.MaxVoltage,
		decodedPayload.AverageVoltage,
		upstreamMeta,
		deviceType,
	)

	assert.Equal(t, decodedPayload.UID, meterReading.UID)
	assert.Equal(t, decodedPayload.Reading, meterReading.Reading)
	assert.Equal(t, decodedPayload.AverageCurrent, meterReading.AverageCurrent)
	assert.Equal(t, upstreamMeta.DeviceEUI, meterReading.DeviceEUI)
	assert.Equal(t, upstreamMeta.ClientID, meterReading.ClientID)
	assert.Equal(t, upstreamMeta.LocationID, meterReading.LocationID)
	assert.Equal(t, upstreamMeta.DeviceCategory, meterReading.DeviceCategory)
	assert.Equal(t, upstreamMeta.OriginalMQTTTime, meterReading.OriginalMQTTTime)
	assert.Equal(t, upstreamMeta.IngestionTimestamp, meterReading.UpstreamIngestionTimestamp)
	assert.Equal(t, deviceType, meterReading.DeviceType)
	assert.False(t, meterReading.ProcessedTimestamp.IsZero(), "ProcessedTimestamp should be set")
	assert.WithinDuration(t, time.Now().UTC(), meterReading.ProcessedTimestamp, 5*time.Second, "ProcessedTimestamp should be recent")
}
