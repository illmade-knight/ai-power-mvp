package connectors

import (
	"encoding/json"
	"time"
)

// LoRaWANData contains common LoRaWAN-specific information that might come from the network server via MQTT.
type LoRaWANData struct {
	RSSI       int       `json:"rssi,omitempty"`        // Received Signal Strength Indicator
	SNR        float64   `json:"snr,omitempty"`         // Signal to Noise Ratio
	DataRate   string    `json:"data_rate,omitempty"`   // e.g., "SF7BW125"
	Frequency  float64   `json:"frequency,omitempty"`   // e.g., 868.1
	GatewayEUI string    `json:"gateway_eui,omitempty"` // EUI of the gateway that received the message
	ReceivedAt time.Time `json:"received_at,omitempty"` // Timestamp when the message was received by the network server/broker
}

// DeviceInfo contains identifiers for the end device.
type DeviceInfo struct {
	DeviceEUI string `json:"device_eui"` // Unique EUI for the LoRaWAN device (critical for identification)
	// Add other identifiers if provided by your broker, e.g.:
	// DeviceName  string `json:"device_name,omitempty"`
	// ApplicationID string `json:"application_id,omitempty"`
}

// MQTTMessage represents the structure of the JSON message received from the MQTT broker.
type MQTTMessage struct {
	DeviceInfo DeviceInfo  `json:"device_info"`            // Information to identify the device
	LoRaWAN    LoRaWANData `json:"lorawan_data,omitempty"` // LoRaWAN specific network parameters
	RawPayload string      `json:"raw_payload"`            // The actual payload from the device, often base64 encoded or hex string.
	// We assume RawPayload is a string. If it's a complex object, this would be different.
	// If it's binary data that's base64 encoded, you might want to handle its decoding later.
	MessageTimestamp time.Time `json:"message_timestamp"` // Timestamp from the MQTT message itself or when it was published
}

// UnidentifiedDeviceMessage is the structure for messages when metadata is not found.
type UnidentifiedDeviceMessage struct {
	DeviceEUI          string    `json:"device_eui"`
	RawPayload         string    `json:"raw_payload"`
	OriginalMQTTTime   time.Time `json:"original_mqtt_time,omitempty"`
	LoRaWANReceivedAt  time.Time `json:"lorawan_received_at,omitempty"`
	IngestionTimestamp time.Time `json:"ingestion_timestamp"`
	ProcessingError    string    `json:"processing_error"` // e.g., "ErrMetadataNotFound"
}

// EnrichedMessage is what the ingestion service will produce to send to the next topic (e.g., for the decoder).
// This includes the original raw payload and the added metadata.
type EnrichedMessage struct {
	// Original data
	RawPayload        string    `json:"raw_payload"`
	DeviceEUI         string    `json:"device_eui"`
	OriginalMQTTTime  time.Time `json:"original_mqtt_time,omitempty"`  // From MQTTMessage.MessageTimestamp
	LoRaWANReceivedAt time.Time `json:"lorawan_received_at,omitempty"` // From LoRaWANData.ReceivedAt

	// Added Metadata
	ClientID       string `json:"client_id"`
	LocationID     string `json:"location_id"`
	DeviceCategory string `json:"device_category"`

	// Ingestion service metadata
	IngestionTimestamp time.Time `json:"ingestion_timestamp"`
}

// Helper function to parse a JSON string into an MQTTMessage struct.
// This would be used in your main service logic and in tests.
func ParseMQTTMessage(jsonData []byte) (*MQTTMessage, error) {
	var msg MQTTMessage
	err := json.Unmarshal(jsonData, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
