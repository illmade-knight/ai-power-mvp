package converter

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
	MessageID        string    `json:"message_id"`
	Topic            string    `json:"topic"`
	MessageTimestamp time.Time `json:"message_timestamp"` // Timestamp from the MQTT message itself or when it was published
}

type InMessage struct {
	Payload   []byte `json:"payload"`
	Topic     string `json:"topic"`
	MessageID string `json:"message_id"`
}

// ParseMQTTMessage Helper function to parse a JSON string into an MQTTMessage struct.
// This would be used in your main service logic and in tests.
func ParseMQTTMessage(jsonData []byte) (*MQTTMessage, error) {
	var msg MQTTMessage
	err := json.Unmarshal(jsonData, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
