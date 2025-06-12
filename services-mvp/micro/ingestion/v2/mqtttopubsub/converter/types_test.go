package converter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseMQTTMessage tests the ParseMQTTMessage function.
func TestParseMQTTMessage(t *testing.T) {
	t.Run("ValidJSON", func(t *testing.T) {
		jsonData := `
		{
			"device_info": {
				"device_eui": "0102030405060708"
			},
			"lorawan_data": {
				"rssi": -50,
				"snr": 10.5,
				"data_rate": "SF7BW125",
				"frequency": 868.1,
				"gateway_eui": "AABBCCDDEEFF0011",
				"received_at": "2023-01-01T12:00:00Z"
			},
			"raw_payload": "AQIDBA==",
			"message_id": "msg123",
			"topic": "device/uplink",
			"message_timestamp": "2023-01-01T12:00:05Z"
		}
		`
		expectedReceivedAt, _ := time.Parse(time.RFC3339, "2023-01-01T12:00:00Z")
		expectedMsgTimestamp, _ := time.Parse(time.RFC3339, "2023-01-01T12:00:05Z")

		msg, err := ParseMQTTMessage([]byte(jsonData))
		require.NoError(t, err, "ParseMQTTMessage should not return an error for valid JSON")
		require.NotNil(t, msg, "ParseMQTTMessage should return a non-nil message for valid JSON")

		assert.Equal(t, "0102030405060708", msg.DeviceInfo.DeviceEUI, "DeviceEUI does not match")
		assert.Equal(t, -50, msg.LoRaWAN.RSSI, "RSSI does not match")
		assert.Equal(t, 10.5, msg.LoRaWAN.SNR, "SNR does not match")
		assert.Equal(t, "SF7BW125", msg.LoRaWAN.DataRate, "DataRate does not match")
		assert.Equal(t, 868.1, msg.LoRaWAN.Frequency, "Frequency does not match")
		assert.Equal(t, "AABBCCDDEEFF0011", msg.LoRaWAN.GatewayEUI, "GatewayEUI does not match")
		assert.True(t, expectedReceivedAt.Equal(msg.LoRaWAN.ReceivedAt), "ReceivedAt does not match")
		assert.Equal(t, "AQIDBA==", msg.RawPayload, "RawPayload does not match")
		assert.Equal(t, "msg123", msg.MessageID, "MessageID does not match")
		assert.Equal(t, "device/uplink", msg.Topic, "Topic does not match")
		assert.True(t, expectedMsgTimestamp.Equal(msg.MessageTimestamp), "MessageTimestamp does not match")
	})

	t.Run("ValidJSONMinimal", func(t *testing.T) {
		// Test with only mandatory fields
		jsonData := `
		{
			"device_info": {
				"device_eui": "0102030405060708"
			},
			"raw_payload": "AQIDBA==",
			"message_id": "msg123",
			"topic": "device/uplink",
			"message_timestamp": "2023-01-01T12:00:05Z"
		}
		`
		expectedMsgTimestamp, _ := time.Parse(time.RFC3339, "2023-01-01T12:00:05Z")

		msg, err := ParseMQTTMessage([]byte(jsonData))
		require.NoError(t, err, "ParseMQTTMessage should not return an error for valid minimal JSON")
		require.NotNil(t, msg, "ParseMQTTMessage should return a non-nil message for valid minimal JSON")

		assert.Equal(t, "0102030405060708", msg.DeviceInfo.DeviceEUI, "DeviceEUI does not match")
		assert.Equal(t, "AQIDBA==", msg.RawPayload, "RawPayload does not match")
		assert.Equal(t, "msg123", msg.MessageID, "MessageID does not match")
		assert.Equal(t, "device/uplink", msg.Topic, "Topic does not match")
		assert.True(t, expectedMsgTimestamp.Equal(msg.MessageTimestamp), "MessageTimestamp does not match")
		// Optional fields should have their zero values
		assert.Zero(t, msg.LoRaWAN.RSSI, "RSSI should be zero for minimal JSON")
		assert.Zero(t, msg.LoRaWAN.SNR, "SNR should be zero for minimal JSON")
		assert.Empty(t, msg.LoRaWAN.DataRate, "DataRate should be empty for minimal JSON")
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		jsonData := `{"device_info": {"device_eui": "0102030405060708"}, "raw_payload": "AQIDBA==",` // Malformed JSON
		msg, err := ParseMQTTMessage([]byte(jsonData))
		require.Error(t, err, "ParseMQTTMessage should return an error for invalid JSON")
		assert.Nil(t, msg, "ParseMQTTMessage should return a nil message for invalid JSON")
		// Check if the error is a json.SyntaxError or similar
		var syntaxError *json.SyntaxError
		assert.ErrorAs(t, err, &syntaxError, "Error should be a JSON syntax error")
	})

	t.Run("EmptyJSON", func(t *testing.T) {
		jsonData := `{}`
		msg, err := ParseMQTTMessage([]byte(jsonData))
		// Depending on struct tags, this might or might not be an error
		// if fields are required and not omitempty.
		// For MQTTMessage, DeviceInfo.DeviceEUI is not omitempty.
		// However, json.Unmarshal won't error if a sub-struct is zero and its fields are not set.
		// It will error if a top-level required field is missing and not omitempty in the JSON struct definition.
		// Let's assume an empty JSON leads to a message with zero values for its fields.
		require.NoError(t, err, "ParseMQTTMessage should not error on empty JSON if fields are optional or have defaults")
		require.NotNil(t, msg, "ParseMQTTMessage should return a non-nil message for empty JSON")
		assert.Empty(t, msg.DeviceInfo.DeviceEUI, "DeviceEUI should be empty")
		assert.Empty(t, msg.RawPayload, "RawPayload should be empty")
	})

	t.Run("NilInput", func(t *testing.T) {
		msg, err := ParseMQTTMessage(nil)
		require.Error(t, err, "ParseMQTTMessage should return an error for nil input")
		assert.Nil(t, msg, "ParseMQTTMessage should return a nil message for nil input")
	})

	t.Run("EmptyByteArray", func(t *testing.T) {
		msg, err := ParseMQTTMessage([]byte{})
		require.Error(t, err, "ParseMQTTMessage should return an error for empty byte array input")
		assert.Nil(t, msg, "ParseMQTTMessage should return a nil message for empty byte array input")
	})
}
