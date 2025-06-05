package connectors

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp" // For deep comparison of structs
)

// Helper to create a sample valid MQTTMessage JSON []byte
func makeSampleRawMQTTMsgBytes(deviceEUI, payload string, msgTime time.Time) []byte {
	msg := MQTTMessage{
		DeviceInfo:       DeviceInfo{DeviceEUI: deviceEUI},
		RawPayload:       payload,
		MessageTimestamp: msgTime,
		LoRaWAN: LoRaWANData{ // Add some LoRaWAN data for completeness
			RSSI:       -70,
			SNR:        7.5,
			ReceivedAt: msgTime.Add(-2 * time.Second), // Slightly before message timestamp
		},
	}
	bytes, _ := json.Marshal(msg)
	return bytes
}

// TestParseMQTTMessage_ValidJSON tests unmarshaling of a valid JSON string
// into the MQTTMessage struct.
func TestParseMQTTMessage_ValidJSON(t *testing.T) {
	// Sample timestamp for consistency in tests
	sampleTimeStr := "2023-10-26T10:00:00Z"
	sampleTime, _ := time.Parse(time.RFC3339, sampleTimeStr)

	sampleJSON := []byte(`{
		"device_info": {
			"device_eui": "0102030405060708"
		},
		"lorawan_data": {
			"rssi": -50,
			"snr": 10.5,
			"data_rate": "SF7BW125",
			"frequency": 868.1,
			"gateway_eui": "AABBCCDDEEFF0011",
			"received_at": "` + sampleTimeStr + `"
		},
		"raw_payload": "AQIDBAUGBwg=",
		"message_timestamp": "` + sampleTimeStr + `"
	}`)

	expectedMsg := &MQTTMessage{
		DeviceInfo: DeviceInfo{
			DeviceEUI: "0102030405060708",
		},
		LoRaWAN: LoRaWANData{
			RSSI:       -50,
			SNR:        10.5,
			DataRate:   "SF7BW125",
			Frequency:  868.1,
			GatewayEUI: "AABBCCDDEEFF0011",
			ReceivedAt: sampleTime,
		},
		RawPayload:       "AQIDBAUGBwg=",
		MessageTimestamp: sampleTime,
	}

	gotMsg, err := ParseMQTTMessage(sampleJSON)
	if err != nil {
		t.Fatalf("ParseMQTTMessage() returned an unexpected error: %v", err)
	}

	if !cmp.Equal(expectedMsg, gotMsg) {
		t.Errorf("ParseMQTTMessage() mismatch (-want +got):\n%s", cmp.Diff(expectedMsg, gotMsg))
	}

	// Test individual fields for clarity if needed, though cmp.Equal is comprehensive
	if gotMsg.DeviceInfo.DeviceEUI != "0102030405060708" {
		t.Errorf("Expected DeviceEUI %s, got %s", "0102030405060708", gotMsg.DeviceInfo.DeviceEUI)
	}
	if gotMsg.RawPayload != "AQIDBAUGBwg=" {
		t.Errorf("Expected RawPayload %s, got %s", "AQIDBAUGBwg=", gotMsg.RawPayload)
	}
	if gotMsg.LoRaWAN.RSSI != -50 {
		t.Errorf("Expected LoRaWAN.RSSI %d, got %d", -50, gotMsg.LoRaWAN.RSSI)
	}
}

// TestParseMQTTMessage_MalformedJSON tests handling of invalid JSON.
func TestParseMQTTMessage_MalformedJSON(t *testing.T) {
	malformedJSON := []byte(`{
		"device_info": {
			"device_eui": "0102030405060708"
		},
		"raw_payload": "AQIDBAUGBwg=",
		"message_timestamp": "2023-10-26T10:00:00Z" 
		// Missing closing brace for the main object
	`)

	_, err := ParseMQTTMessage(malformedJSON)
	if err == nil {
		t.Errorf("ParseMQTTMessage() expected an error for malformed JSON, but got nil")
	} else {
		// Optionally, check for a specific type of error, e.g., json.SyntaxError
		// For now, just checking that an error occurred is sufficient.
		t.Logf("Received expected error for malformed JSON: %v", err)
	}
}

// TestParseMQTTMessage_MissingCriticalFields tests handling of JSON
// that is valid but missing fields critical for processing.
func TestParseMQTTMessage_MissingCriticalFields(t *testing.T) {
	testCases := []struct {
		name        string
		jsonData    []byte
		expectError bool // True if ParseMQTTMessage itself should error, false if we expect downstream logic to catch it
		// For this test, we assume ParseMQTTMessage doesn't error if fields are simply missing,
		// but the resulting struct will have zero values for those fields.
		// Error handling for missing *required* data would typically be in the business logic
		// that consumes the MQTTMessage.
	}{
		{
			name: "Missing device_info",
			jsonData: []byte(`{
				"lorawan_data": {"rssi": -60},
				"raw_payload": "TESTPAYLOAD",
				"message_timestamp": "2023-10-26T11:00:00Z"
			}`),
			expectError: false, // json.Unmarshal won't error, DeviceInfo will be zero-struct
		},
		{
			name: "Missing raw_payload",
			jsonData: []byte(`{
				"device_info": {"device_eui": "AABBCCDDEEFF1122"},
				"message_timestamp": "2023-10-26T11:00:00Z"
			}`),
			expectError: false, // RawPayload will be empty string
		},
		{
			name: "Missing device_eui within device_info",
			jsonData: []byte(`{
				"device_info": {}, 
				"raw_payload": "TESTPAYLOAD",
				"message_timestamp": "2023-10-26T11:00:00Z"
			}`),
			expectError: false, // DeviceInfo.DeviceEUI will be empty string
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := ParseMQTTMessage(tc.jsonData)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected an error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got: %v", err)
				}
				// Further checks can be done here based on how your application
				// defines "critical". For example, the function that *uses* MQTTMessage
				// would check if msg.DeviceInfo.DeviceEUI is empty.
				if msg == nil && err == nil { // Should not happen if no error
					t.Errorf("Expected a non-nil message when no error occurred")
				}
				if msg != nil {
					if strings.Contains(tc.name, "Missing device_eui") && msg.DeviceInfo.DeviceEUI != "" {
						t.Errorf("Expected DeviceEUI to be empty, got '%s'", msg.DeviceInfo.DeviceEUI)
					}
					if strings.Contains(tc.name, "Missing raw_payload") && msg.RawPayload != "" {
						t.Errorf("Expected RawPayload to be empty, got '%s'", msg.RawPayload)
					}
				}

			}
		})
	}
}
