package xdevice

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a valid hex payload for testing.
// UID is now a string, expected to be 4 characters long.
func createValidTestHexPayload(uid string, reading, avgCurrent, maxCurrent, maxVoltage, avgVoltage float32) (string, error) {
	if len(uid) != 4 {
		return "", fmt.Errorf("test UID must be 4 characters long, got %d", len(uid))
	}
	buf := make([]byte, expectedPayloadLengthBytes)
	offset := 0

	// Copy UID string bytes
	copy(buf[offset:offset+4], []byte(uid))
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(reading))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(avgCurrent))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(maxCurrent))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(maxVoltage))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(avgVoltage))

	return hex.EncodeToString(buf), nil
}

func TestDecodeXDevicePayload_ValidPayload(t *testing.T) {
	expectedUID := "XUID" // 4-character string UID
	expectedReading := float32(25.67)
	expectedAvgCurrent := float32(1.23)
	expectedMaxCurrent := float32(2.55)
	expectedMaxVoltage := float32(240.5)
	expectedAvgVoltage := float32(230.1)

	hexPayload, err := createValidTestHexPayload(
		expectedUID,
		expectedReading,
		expectedAvgCurrent,
		expectedMaxCurrent,
		expectedMaxVoltage,
		expectedAvgVoltage,
	)
	require.NoError(t, err, "Failed to create test hex payload")

	decoded, err := DecodePayload(hexPayload)
	require.NoError(t, err, "DecodePayload failed for a valid payload")
	require.NotNil(t, decoded, "Decoded payload should not be nil for a valid input")

	assert.Equal(t, expectedUID, decoded.UID, "UID mismatch")
	assert.InDelta(t, expectedReading, decoded.Reading, 0.001, "Reading mismatch")
	assert.InDelta(t, expectedAvgCurrent, decoded.AverageCurrent, 0.001, "AverageCurrent mismatch")
	assert.InDelta(t, expectedMaxCurrent, decoded.MaxCurrent, 0.001, "MaxCurrent mismatch")
	assert.InDelta(t, expectedMaxVoltage, decoded.MaxVoltage, 0.001, "MaxVoltage mismatch")
	assert.InDelta(t, expectedAvgVoltage, decoded.AverageVoltage, 0.001, "AverageVoltage mismatch")
}

func TestDecodeXDevicePayload_InvalidHex(t *testing.T) {
	testCases := []struct {
		name        string
		hexPayload  string
		expectedErr string
	}{
		{
			name:        "Odd length hex string",
			hexPayload:  "010203040",
			expectedErr: "failed to decode hex payload",
		},
		{
			name:        "Non-hex characters",
			hexPayload:  "010203040506070G",
			expectedErr: "failed to decode hex payload",
		},
		{
			name:        "Empty string",
			hexPayload:  "",
			expectedErr: "invalid payload length",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := DecodePayload(tc.hexPayload)
			require.Error(t, err, "Expected an error for invalid hex payload")
			assert.Nil(t, decoded, "Decoded payload should be nil on error")
			assert.Contains(t, err.Error(), tc.expectedErr, "Error message mismatch")
		})
	}
	_, errEmpty := DecodePayload("")
	require.Error(t, errEmpty)
	assert.Contains(t, errEmpty.Error(), "invalid payload length")
}

func TestDecodeXDevicePayload_InvalidLength(t *testing.T) {
	testCases := []struct {
		name        string
		byteLength  int
		expectedErr string
	}{
		{
			name:        "Payload too short",
			byteLength:  expectedPayloadLengthBytes - 1,
			expectedErr: "invalid payload length",
		},
		{
			name:        "Payload too long",
			byteLength:  expectedPayloadLengthBytes + 1,
			expectedErr: "invalid payload length",
		},
		{
			name:        "Zero length payload (after hex decoding)",
			byteLength:  0,
			expectedErr: "invalid payload length",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hexPayload := hex.EncodeToString(make([]byte, tc.byteLength))
			if tc.byteLength == 0 {
				hexPayload = ""
			}
			decoded, err := DecodePayload(hexPayload)
			require.Error(t, err, "Expected an error for payload with invalid length")
			assert.Nil(t, decoded, "Decoded payload should be nil on error")
			assert.Contains(t, err.Error(), tc.expectedErr, "Error message mismatch")
		})
	}
}

func TestDecodeXDevicePayload_EdgeFloatValues(t *testing.T) {
	uid := "EDGE" // 4-character string UID
	payloadZero, _ := createValidTestHexPayload(uid, 0.0, 0.0, 0.0, 0.0, 0.0)
	payloadNegative, _ := createValidTestHexPayload(uid, -123.45, -0.5, -10.0, -300.0, -200.0)
	nan := math.Float32frombits(0x7FC00000)
	inf := math.Float32frombits(0x7F800000)
	payloadNaN, _ := createValidTestHexPayload(uid, nan, 1.0, 2.0, 3.0, 4.0)
	payloadInf, _ := createValidTestHexPayload(uid, inf, 1.0, 2.0, 3.0, 4.0)

	t.Run("ZeroValues", func(t *testing.T) {
		decoded, err := DecodePayload(payloadZero)
		require.NoError(t, err)
		assert.Equal(t, float32(0.0), decoded.Reading)
		assert.Equal(t, uid, decoded.UID)
	})

	t.Run("NegativeValues", func(t *testing.T) {
		decoded, err := DecodePayload(payloadNegative)
		require.NoError(t, err)
		assert.Equal(t, float32(-123.45), decoded.Reading)
		assert.Equal(t, uid, decoded.UID)
	})

	t.Run("NaNValue", func(t *testing.T) {
		decoded, err := DecodePayload(payloadNaN)
		require.NoError(t, err)
		assert.True(t, math.IsNaN(float64(decoded.Reading)), "Expected Reading to be NaN")
		assert.Equal(t, uid, decoded.UID)
	})

	t.Run("InfValue", func(t *testing.T) {
		decoded, err := DecodePayload(payloadInf)
		require.NoError(t, err)
		assert.True(t, math.IsInf(float64(decoded.Reading), 1), "Expected Reading to be +Inf")
		assert.Equal(t, uid, decoded.UID)
	})
}

func TestCreateValidTestHexPayload_UIDLength(t *testing.T) {
	t.Run("UID too short", func(t *testing.T) {
		_, err := createValidTestHexPayload("ABC", 1, 1, 1, 1, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "test UID must be 4 characters long")
	})
	t.Run("UID too long", func(t *testing.T) {
		_, err := createValidTestHexPayload("ABCDE", 1, 1, 1, 1, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "test UID must be 4 characters long")
	})
}
