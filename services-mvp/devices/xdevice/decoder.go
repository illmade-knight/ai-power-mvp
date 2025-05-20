package xdevice

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
)

// DecodedPayload represents the structured data decoded from the XDevice's raw payload.
type DecodedPayload struct {
	UID            string  // Unique Identifier (4-byte string)
	Reading        float32 // The primary reading value
	AverageCurrent float32 // Average current
	MaxCurrent     float32 // Maximum current
	MaxVoltage     float32 // Maximum voltage
	AverageVoltage float32 // Average voltage
}

const (
	// Expected byte length of the decoded payload for XDevice.
	// UID (4) + Reading (4) + AvgCurrent (4) + MaxCurrent (4) + MaxVoltage (4) + AvgVoltage (4) = 24 bytes
	expectedPayloadLengthBytes = 24
)

// DecodePayload decodes a raw payload string (assumed to be hex-encoded)
// into an DecodedPayload struct.
func DecodePayload(rawPayloadHex string) (*DecodedPayload, error) {
	// 1. Decode the hex string into a byte slice.
	payloadBytes, err := hex.DecodeString(rawPayloadHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex payload: %w", err)
	}

	// 2. Validate the length of the byte slice.
	if len(payloadBytes) != expectedPayloadLengthBytes {
		return nil, fmt.Errorf("invalid payload length: expected %d bytes, got %d bytes", expectedPayloadLengthBytes, len(payloadBytes))
	}

	// 3. Parse the byte slice according to the XDevice's payload specification.
	decoded := &DecodedPayload{}
	offset := 0

	// UID: first 4 bytes, interpreted as a string
	decoded.UID = string(payloadBytes[offset : offset+4])
	offset += 4

	// Reading: next 4 bytes, float32 (BigEndian)
	bitsReading := binary.BigEndian.Uint32(payloadBytes[offset : offset+4])
	decoded.Reading = math.Float32frombits(bitsReading)
	offset += 4

	// AverageCurrent: next 4 bytes, float32 (BigEndian)
	bitsAvgCurrent := binary.BigEndian.Uint32(payloadBytes[offset : offset+4])
	decoded.AverageCurrent = math.Float32frombits(bitsAvgCurrent)
	offset += 4

	// MaxCurrent: next 4 bytes, float32 (BigEndian)
	bitsMaxCurrent := binary.BigEndian.Uint32(payloadBytes[offset : offset+4])
	decoded.MaxCurrent = math.Float32frombits(bitsMaxCurrent)
	offset += 4

	// MaxVoltage: next 4 bytes, float32 (BigEndian)
	bitsMaxVoltage := binary.BigEndian.Uint32(payloadBytes[offset : offset+4])
	decoded.MaxVoltage = math.Float32frombits(bitsMaxVoltage)
	offset += 4

	// AverageVoltage: next 4 bytes, float32 (BigEndian)
	bitsAvgVoltage := binary.BigEndian.Uint32(payloadBytes[offset : offset+4])
	decoded.AverageVoltage = math.Float32frombits(bitsAvgVoltage)
	// offset += 4 // Not strictly needed for the last field

	return decoded, nil
}
