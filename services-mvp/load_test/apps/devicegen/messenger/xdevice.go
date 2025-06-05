package messenger

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
)

const expectedXDevicePayloadLengthBytes = 24

// XDeviceDecodedPayload mirrors the structure from your xdevice.decoder.go.
type XDeviceDecodedPayload struct {
	UID            string  `json:"uid"`
	Reading        float32 `json:"reading"`
	AverageCurrent float32 `json:"averageCurrent"`
	MaxCurrent     float32 `json:"maxCurrent"`
	MaxVoltage     float32 `json:"maxVoltage"`
	AverageVoltage float32 `json:"averageVoltage"`
}

// generateXDeviceRawPayloadBytes creates a byte slice representing the XDevice payload.
func generateXDeviceRawPayloadBytes(d string) []byte {
	payload := XDeviceDecodedPayload{
		UID:            fmt.Sprintf("DV%s", d),
		Reading:        rand.Float32() * 100,
		AverageCurrent: rand.Float32() * 10,
		MaxCurrent:     rand.Float32() * 15,
		MaxVoltage:     rand.Float32() * 250,
		AverageVoltage: rand.Float32() * 240,
	}
	if len(payload.UID) > 4 {
		payload.UID = payload.UID[:4]
	} else {
		payload.UID = fmt.Sprintf("%-4s", payload.UID)
	}

	buf := make([]byte, expectedXDevicePayloadLengthBytes)
	offset := 0
	copy(buf[offset:offset+4], payload.UID)
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.Reading))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.AverageCurrent))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.MaxCurrent))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.MaxVoltage))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.AverageVoltage))
	return buf
}
