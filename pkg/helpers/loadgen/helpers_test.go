package loadgen_test

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestGardenMonitorPayloadGenerator(t *testing.T) {
	eui := "test-eui-01"
	gen := NewGardenMonitorPayloadGenerator(eui)

	t.Run("GeneratePayload returns valid JSON", func(t *testing.T) {
		// Act
		payloadBytes, err := gen.GeneratePayload()

		// Assert
		require.NoError(t, err)
		assert.True(t, json.Valid(payloadBytes), "Payload should be valid JSON")

		var payload GardenMonitorPayload
		err = json.Unmarshal(payloadBytes, &payload)
		require.NoError(t, err)

		assert.Equal(t, eui, payload.DE)
	})

	t.Run("State is updated after generating payload", func(t *testing.T) {
		// Arrange
		initialSequence := gen.state.Sequence

		// Act
		_, err := gen.GeneratePayload()

		// Assert
		require.NoError(t, err)
		assert.Equal(t, initialSequence+1, gen.state.Sequence, "Sequence number should be incremented")

		// Act again
		_, err = gen.GeneratePayload()
		require.NoError(t, err)
		assert.Equal(t, initialSequence+2, gen.state.Sequence, "Sequence number should be incremented again")
	})
}

// GardenMonitorPayload represents the specific payload for the garden monitor devices.
type GardenMonitorPayload struct {
	DE           string `json:"de"`
	SIM          string `json:"sim"`
	RSSI         string `json:"rssi"`
	Version      string `json:"version"`
	Sequence     int    `json:"sequence"`
	Battery      int    `json:"battery"`
	Temperature  int    `json:"temperature"`
	Humidity     int    `json:"humidity"`
	SoilMoisture int    `json:"soil_moisture"`
}

// GardenMonitorPayloadGenerator implements the lib.PayloadGenerator interface.
type GardenMonitorPayloadGenerator struct {
	eui   string
	state deviceState
}

// deviceState holds the dynamic state for a single simulated garden monitor.
type deviceState struct {
	Sequence     int
	Battery      int
	Temperature  int
	Humidity     int
	SoilMoisture int
	RSSI         int
}

// NewGardenMonitorPayloadGenerator creates a new generator for garden monitor payloads.
func NewGardenMonitorPayloadGenerator(eui string) *GardenMonitorPayloadGenerator {
	return &GardenMonitorPayloadGenerator{
		eui: eui,
		state: deviceState{
			Sequence:     0,
			Battery:      rand.Intn(21) + 80,        // Start between 80-100%
			Temperature:  rand.Intn(15) + 10,        // Start between 10-25°C
			Humidity:     rand.Intn(30) + 40,        // Start between 40-70%
			SoilMoisture: rand.Intn(400) + 300,      // Start between 300-700
			RSSI:         (rand.Intn(40) + 50) * -1, // Start between -50 to -90 dBm
		},
	}
}

// GeneratePayload creates the next payload for a garden monitor device, updating its state.
func (g *GardenMonitorPayloadGenerator) GeneratePayload() ([]byte, error) {
	// Update device state for the next message
	g.state.Sequence++
	if g.state.Battery > 10 {
		g.state.Battery -= rand.Intn(2) // Decrease by 0 or 1
	}
	g.state.Temperature += rand.Intn(3) - 1    // Fluctuate by -1, 0, or 1
	g.state.Humidity += rand.Intn(5) - 2       // Fluctuate by -2 to +2
	g.state.SoilMoisture += rand.Intn(41) - 20 // Fluctuate by -20 to +20
	if g.state.SoilMoisture < 100 {
		g.state.SoilMoisture = 100
	}
	if g.state.SoilMoisture > 900 {
		g.state.SoilMoisture = 900
	}

	// Create the payload from the new state
	payload := GardenMonitorPayload{
		DE:           g.eui,
		SIM:          fmt.Sprintf("SIM_LOAD_%s", g.eui[len(g.eui)-4:]),
		RSSI:         fmt.Sprintf("%ddBm", g.state.RSSI),
		Version:      "1.3.0-loadtest",
		Sequence:     g.state.Sequence,
		Battery:      g.state.Battery,
		Temperature:  g.state.Temperature,
		Humidity:     g.state.Humidity,
		SoilMoisture: g.state.SoilMoisture,
	}

	return json.Marshal(payload)
}
