//go:build integration

package main_test

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/helpers/loadgen"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
)

// gardenMonitorPayloadGenerator is a stateful payload generator.
// It implements the loadgen.PayloadGenerator interface and maintains
// the state of a device's telemetry between calls.
// It lazy-initializes its state on the first call to GeneratePayload.
type gardenMonitorPayloadGenerator struct {
	mu            sync.Mutex // Protects the fields below, ensuring thread safety.
	isInitialized bool
	sequence      *loadgen.Sequence

	// State for all telemetry fields from the real GardenMonitorPayload
	lastBattery      int
	lastTemp         int
	lastHumidity     int
	lastMoisture     int
	lastWaterFlow    int
	lastWaterQuality int
	lastTankLevel    int
	lastAmbientLight int
}

// GeneratePayload creates a new GardenMonitorPayload based on the previous state.
// It uses the correct, complete struct definition and generates data accordingly.
func (g *gardenMonitorPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Lazy initialization on the first run.
	if !g.isInitialized {
		g.sequence = loadgen.NewSequence(0)
		g.lastBattery = 100
		g.lastTemp = 15 + rand.Intn(10)
		g.lastHumidity = 40 + rand.Intn(20)
		g.lastMoisture = 30 + rand.Intn(20)
		g.lastWaterFlow = 0
		g.lastWaterQuality = 95 + rand.Intn(5)
		g.lastTankLevel = 80 + rand.Intn(20)
		g.lastAmbientLight = 500 + rand.Intn(200)
		g.isInitialized = true
	}

	// --- Generate new values as a delta from the last state ---

	// Battery slowly depletes.
	if g.lastBattery > 0 {
		g.lastBattery--
	}

	// Other telemetry fluctuates randomly in a small range.
	g.lastTemp += rand.Intn(3) - 1     // +/- 1
	g.lastHumidity += rand.Intn(5) - 2 // +/- 2
	g.lastMoisture += rand.Intn(3) - 1 // +/- 1
	g.lastTankLevel -= 1               // Slowly depletes
	if g.lastTankLevel < 10 {
		g.lastTankLevel = 100 // Magically refilled!
	}
	// Occasional water flow burst
	if rand.Float32() < 0.1 {
		g.lastWaterFlow = rand.Intn(50)
	} else {
		g.lastWaterFlow = 0
	}

	// Create the payload with the new state, matching the real struct.
	payload := types.GardenMonitorPayload{
		DE:           device.ID,
		SIM:          "sim-8675309", // Static value for test
		RSSI:         "-75dBm",      // Static value for test
		Version:      "1.1.0",       // Static value for test
		Sequence:     int(g.sequence.Next()),
		Battery:      g.lastBattery,
		Temperature:  g.lastTemp,
		Humidity:     g.lastHumidity,
		SoilMoisture: g.lastMoisture,
		WaterFlow:    g.lastWaterFlow,
		WaterQuality: g.lastWaterQuality,
		TankLevel:    g.lastTankLevel,
		AmbientLight: g.lastAmbientLight,
		Timestamp:    time.Now().UTC(),
	}

	return json.Marshal(payload)
}
