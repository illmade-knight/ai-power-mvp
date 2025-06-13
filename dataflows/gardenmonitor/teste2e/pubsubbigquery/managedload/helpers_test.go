//go:build integration

package managedload_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/illmade-knight/go-iot/pkg/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// gardenMonitorPayloadGenerator is a stateful payload generator.
// It implements the loadgen.PayloadGenerator interface and maintains
// the state of a device's telemetry between calls.
type gardenMonitorPayloadGenerator struct {
	mu               sync.Mutex // Protects the fields below, ensuring thread safety.
	isInitialized    bool
	sequence         *loadgen.Sequence
	lastBattery      int
	lastTemp         int
	lastHumidity     int
	lastMoisture     int
	lastWaterFlow    int
	lastWaterQuality int
	lastTankLevel    int
	lastAmbientLight int
}

// GeneratePayload creates a new GardenMonitorReadings based on the previous state.
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

	if g.lastBattery > 0 {
		g.lastBattery--
	}
	g.lastTemp += rand.Intn(3) - 1
	g.lastHumidity += rand.Intn(5) - 2
	g.lastMoisture += rand.Intn(3) - 1
	g.lastTankLevel--
	if g.lastTankLevel < 10 {
		g.lastTankLevel = 100
	}
	if rand.Float32() < 0.1 {
		g.lastWaterFlow = rand.Intn(50)
	} else {
		g.lastWaterFlow = 0
	}

	payload := types.GardenMonitorReadings{
		DE:           device.ID,
		SIM:          "sim-8675309",
		RSSI:         "-75dBm",
		Version:      "1.1.0",
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

// respaceTimestampsForDemo fetches all readings for a set of devices and updates their
// timestamps to be staggered by a variable duration, making them easier to visualize.
func respaceTimestampsForDemo(t *testing.T, ctx context.Context, bqClient *bigquery.Client, projectID, datasetID, tableID string, deviceIDs []string) {
	t.Helper()
	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("Starting timestamp respacing for demo...")

	// 1. Fetch all rows for the given devices to determine the update order.
	query := bqClient.Query(fmt.Sprintf("SELECT uid, timestamp FROM `%s.%s.%s` WHERE uid IN UNNEST(@devices)", projectID, datasetID, tableID))
	query.Parameters = []bigquery.QueryParameter{{Name: "devices", Value: deviceIDs}}

	it, err := query.Read(ctx)
	require.NoError(t, err, "Failed to query rows for respacing")

	type deviceReading struct {
		DeviceID          string
		OriginalTimestamp time.Time
	}

	var allReadings []deviceReading
	for {
		var row struct {
			DE        string
			Timestamp time.Time
		}

		if irr := it.Next(&row); errors.Is(irr, iterator.Done) {
			break
		}
		require.NoError(t, err, "Failed to iterate rows")
		allReadings = append(allReadings, deviceReading{
			DeviceID:          row.DE,
			OriginalTimestamp: row.Timestamp,
		})
	}

	if len(allReadings) == 0 {
		log.Warn().Msg("No rows found to respace.")
		return
	}

	// Group readings by device
	readingsByDevice := make(map[string][]deviceReading)
	for _, reading := range allReadings {
		readingsByDevice[reading.DeviceID] = append(readingsByDevice[reading.DeviceID], reading)
	}

	baseTime := time.Now().Add(-2 * time.Hour)

	// 2. Iterate through each device, sort its readings, and execute DML UPDATE statements.
	for deviceID, readings := range readingsByDevice {
		log.Info().Str("device_id", deviceID).Int("reading_count", len(readings)).Msg("Respacing timestamps for device")

		// Sort this device's readings by their original timestamp to ensure correct order.
		sort.Slice(readings, func(i, j int) bool {
			return readings[i].OriginalTimestamp.Before(readings[j].OriginalTimestamp)
		})

		currentTime := baseTime
		for _, reading := range readings {
			// Construct and execute a DML UPDATE statement for each row.
			updateQuery := bqClient.Query(fmt.Sprintf(
				"UPDATE `%s.%s.%s` SET Timestamp = @new_timestamp WHERE uid = @device_id AND timestamp = @original_timestamp",
				projectID, datasetID, tableID,
			))
			updateQuery.Parameters = []bigquery.QueryParameter{
				{Name: "new_timestamp", Value: currentTime},
				{Name: "device_id", Value: reading.DeviceID},
				{Name: "original_timestamp", Value: reading.OriginalTimestamp},
			}

			job, err := updateQuery.Run(ctx)
			require.NoError(t, err, "Failed to run update DML job")

			status, err := job.Wait(ctx)
			require.NoError(t, err, "Failed to wait for update DML job completion")
			if status != nil {
				require.NoError(t, status.Err(), "DML job failed with an error")
			}

			// Increment the time for the next reading for this device.
			jitter := time.Duration(16+rand.Intn(7)) * time.Second // 16-22 seconds
			currentTime = currentTime.Add(jitter)
		}
	}

	log.Info().Msg("Timestamp respacing complete.")
}
