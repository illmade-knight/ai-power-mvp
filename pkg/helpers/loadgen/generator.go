// loadgen/loadgen.go

package loadgen

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Device represents a single simulated device in the load test.
// It is generic and uses a PayloadGenerator to create its payload.
type Device struct {
	ID               string
	MessageRate      float64
	PayloadGenerator PayloadGenerator
}

// LoadGenerator orchestrates the load test.
// It manages the devices and the client, and runs the test for a specified duration.
type LoadGenerator struct {
	client  Client
	devices []*Device
	logger  zerolog.Logger
}

// NewLoadGenerator creates a new LoadGenerator.
func NewLoadGenerator(client Client, devices []*Device, logger zerolog.Logger) *LoadGenerator {
	return &LoadGenerator{
		client:  client,
		devices: devices,
		logger:  logger,
	}
}

// Run starts the load generation process.
// It starts a goroutine for each device to publish messages at the specified rate.
func (lg *LoadGenerator) Run(ctx context.Context, duration time.Duration) error {
	lg.logger.Info().Int("num_devices", len(lg.devices)).Dur("duration", duration).Msg("Starting load generator")

	if err := lg.client.Connect(); err != nil {
		lg.logger.Error().Err(err).Msg("Failed to connect client")
		return err
	}
	defer lg.client.Disconnect()

	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	for _, device := range lg.devices {
		wg.Add(1)
		go func(d *Device) {
			defer wg.Done()
			lg.runDevice(ctx, d)
		}(device)
	}

	wg.Wait()
	lg.logger.Info().Msg("Load generator finished")
	return nil
}

// runDevice runs the message publishing loop for a single device.
func (lg *LoadGenerator) runDevice(ctx context.Context, device *Device) {
	if device.MessageRate <= 0 {
		lg.logger.Warn().Str("device_id", device.ID).Msg("Device has a message rate of 0, no messages will be sent")
		return
	}

	interval := time.Duration(float64(time.Second) / device.MessageRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	lg.logger.Info().Str("device_id", device.ID).Float64("rate_hz", device.MessageRate).Dur("interval", interval).Msg("Device starting")

	for {
		select {
		case <-ctx.Done():
			lg.logger.Info().Str("device_id", device.ID).Msg("Device stopping")
			return
		case <-ticker.C:
			if err := lg.client.Publish(ctx, device); err != nil {
				lg.logger.Error().Err(err).Str("device_id", device.ID).Msg("Failed to publish message")
			}
		}
	}
}
