package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	// Using the messenger package as provided by the user.
	// Ensure the path is correct for your project structure.
	"load_test/apps/messagegen/messenger"
)

// Config holds the configuration for the load generator.
type Config struct {
	BrokerURL        string
	TopicPattern     string
	NumDevices       int
	MsgRatePerDevice float64 // Changed from MsgPerSecond (total)
	TestDuration     time.Duration
	ClientIDPrefix   string
	LogLevel         string
}

func main() {
	// --- Configuration via Command-Line Flags ---
	cfg := Config{}
	flag.StringVar(&cfg.BrokerURL, "broker", "tcp://localhost:1883", "MQTT broker URL")
	flag.StringVar(&cfg.TopicPattern, "topic", "devices/garden-monitor/+/telemetry", "MQTT topic pattern with '+' for device UID")
	flag.IntVar(&cfg.NumDevices, "devices", 10, "Number of concurrent devices to simulate")
	flag.Float64Var(&cfg.MsgRatePerDevice, "rate-per-device", 1.0, "Messages per second for each device")
	flag.DurationVar(&cfg.TestDuration, "duration", 1*time.Minute, "Duration of the load test (e.g., 30s, 5m, 1h)")
	flag.StringVar(&cfg.ClientIDPrefix, "client-prefix", "load-gen", "Prefix for MQTT client IDs")
	flag.StringVar(&cfg.LogLevel, "loglevel", "info", "Log level (e.g., debug, info, warn, error)")
	flag.Parse()

	// --- Setup Logger ---
	logLevel, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).Level(logLevel)

	log.Info().
		Str("broker", cfg.BrokerURL).
		Int("devices", cfg.NumDevices).
		Float64("rate_per_device", cfg.MsgRatePerDevice).
		Dur("duration", cfg.TestDuration).
		Msg("Starting load test")

	// --- Setup MQTT Client ---
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(fmt.Sprintf("%s-%s", cfg.ClientIDPrefix, uuid.New().String()[:8])).
		SetConnectTimeout(10 * time.Second)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal().Err(token.Error()).Msg("Failed to connect to MQTT broker")
	}
	log.Info().Str("broker", cfg.BrokerURL).Msg("Successfully connected to MQTT broker")
	defer client.Disconnect(250)

	// --- Initialize Simulated Devices using the provided DeviceManager approach ---
	deviceGenConfig := &messenger.DeviceGeneratorConfig{
		NumDevices:       cfg.NumDevices,
		MsgRatePerDevice: cfg.MsgRatePerDevice,
	}
	deviceGenerator, err := messenger.NewDeviceGenerator(deviceGenConfig, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create device generator")
	}
	deviceGenerator.CreateDevices()
	log.Info().Int("device_count", len(deviceGenerator.Devices)).Msg("Created simulated devices")

	// --- Setup Graceful Shutdown & Test Timer ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Info().Msg("Shutdown signal received, stopping generator...")
		cancel()
	}()

	testTimer := time.NewTimer(cfg.TestDuration)
	go func() {
		<-testTimer.C
		log.Info().Msg("Test duration finished, stopping generator...")
		cancel()
	}()

	// --- Main Simulation Loop ---
	// Launch one goroutine per device for concurrent publishing.
	var wg sync.WaitGroup
	for _, device := range deviceGenerator.Devices {
		wg.Add(1)
		go func(d *messenger.Device) {
			defer wg.Done()
			deviceLogger := log.With().Str("device_eui", d.GetEUI()).Logger()
			topic := strings.Replace(cfg.TopicPattern, "+", d.GetEUI(), 1)
			interval := time.Duration(float64(time.Second) / d.MessageRate())
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			deviceLogger.Info().Float64("rate_hz", d.MessageRate()).Msg("Starting publisher for device")

			for {
				select {
				case <-ticker.C:
					payloadBytes, err := d.GeneratePayload()
					if err != nil {
						deviceLogger.Error().Err(err).Msg("Failed to generate payload")
						continue
					}

					token := client.Publish(topic, 1, false, payloadBytes)
					// Fire-and-forget publish to maximize throughput.
					_ = token // In a real test, you might add token error handling.

				case <-ctx.Done():
					deviceLogger.Info().Msg("Stopping publisher for device")
					return
				}
			}
		}(device)
	}

	log.Info().Msg("Load generator started. Press Ctrl+C to stop early.")
	// Wait until the context is canceled (by timer or signal).
	<-ctx.Done()
	// Wait for all device goroutines to finish.
	wg.Wait()
	log.Info().Msg("Load generator stopped.")
}
