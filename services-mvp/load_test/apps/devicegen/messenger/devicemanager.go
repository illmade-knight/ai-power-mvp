package messenger

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

// FirestoreDeviceData represents the data to be stored in Firestore for a device.
type FirestoreDeviceData struct {
	ClientID       string `firestore:"clientID"`
	LocationID     string `firestore:"locationID"`
	DeviceCategory string `firestore:"deviceCategory"`
}

// MessageConfig we've reduced this to just the rate but leave for the moment
type MessageConfig struct {
	messagesPerSecond float64
}

type PayloadGenerator func(in string) []byte

type MessageDelivery struct {
	ExpectedCount int
	Published     int
}

// Device represents a single simulated MQTT device.
type Device struct {
	eui          string
	messageRate  float64
	payloadMaker PayloadGenerator
	delivery     MessageDelivery
}

func (d *Device) MessageRate() float64 {
	return d.messageRate
}

func (d *Device) GetEUI() string {
	return d.eui
}

// NewDevice creates and initializes a new simulated device.
// It no longer takes fsClient directly. Seeding is orchestrated by DeviceGenerator.
func NewDevice(eui string, rate float64, generator PayloadGenerator) *Device {
	d := &Device{
		eui:          eui,
		messageRate:  rate,
		payloadMaker: generator,
	}
	return d
}

type DeviceGeneratorConfig struct {
	NumDevices       int     `yaml:"numDevices"`
	MsgRatePerDevice float64 `yaml:"msgRatePerDevice"`

	FirestoreEmulatorHost string `yaml:"firestoreEmulatorHost"`
	FirestoreProjectID    string `yaml:"firestoreProjectID"`
	FirestoreCollection   string `yaml:"firestoreCollection"`
}

type DeviceGenerator struct {
	cfg      *DeviceGeneratorConfig
	mqttConn mqtt.Client
	fsClient *firestore.Client
	logger   zerolog.Logger
	Devices  []*Device
}

func NewDeviceGenerator(cfg *DeviceGeneratorConfig, fsClient *firestore.Client, parentLogger zerolog.Logger) (*DeviceGenerator, error) {
	logger := parentLogger.With().Str("component", "DeviceGenerator").Logger()

	return &DeviceGenerator{
		cfg:      cfg,
		fsClient: fsClient,
		logger:   logger,
	}, nil
}

func (dg *DeviceGenerator) CreateDevices() {
	dg.Devices = make([]*Device, dg.cfg.NumDevices)
	for i := 0; i < dg.cfg.NumDevices; i++ {
		eui := fmt.Sprintf("LOADTEST-%06d", i)

		dev := NewDevice(eui, dg.cfg.MsgRatePerDevice, generateXDeviceRawPayloadBytes)

		dg.Devices[i] = dev
	}
}

func (dg *DeviceGenerator) SeedDevices(ctx context.Context) error {
	// Seed Firestore for all Devices first if fsClient is available
	if dg.cfg.FirestoreProjectID != "" && dg.cfg.FirestoreCollection != "" {
		dg.logger.Info().Msg("Seeding Firestore for all Devices...")
		var seedWg sync.WaitGroup
		for i, dev := range dg.Devices {
			seedWg.Add(1)
			go func(d *Device) {
				defer seedWg.Done()
				// Use a short-lived context for each seed operation, derived from the main run context
				// to allow individual seed operations to timeout without halting all if one device has an issue.
				seedCtx, seedCancel := context.WithTimeout(ctx, 15*time.Second)
				defer seedCancel()

				deviceData := FirestoreDeviceData{
					ClientID:       fmt.Sprintf("Client-Load-%06d", i),
					LocationID:     fmt.Sprintf("Location-Load-%03d", i%100),
					DeviceCategory: fmt.Sprintf("SensorType-%s", []string{"Alpha", "Beta", "Gamma"}[i%3]),
				}
				dg.logger.Debug().Str("collection", dg.cfg.FirestoreCollection).Interface("data_to_seed", deviceData).Msg("Attempting to seed device in Firestore")

				docRef := dg.fsClient.Collection(dg.cfg.FirestoreCollection).Doc(d.eui)
				_, err := docRef.Set(seedCtx, deviceData)
				if err != nil {
					dg.logger.Error().Err(err).Str("collection", dg.cfg.FirestoreCollection).Interface("data_attempted", deviceData).Msg("Firestore Set operation failed for device")
				}
				dg.logger.Debug().Str("collection", dg.cfg.FirestoreCollection).Msg("Successfully seeded/updated device in Firestore")
			}(dev)
		}
		seedWg.Wait()
		dg.logger.Info().Msg("Firestore seeding phase complete.")
	} else {
		dg.logger.Info().Msg("Firestore client or config not provided to DeviceGenerator, skipping Firestore seeding.")
	}

	return nil
}
