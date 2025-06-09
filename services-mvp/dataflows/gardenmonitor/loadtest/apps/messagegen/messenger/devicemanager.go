package messenger

import (
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/ingestion/converter"
	"github.com/rs/zerolog"
	"math/rand"
)

// MessageConfig we've reduced this to just the rate but leave for the moment
type MessageConfig struct {
	messagesPerSecond float64
}

type MessageDelivery struct {
	ExpectedCount int
	Published     int
}

// Device represents a single simulated MQTT device.
type Device struct {
	eui         string
	messageRate float64
	state       DeviceState
	delivery    MessageDelivery
}

func (d *Device) MessageRate() float64 {
	return d.messageRate
}

func (d *Device) GetEUI() string {
	return d.eui
}

// DeviceState holds the dynamic state for a single simulated garden monitor.
// This is analogous to the state that would be held in a real device's memory.
type DeviceState struct {
	Sequence     int
	Battery      int
	Temperature  int
	Humidity     int
	SoilMoisture int
	RSSI         int
}

// NewDevice creates and initializes a new simulated device.
// It no longer takes fsClient directly. Seeding is orchestrated by DeviceGenerator.
func NewDevice(eui string, rate float64) *Device {
	d := &Device{
		eui:         eui,
		messageRate: rate,
		state: DeviceState{
			Sequence:     0,
			Battery:      rand.Intn(21) + 80,        // Start between 80-100%
			Temperature:  rand.Intn(15) + 10,        // Start between 10-25°C
			Humidity:     rand.Intn(30) + 40,        // Start between 40-70%
			SoilMoisture: rand.Intn(400) + 300,      // Start between 300-700
			RSSI:         (rand.Intn(40) + 50) * -1, // Start between -50 to -90 dBm
		},
	}
	return d
}

// GeneratePayload creates the next payload for a given device, updating its state.
// This function produces the raw JSON bytes the ingestion service expects,
// rather than bytes for hex-encoding as in your messenger.publisher.
func (d *Device) GeneratePayload() ([]byte, error) {

	// --- Update device state for the next message ---
	d.state.Sequence++
	if d.state.Battery > 10 {
		d.state.Battery -= rand.Intn(2) // Decrease by 0 or 1
	}
	d.state.Temperature += rand.Intn(3) - 1    // Fluctuate by -1, 0, or 1
	d.state.Humidity += rand.Intn(5) - 2       // Fluctuate by -2 to +2
	d.state.SoilMoisture += rand.Intn(41) - 20 // Fluctuate by -20 to +20
	if d.state.SoilMoisture < 100 {
		d.state.SoilMoisture = 100
	}
	if d.state.SoilMoisture > 900 {
		d.state.SoilMoisture = 900
	}

	// --- Create the payload from the new state ---
	payload := converter.GardenMonitorPayload{
		DE:           d.eui,
		SIM:          fmt.Sprintf("SIM_LOAD_%s", d.eui[len(d.eui)-4:]),
		RSSI:         fmt.Sprintf("%ddBm", d.state.RSSI),
		Version:      "1.3.0-loadtest",
		Sequence:     d.state.Sequence,
		Battery:      d.state.Battery,
		Temperature:  d.state.Temperature,
		Humidity:     d.state.Humidity,
		SoilMoisture: d.state.SoilMoisture,
		WaterFlow:    rand.Intn(5),
		WaterQuality: rand.Intn(100) + 700,
		TankLevel:    rand.Intn(101),
		AmbientLight: rand.Intn(20000),
	}

	return json.Marshal(payload)
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
	logger   zerolog.Logger
	Devices  []*Device
}

func NewDeviceGenerator(cfg *DeviceGeneratorConfig, parentLogger zerolog.Logger) (*DeviceGenerator, error) {
	logger := parentLogger.With().Str("component", "DeviceGenerator").Logger()

	return &DeviceGenerator{
		cfg:    cfg,
		logger: logger,
	}, nil
}

func (dg *DeviceGenerator) CreateDevices() {
	dg.Devices = make([]*Device, dg.cfg.NumDevices)
	for i := 0; i < dg.cfg.NumDevices; i++ {
		eui := fmt.Sprintf("LOADTEST-%06d", i)

		dev := NewDevice(eui, dg.cfg.MsgRatePerDevice)

		dg.Devices[i] = dev
	}
}
