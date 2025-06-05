package main

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const defaultGeneratorConfigFile = "load_generator_config.yaml"

// GeneratorConfig holds all configuration parameters for the load generator.
type GeneratorConfig struct {
	BrokerURL             string        `yaml:"brokerURL"`
	TopicPattern          string        `yaml:"topicPattern"` // e.g., "devices/{DEVICE_EUI}/up"
	NumDevices            int           `yaml:"numDevices"`
	MsgRatePerDevice      float64       `yaml:"msgRatePerDevice"`
	TestDurationString    string        `yaml:"testDuration"` // e.g., "1m", "30s"
	TestDuration          time.Duration `yaml:"-"`            // Parsed value, ignored by YAML
	QOS                   int           `yaml:"qos"`
	ClientIDPrefix        string        `yaml:"clientIDPrefix"`
	LogLevel              string        `yaml:"logLevel"`
	FirestoreEmulatorHost string        `yaml:"firestoreEmulatorHost"`
	FirestoreProjectID    string        `yaml:"firestoreProjectID"`
	FirestoreCollection   string        `yaml:"firestoreCollection"`
}

// MQTTMessage structure for publishing.
type MQTTMessage struct {
	DeviceInfo       DeviceInfo  `json:"device_info"`
	LoRaWAN          LoRaWANData `json:"lorawan_data,omitempty"`
	RawPayload       string      `json:"raw_payload"`
	MessageTimestamp time.Time   `json:"message_timestamp"`
	ClientMessageID  string      `json:"client_message_id"`
}

type DeviceInfo struct {
	DeviceEUI string `json:"device_eui"`
}

type LoRaWANData struct {
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

// XDeviceDecodedPayload mirrors the structure from your xdevice.decoder.go.
type XDeviceDecodedPayload struct {
	UID            string  `json:"uid"`
	Reading        float32 `json:"reading"`
	AverageCurrent float32 `json:"averageCurrent"`
	MaxCurrent     float32 `json:"maxCurrent"`
	MaxVoltage     float32 `json:"maxVoltage"`
	AverageVoltage float32 `json:"averageVoltage"`
}

const expectedXDevicePayloadLengthBytes = 24

// FirestoreDeviceData represents the data to be stored in Firestore for a device.
type FirestoreDeviceData struct {
	ClientID       string `firestore:"clientID"`
	LocationID     string `firestore:"locationID"`
	DeviceCategory string `firestore:"deviceCategory"`
}

// generateXDeviceRawPayloadBytes creates a byte slice representing the XDevice payload.
func generateXDeviceRawPayloadBytes(deviceIndex int) ([]byte, error) {
	payload := XDeviceDecodedPayload{
		UID:            fmt.Sprintf("DV%02d", deviceIndex%100),
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
	copy(buf[offset:offset+4], []byte(payload.UID))
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
	return buf, nil
}

// --- Device Struct and Methods ---

// Device represents a single simulated MQTT device.
type Device struct {
	id           string // Internal unique MessageID for the device instance (derived from MQTT ClientID)
	eui          string
	mqttClient   mqtt.Client
	cfg          *GeneratorConfig // Reference to global generator config
	logger       zerolog.Logger
	deviceIndex  int // For generating varied data
	connectToken mqtt.Token
}

// NewDevice creates and initializes a new simulated device.
// It no longer takes fsClient directly. Seeding is orchestrated by DeviceGenerator.
func NewDevice(deviceIndex int, cfg *GeneratorConfig, parentLogger zerolog.Logger) (*Device, error) {
	eui := fmt.Sprintf("LOADTEST-%06d", deviceIndex)
	mqttClientID := fmt.Sprintf("%s-%s-%d", cfg.ClientIDPrefix, eui, time.Now().UnixNano()%100000)
	logger := parentLogger.With().Str("deviceEUI", eui).Str("mqttClientID", mqttClientID).Logger()

	d := &Device{
		id:          mqttClientID,
		eui:         eui,
		cfg:         cfg,
		logger:      logger,
		deviceIndex: deviceIndex,
	}
	return d, nil
}

// seedFirestore is now a method of Device, but takes the fsClient from DeviceGenerator.
func (d *Device) seedFirestore(ctx context.Context, fsClient *firestore.Client) error {
	if fsClient == nil {
		d.logger.Debug().Msg("Firestore client not provided to device, skipping seeding.")
		return nil
	}
	if d.cfg.FirestoreProjectID == "" || d.cfg.FirestoreCollection == "" {
		d.logger.Debug().Msg("Firestore project MessageID or collection not configured for device, skipping seeding.")
		return nil
	}

	deviceData := FirestoreDeviceData{
		ClientID:       fmt.Sprintf("Client-Load-%06d", d.deviceIndex),
		LocationID:     fmt.Sprintf("Location-Load-%03d", d.deviceIndex%100),
		DeviceCategory: fmt.Sprintf("SensorType-%s", []string{"Alpha", "Beta", "Gamma"}[d.deviceIndex%3]),
	}
	d.logger.Debug().Str("collection", d.cfg.FirestoreCollection).Interface("data_to_seed", deviceData).Msg("Attempting to seed device in Firestore")

	docRef := fsClient.Collection(d.cfg.FirestoreCollection).Doc(d.eui)
	_, err := docRef.Set(ctx, deviceData)
	if err != nil {
		d.logger.Error().Err(err).Str("collection", d.cfg.FirestoreCollection).Interface("data_attempted", deviceData).Msg("Firestore Set operation failed for device")
		return fmt.Errorf("failed to set device data in Firestore for EUI %s: %w", d.eui, err)
	}
	d.logger.Debug().Str("collection", d.cfg.FirestoreCollection).Msg("Successfully seeded/updated device in Firestore")
	return nil
}

func (d *Device) connectMQTT() error {
	opts := mqtt.NewClientOptions().
		AddBroker(d.cfg.BrokerURL).
		SetClientID(d.id).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			d.logger.Error().Err(err).Msg("MQTT Connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			d.logger.Info().Msg("Connected to MQTT broker")
		})

	d.mqttClient = mqtt.NewClient(opts)
	d.connectToken = d.mqttClient.Connect()
	if d.connectToken.WaitTimeout(10*time.Second) && d.connectToken.Error() != nil {
		err := d.connectToken.Error()
		d.logger.Error().Err(err).Msg("Failed to connect to MQTT broker")
		return err
	}
	return nil
}

func (d *Device) disconnectMQTT() {
	if d.mqttClient != nil && d.mqttClient.IsConnected() {
		d.logger.Debug().Msg("Disconnecting MQTT client")
		d.mqttClient.Disconnect(250)
	}
}

func (d *Device) publishMessages(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer d.disconnectMQTT()

	if err := d.connectMQTT(); err != nil {
		return // Error already logged in connectMQTT
	}

	topic := strings.ReplaceAll(d.cfg.TopicPattern, "{DEVICE_EUI}", d.eui)
	var interval time.Duration
	if d.cfg.MsgRatePerDevice > 0 {
		interval = time.Duration(float64(time.Second) / d.cfg.MsgRatePerDevice)
	} else {
		d.logger.Warn().Msg("Message rate per device is zero or negative, no messages will be published by this device.")
		return
	}

	d.logger.Info().Str("topic", topic).Float64("rate", d.cfg.MsgRatePerDevice).Dur("interval", interval).Dur("device_lifetime_from_config", d.cfg.TestDuration).Msg("Device started publishing")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	msgCount := 0
	publishStartTime := time.Now() // Record when this device actually starts its loop

	for {
		// Check context before blocking on the ticker to allow faster exit if already cancelled
		if ctx.Err() != nil {
			elapsedActual := time.Since(publishStartTime)
			d.logger.Info().Int("total_messages_sent", msgCount).Dur("configured_duration", d.cfg.TestDuration).Dur("actual_elapsed_time", elapsedActual).Msg("Device publisher loop stopping PRE-TICK due to context cancellation.")
			return
		}

		select {
		case <-ctx.Done(): // This context is controlled by GeneratorConfig.TestDuration
			elapsedActual := time.Since(publishStartTime)
			d.logger.Info().Int("total_messages_sent", msgCount).Dur("configured_duration", d.cfg.TestDuration).Dur("actual_elapsed_time", elapsedActual).Msg("Device publisher loop stopping IN-SELECT due to context cancellation.")
			return
		case tickTime := <-ticker.C:
			// Even after a tick, double-check context, as it might have been cancelled
			// while this goroutine was waiting for the tick.
			if ctx.Err() != nil {
				d.logger.Debug().Msg("Context cancelled just before publish attempt after tick, skipping publish.")
				// Loop will terminate on next iteration's select or pre-tick check
				continue
			}

			rawPayloadBytes, err := generateXDeviceRawPayloadBytes(d.deviceIndex)
			if err != nil {
				d.logger.Error().Err(err).Msg("Error generating raw payload bytes")
				continue
			}
			rawPayloadHex := hex.EncodeToString(rawPayloadBytes)
			mqttMsg := MQTTMessage{
				DeviceInfo:       DeviceInfo{DeviceEUI: d.eui},
				RawPayload:       rawPayloadHex,
				MessageTimestamp: time.Now().UTC(),
				ClientMessageID:  uuid.NewString(),
				LoRaWAN:          LoRaWANData{ReceivedAt: time.Now().UTC().Add(-1 * time.Second)},
			}
			jsonData, err := json.Marshal(mqttMsg)
			if err != nil {
				d.logger.Error().Err(err).Msg("Error marshalling MQTT message")
				continue
			}

			d.logger.Debug().Time("tickTime", tickTime).Int("msg_count_attempt", msgCount+1).Msg("Ticker fired, attempting to publish")
			pubToken := d.mqttClient.Publish(topic, byte(d.cfg.QOS), false, jsonData)
			if pubToken.Error() != nil {
				d.logger.Error().Err(pubToken.Error()).Msg("Error publishing message")
			} else {
				msgCount++
				d.logger.Debug().Int("msg_count_sent", msgCount).Msg("Message published successfully")
			}
		}
	}
}

// --- DeviceGenerator Struct and Methods ---

type DeviceGenerator struct {
	cfg      *GeneratorConfig
	devices  []*Device
	fsClient *firestore.Client // Generator holds the single Firestore client
	logger   zerolog.Logger
}

func NewDeviceGenerator(cfg *GeneratorConfig, fsClient *firestore.Client, parentLogger zerolog.Logger) (*DeviceGenerator, error) {
	logger := parentLogger.With().Str("component", "DeviceGenerator").Logger()
	devices := make([]*Device, cfg.NumDevices)
	for i := 0; i < cfg.NumDevices; i++ {
		// Device constructor no longer takes fsClient
		dev, err := NewDevice(i, cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create device %d: %w", i, err)
		}
		devices[i] = dev
	}
	return &DeviceGenerator{
		cfg:      cfg,
		devices:  devices,
		fsClient: fsClient, // Generator holds the client
		logger:   logger,
	}, nil
}

func (dg *DeviceGenerator) Run(ctx context.Context) { // ctx here is runDurationCtx
	dg.logger.Info().Int("num_devices", len(dg.devices)).Dur("target_duration", dg.cfg.TestDuration).Msg("DeviceGenerator starting all devices...")
	var wg sync.WaitGroup

	// Seed Firestore for all devices first if fsClient is available
	if dg.fsClient != nil && dg.cfg.FirestoreProjectID != "" && dg.cfg.FirestoreCollection != "" {
		dg.logger.Info().Msg("Seeding Firestore for all devices...")
		var seedWg sync.WaitGroup
		for _, dev := range dg.devices {
			seedWg.Add(1)
			go func(d *Device) {
				defer seedWg.Done()
				// Use a short-lived context for each seed operation, derived from the main run context
				// to allow individual seed operations to timeout without halting all if one device has an issue.
				seedCtx, seedCancel := context.WithTimeout(ctx, 15*time.Second)
				defer seedCancel()
				// Call seedFirestore method on the device, passing the generator's fsClient
				if err := d.seedFirestore(seedCtx, dg.fsClient); err != nil { // Pass dg.fsClient here
					d.logger.Error().Err(err).Msg("Failed to seed device during batch seeding")
				}
			}(dev)
		}
		seedWg.Wait()
		dg.logger.Info().Msg("Firestore seeding phase complete.")
	} else {
		dg.logger.Info().Msg("Firestore client or config not provided to DeviceGenerator, skipping Firestore seeding.")
	}

	// Launch publishing goroutines
	for _, dev := range dg.devices {
		wg.Add(1)
		go dev.publishMessages(ctx, &wg) // Pass the runDurationCtx
		if len(dg.devices) > 10 && dev.deviceIndex%10 == 0 {
			time.Sleep(time.Duration(rand.IntN(50)+50) * time.Millisecond)
		}
	}

	dg.logger.Info().Msg("All device publishing goroutines launched.")
	wg.Wait() // Wait for all device.publishMessages goroutines to complete
	dg.logger.Info().Msg("All device publishing goroutines have finished (due to context cancellation or completion).")
}

// --- Main Application Logic ---

func loadConfigFromYAMLFile(filePath string, cfg *GeneratorConfig) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		if filePath == defaultGeneratorConfigFile {
			log.Info().Str("file", filePath).Msg("Default config file not found, using built-in defaults and command-line flags.")
			return nil
		}
		return fmt.Errorf("specified config file '%s' not found", filePath)
	}
	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading YAML config file '%s': %w", filePath, err)
	}
	err = yaml.Unmarshal(yamlFile, cfg)
	if err != nil {
		return fmt.Errorf("error unmarshalling YAML config from '%s': %w", filePath, err)
	}
	log.Info().Str("file", filePath).Msg("Configuration loaded from YAML file.")
	return nil
}

func isFlagSet(name string) bool {
	wasSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			wasSet = true
		}
	})
	return wasSet
}

func main() {
	codeDefaultCfg := GeneratorConfig{
		BrokerURL:             "tcp://localhost:1883",
		TopicPattern:          "devices/{DEVICE_EUI}/up",
		NumDevices:            10,
		MsgRatePerDevice:      1.0,
		TestDurationString:    "1m",
		QOS:                   0,
		ClientIDPrefix:        "loadgen",
		LogLevel:              "info",
		FirestoreEmulatorHost: "",
		FirestoreProjectID:    "",
		FirestoreCollection:   "devices",
	}

	configFileFlag := flag.String("config", defaultGeneratorConfigFile, "Path to YAML configuration file.")
	brokerURLFlag := flag.String("broker", codeDefaultCfg.BrokerURL, "MQTT broker URL")
	topicPatternFlag := flag.String("topic", codeDefaultCfg.TopicPattern, "MQTT topic pattern ({DEVICE_EUI} placeholder)")
	numDevicesFlag := flag.Int("devices", codeDefaultCfg.NumDevices, "Number of concurrent devices")
	msgRateFlag := flag.Float64("rate", codeDefaultCfg.MsgRatePerDevice, "Messages per second per device")
	durationStrFlag := flag.String("duration", codeDefaultCfg.TestDurationString, "Total duration for the load test (e.g., 30s, 2m, 1h)")
	qosFlag := flag.Int("qos", codeDefaultCfg.QOS, "MQTT QoS level (0, 1, or 2)")
	clientIDPrefixFlag := flag.String("clientid-prefix", codeDefaultCfg.ClientIDPrefix, "Prefix for MQTT client IDs")
	logLevelFlag := flag.String("log-level", codeDefaultCfg.LogLevel, "Log level (trace, debug, info, warn, error, fatal, panic)")
	firestoreEmulatorHostFlag := flag.String("firestore-emulator-host", codeDefaultCfg.FirestoreEmulatorHost, "Firestore emulator host. If empty, Firestore seeding is skipped.")
	firestoreProjectIDFlag := flag.String("firestore-project-id", codeDefaultCfg.FirestoreProjectID, "GCP Project MessageID for Firestore (required if emulator host is set).")
	firestoreCollectionFlag := flag.String("firestore-collection", codeDefaultCfg.FirestoreCollection, "Firestore collection name for device metadata.")

	flag.Parse()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	log.Logger = zerolog.New(consoleWriter).With().Timestamp().Logger()

	cliLogLevel := *logLevelFlag
	level, err := zerolog.ParseLevel(cliLogLevel)
	if err != nil {
		log.Warn().Str("cli_log_level", cliLogLevel).Msg("Invalid log level from CLI/default, defaulting to 'info' for now.")
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(level)
	}

	finalCfg := codeDefaultCfg
	yamlPathToLoad := *configFileFlag
	yamlLoadedCfg := codeDefaultCfg
	if errLoadYAML := loadConfigFromYAMLFile(yamlPathToLoad, &yamlLoadedCfg); errLoadYAML != nil {
		if !os.IsNotExist(errLoadYAML) || yamlPathToLoad != defaultGeneratorConfigFile {
			log.Fatal().Err(errLoadYAML).Str("file", yamlPathToLoad).Msg("Error loading config from YAML")
		}
	}
	finalCfg = yamlLoadedCfg

	if isFlagSet("broker") {
		finalCfg.BrokerURL = *brokerURLFlag
	}
	if isFlagSet("topic") {
		finalCfg.TopicPattern = *topicPatternFlag
	}
	if isFlagSet("devices") {
		finalCfg.NumDevices = *numDevicesFlag
	}
	if isFlagSet("rate") {
		finalCfg.MsgRatePerDevice = *msgRateFlag
	}
	if isFlagSet("duration") {
		finalCfg.TestDurationString = *durationStrFlag
	}
	if isFlagSet("qos") {
		finalCfg.QOS = *qosFlag
	}
	if isFlagSet("clientid-prefix") {
		finalCfg.ClientIDPrefix = *clientIDPrefixFlag
	}
	if isFlagSet("log-level") {
		finalCfg.LogLevel = *logLevelFlag
	}
	if isFlagSet("firestore-emulator-host") {
		finalCfg.FirestoreEmulatorHost = *firestoreEmulatorHostFlag
	}
	if isFlagSet("firestore-project-id") {
		finalCfg.FirestoreProjectID = *firestoreProjectIDFlag
	}
	if isFlagSet("firestore-collection") {
		finalCfg.FirestoreCollection = *firestoreCollectionFlag
	}

	finalLevel, errLogLevel := zerolog.ParseLevel(finalCfg.LogLevel)
	if errLogLevel != nil {
		log.Warn().Str("final_effective_level", finalCfg.LogLevel).Msg("Invalid log level in final config, keeping current logger level.")
	} else {
		if zerolog.GlobalLevel() != finalLevel {
			zerolog.SetGlobalLevel(finalLevel)
			log.Info().Str("level", finalCfg.LogLevel).Msg("Effective log level updated based on final config.")
		}
	}

	var errParseDuration error
	finalCfg.TestDuration, errParseDuration = time.ParseDuration(finalCfg.TestDurationString)
	if errParseDuration != nil {
		log.Fatal().Err(errParseDuration).Str("duration_string", finalCfg.TestDurationString).Msg("Invalid format for testDuration")
	}
	if finalCfg.NumDevices <= 0 {
		log.Fatal().Msg("Number of devices must be greater than 0")
	}
	if finalCfg.TestDuration <= 0 {
		log.Fatal().Msg("Duration must be positive")
	}
	if !strings.Contains(finalCfg.TopicPattern, "{DEVICE_EUI}") {
		log.Fatal().Msg("Topic pattern must contain '{DEVICE_EUI}' placeholder")
	}
	if finalCfg.FirestoreEmulatorHost != "" && finalCfg.FirestoreProjectID == "" {
		log.Fatal().Msg("firestoreProjectID (or -firestore-project-id flag) is required when firestoreEmulatorHost is set.")
	}

	log.Info().Interface("effective_config", finalCfg).Msg("MQTT Load Generator starting with effective configuration")

	runDurationCtx, runDurationCancel := context.WithTimeout(context.Background(), finalCfg.TestDuration)
	defer runDurationCancel()

	appCtx, appShutdownSignal := context.WithCancel(context.Background())
	defer appShutdownSignal()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Warn().Str("signal", sig.String()).Msg("Received shutdown signal. Shutting down load generator...")
		runDurationCancel()
		appShutdownSignal()
	}()

	var fsClient *firestore.Client
	if finalCfg.FirestoreEmulatorHost != "" {
		clientCreateCtx, clientCreateCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer clientCreateCancel()
		var fsErr error
		log.Info().Str("emulator_host", finalCfg.FirestoreEmulatorHost).Str("project_id", finalCfg.FirestoreProjectID).Msg("Attempting to create Firestore client for emulator")
		fsClient, fsErr = firestore.NewClient(clientCreateCtx, finalCfg.FirestoreProjectID, option.WithEndpoint(finalCfg.FirestoreEmulatorHost), option.WithoutAuthentication())
		if fsErr != nil {
			log.Fatal().Err(fsErr).Str("emulator_host", finalCfg.FirestoreEmulatorHost).Str("project_id", finalCfg.FirestoreProjectID).Msg("Failed to create Firestore client for emulator")
		}
		defer fsClient.Close()
		log.Info().Str("emulator_host", finalCfg.FirestoreEmulatorHost).Msg("Firestore client initialized for emulator")
	} else {
		log.Info().Msg("Firestore emulator host not provided, Firestore seeding will be skipped by devices.")
	}

	deviceGen, err := NewDeviceGenerator(&finalCfg, fsClient, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create DeviceGenerator")
	}

	deviceGen.Run(runDurationCtx)

	log.Info().Msg("Load generation phase complete (runDurationCtx done). Waiting for OS signal or app context cancellation to exit.")
	<-appCtx.Done()
	log.Info().Msg("Application shutdown initiated. All device goroutines should have completed their publishing phase.")
}
