package main

import (
	"context"
	"flag"
	"fmt"
	messenger2 "load_test/apps/devicegen/messenger"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v3"
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

// --- DeviceGenerator Struct and Methods ---

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

	log.Info().Interface("effective_config", finalCfg).Msg("MQTT Load DeviceGenerator starting with effective configuration")

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

	deviceCfg := &messenger2.DeviceGeneratorConfig{
		NumDevices:            finalCfg.NumDevices,
		MsgRatePerDevice:      finalCfg.MsgRatePerDevice,
		FirestoreEmulatorHost: finalCfg.FirestoreEmulatorHost,
		FirestoreProjectID:    finalCfg.FirestoreProjectID,
		FirestoreCollection:   finalCfg.FirestoreCollection,
	}

	deviceGen, err := messenger2.NewDeviceGenerator(deviceCfg, fsClient, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create DeviceGenerator")
	}

	deviceGen.CreateDevices()

	if deviceCfg.FirestoreEmulatorHost != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		err = deviceGen.SeedDevices(ctx)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to seed devices")
		}
	}

	factory := &messenger2.PahoMQTTClientFactory{}
	publisher := messenger2.NewPublisher(finalCfg.TopicPattern, finalCfg.BrokerURL, finalCfg.ClientIDPrefix, finalCfg.QOS, log.Logger, factory)

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

	err = publisher.Publish(deviceGen.Devices, finalCfg.TestDuration, runDurationCtx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish Device messages")
	}

	log.Info().Msg("Load generation phase complete (runDurationCtx done). Waiting for OS signal or app context cancellation to exit.")
	<-appCtx.Done()
	log.Info().Msg("Application shutdown initiated. All device goroutines should have completed their publishing phase.")
}
