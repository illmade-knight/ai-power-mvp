package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gopkg.in/yaml.v3" // Added for YAML parsing
)

const (
	mosquittoImage                = "eclipse-mosquitto:2.0"
	mosquittoPort                 = "1883/tcp"
	defaultOrchestratorConfigFile = "orchestrator_config.yaml"
)

type OrchestratorConfig struct {
	SmctlBinaryPath             string        `yaml:"smctlBinaryPath"`
	ServiceManagerAPIBinaryPath string        `yaml:"serviceManagerAPIBinaryPath"`
	IngestionServiceBinaryPath  string        `yaml:"ingestionServiceBinaryPath"`
	LoadGeneratorBinaryPath     string        `yaml:"loadGeneratorBinaryPath"`
	PubSubVerifierBinaryPath    string        `yaml:"pubSubVerifierBinaryPath"`
	ServiceManagerYAMLPath      string        `yaml:"serviceManagerYAMLPath"`
	TestEnvName                 string        `yaml:"testEnvName"`
	GCPProjectID                string        `yaml:"gcpProjectID"`
	EnrichedTopicID             string        `yaml:"enrichedTopicID"`
	UnidentifiedTopicID         string        `yaml:"unidentifiedTopicID"`
	NumDevices                  int           `yaml:"numDevices"`
	MsgRatePerDevice            float64       `yaml:"msgRatePerDevice"`
	TestDurationString          string        `yaml:"testDuration"` // Store as string for YAML, parse to time.Duration
	TestDuration                time.Duration `yaml:"-"`            // Parsed value, ignore for YAML
	VerifierOutputFile          string        `yaml:"verifierOutputFile"`
	TestRunID                   string        `yaml:"testRunID"`
	MachineInfo                 string        `yaml:"machineInfo"`
	ServiceManagerAPIPort       string        `yaml:"serviceManagerAPIPort"`
	IngestionServiceHTTPPort    string        `yaml:"ingestionServiceHTTPPort"`
}

// logProcessOutput pipes stdout and stderr of a command to the logger.
func logProcessOutput(cmd *exec.Cmd, processName string) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Error().Err(err).Str("process", processName).Msg("Failed to get stdout pipe")
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Error().Err(err).Str("process", processName).Msg("Failed to get stderr pipe")
		return
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			log.Info().Str("process", processName).Str("stream", "stdout").Msg(scanner.Text())
		}
	}()
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Warn().Str("process", processName).Str("stream", "stderr").Msg(scanner.Text())
		}
	}()
}

// loadConfigFromYAML loads configuration from a YAML file into the cfg struct.
func loadConfigFromYAML(filePath string, cfg *OrchestratorConfig) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Info().Str("file", filePath).Msg("Orchestrator YAML config file not found, using defaults and flags.")
		return nil // Not an error if default file is not found, flags will take over
	}

	yamlFile, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read orchestrator YAML config file '%s': %w", filePath, err)
	}

	err = yaml.Unmarshal(yamlFile, cfg)
	if err != nil {
		return fmt.Errorf("failed to unmarshal orchestrator YAML config from '%s': %w", filePath, err)
	}
	log.Info().Str("file", filePath).Msg("Orchestrator configuration loaded from YAML.")
	return nil
}

func main() {
	// --- Initial Default Configuration Values ---
	// These will be used if not specified in YAML or by flags.
	cfg := OrchestratorConfig{
		SmctlBinaryPath:             "./smctl",
		ServiceManagerAPIBinaryPath: "./servicemanager_api",
		IngestionServiceBinaryPath:  "./ingestionservice_app",
		LoadGeneratorBinaryPath:     "./mqtt_load_generator",
		PubSubVerifierBinaryPath:    "./pubsub_verifier",
		ServiceManagerYAMLPath:      "./loadtest.yaml",
		TestEnvName:                 "gcp-loadtest",
		EnrichedTopicID:             "enriched-messages",
		UnidentifiedTopicID:         "unidentified-messages",
		NumDevices:                  10,
		MsgRatePerDevice:            1.0,
		TestDurationString:          "1m", // Default duration as string
		VerifierOutputFile:          "loadtest_results.json",
		ServiceManagerAPIPort:       "8090",
		IngestionServiceHTTPPort:    "8081",
	}

	// --- Command-line Flag Definitions ---
	// Flags will point to the fields in the 'cfg' struct.
	// Their default values here will be the ultimate fallback if not in YAML and not on CLI.
	//orchestratorConfigFile := flag.String("orchestrator-config", defaultOrchestratorConfigFile, "Path to orchestrator's own YAML config file")

	flag.StringVar(&cfg.SmctlBinaryPath, "smctl-bin", cfg.SmctlBinaryPath, "Path to servicemanager CLI (smctl) binary")
	flag.StringVar(&cfg.ServiceManagerAPIBinaryPath, "sm-api-bin", cfg.ServiceManagerAPIBinaryPath, "Path to servicemanager API server binary")
	flag.StringVar(&cfg.IngestionServiceBinaryPath, "ingestion-bin", cfg.IngestionServiceBinaryPath, "Path to ingestionservice binary")
	flag.StringVar(&cfg.LoadGeneratorBinaryPath, "loadgen-bin", cfg.LoadGeneratorBinaryPath, "Path to MQTT load generator binary")
	flag.StringVar(&cfg.PubSubVerifierBinaryPath, "verifier-bin", cfg.PubSubVerifierBinaryPath, "Path to PubSub verifier binary")
	flag.StringVar(&cfg.ServiceManagerYAMLPath, "sm-config", cfg.ServiceManagerYAMLPath, "Path to servicemanager YAML configuration")

	flag.StringVar(&cfg.TestEnvName, "test-env", cfg.TestEnvName, "Environment name for servicemanager operations")
	flag.StringVar(&cfg.GCPProjectID, "gcp-project", cfg.GCPProjectID, "GCP Project ID (required, can be set in YAML or CLI)")
	flag.StringVar(&cfg.EnrichedTopicID, "enriched-topic", cfg.EnrichedTopicID, "Short ID of the enriched Pub/Sub topic")
	flag.StringVar(&cfg.UnidentifiedTopicID, "unidentified-topic", cfg.UnidentifiedTopicID, "Short ID of the unidentified Pub/Sub topic")

	flag.IntVar(&cfg.NumDevices, "devices", cfg.NumDevices, "Number of devices for load generator")
	flag.Float64Var(&cfg.MsgRatePerDevice, "rate", cfg.MsgRatePerDevice, "Messages per second per device for load generator")
	flag.StringVar(&cfg.TestDurationString, "duration", cfg.TestDurationString, "Total duration for the load test (e.g., 30s, 2m, 1h)")

	flag.StringVar(&cfg.VerifierOutputFile, "verifier-output", cfg.VerifierOutputFile, "Output file for PubSub verifier")
	flag.StringVar(&cfg.TestRunID, "run-id", "", "Unique ID for this test run (default: new UUID)") // Default to empty, will be set if not in YAML/CLI
	flag.StringVar(&cfg.MachineInfo, "machine-info", cfg.MachineInfo, "Machine info for verifier metadata (optional)")
	flag.StringVar(&cfg.ServiceManagerAPIPort, "sm-api-port", cfg.ServiceManagerAPIPort, "Port for the ServiceManager API server")
	flag.StringVar(&cfg.IngestionServiceHTTPPort, "ingestion-http-port", cfg.IngestionServiceHTTPPort, "HTTP Port for the IngestionService")

	logLevelStr := flag.String("log-level", "info", "Log level for orchestrator (debug, info, warn, error)")

	// --- Load Configuration from YAML (if file exists) ---
	// This happens *before* flag.Parse() so YAML values become defaults for flags.
	// However, to know which config file to load, we need to parse -orchestrator-config first,
	// or check os.Args. For simplicity with standard flag package:
	// 1. Define all flags with their ultimate defaults.
	// 2. Parse flags once to get the orchestrator-config path.
	// 3. Load YAML into cfg, potentially overwriting initial defaults.
	// 4. Re-parse flags. This is not ideal.

	// Simpler approach:
	// 1. Set hardcoded defaults in cfg struct.
	// 2. Check for -orchestrator-config in os.Args manually before full flag parsing.
	// This is also a bit clunky.

	// Best approach with standard flags for this precedence (YAML then CLI override):
	// Step 1: Load initial defaults into cfg (done above).
	// Step 2: Check if a config file is specified via a flag or default, and load it.
	//         This will overwrite the initial defaults.
	// Step 3: Parse all flags. Any flag set on the command line will overwrite
	//         the value that came from YAML or the initial default.

	// Temporarily parse to find the config file path
	tempFlagSet := flag.NewFlagSet("preparse", flag.ContinueOnError)
	tempOrchestratorConfigFile := tempFlagSet.String("orchestrator-config", defaultOrchestratorConfigFile, "Path to orchestrator's own YAML config file")
	// Ignore errors, we only care about the value if provided
	_ = tempFlagSet.Parse(os.Args[1:])

	// Load YAML config if the file path (from flag or default) is valid
	if err := loadConfigFromYAML(*tempOrchestratorConfigFile, &cfg); err != nil {
		log.Fatal().Err(err).Msg("Error loading orchestrator configuration from YAML")
	}

	// Now, parse all flags. Values from command line will override YAML / initial defaults.
	flag.Parse()

	// --- Post-Processing and Validation of Config ---
	var err error
	cfg.TestDuration, err = time.ParseDuration(cfg.TestDurationString)
	if err != nil {
		log.Fatal().Err(err).Str("duration_string", cfg.TestDurationString).Msg("Invalid -duration format")
	}
	if cfg.TestRunID == "" {
		cfg.TestRunID = uuid.NewString()
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	level, err := zerolog.ParseLevel(*logLevelStr)
	if err != nil {
		log.Warn().Str("level", *logLevelStr).Msg("Invalid log level, defaulting to info")
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(level)
	}

	if cfg.GCPProjectID == "" {
		log.Fatal().Msg("-gcp-project (or gcpProjectID in YAML) is required")
	}

	log.Info().Interface("final_config", cfg).Msg("Load Test Orchestrator starting with effective configuration")

	orchestratorCtx, orchestratorCancel := context.WithTimeout(context.Background(), cfg.TestDuration+2*time.Minute)
	defer orchestratorCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Warn().Str("signal", sig.String()).Msg("Orchestrator received shutdown signal. Initiating cleanup...")
		orchestratorCancel()
	}()

	var processes []*exec.Cmd
	var processWg sync.WaitGroup
	var mqttBrokerURL string

	defer func() {
		log.Info().Msg("Starting orchestrator cleanup...")
		for _, proc := range processes {
			if proc.Process != nil {
				log.Info().Str("process", filepath.Base(proc.Path)).Msg("Sending SIGTERM to process...")
				if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
					log.Error().Err(err).Str("process", filepath.Base(proc.Path)).Msg("Failed to send SIGTERM, attempting SIGKILL...")
					proc.Process.Kill()
				}
			}
		}
		processWg.Wait()
		log.Info().Msg("Orchestrator cleanup finished.")
	}()

	// --- Step 1: Start MQTT Broker (Testcontainers) ---
	log.Info().Msg("Starting MQTT Broker (Mosquitto via Testcontainers)...")
	mqttNatPort, _ := nat.NewPort("tcp", strings.Split(mosquittoPort, "/")[0])
	mqttReq := testcontainers.ContainerRequest{
		Image:        mosquittoImage,
		ExposedPorts: []string{string(mqttNatPort)},
		WaitingFor:   wait.ForListeningPort(mqttNatPort).WithStartupTimeout(60 * time.Second),
	}
	mqttContainer, err := testcontainers.GenericContainer(orchestratorCtx, testcontainers.GenericContainerRequest{ContainerRequest: mqttReq, Started: true})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start MQTT Broker container")
	}
	defer mqttContainer.Terminate(orchestratorCtx)

	mqttHost, _ := mqttContainer.Host(orchestratorCtx)
	mqttMappedPort, _ := mqttContainer.MappedPort(orchestratorCtx, mqttNatPort)
	mqttBrokerURL = fmt.Sprintf("tcp://%s:%s", mqttHost, mqttMappedPort.Port())
	log.Info().Str("url", mqttBrokerURL).Msg("MQTT Broker started.")

	// --- Step 2: Start ServiceManager API Server ---
	log.Info().Msg("Starting ServiceManager API server...")
	smAPICmd := exec.CommandContext(orchestratorCtx, cfg.ServiceManagerAPIBinaryPath,
		"--config", cfg.ServiceManagerYAMLPath,
		"--port", cfg.ServiceManagerAPIPort,
		"--log-level", *logLevelStr,
	)
	logProcessOutput(smAPICmd, "servicemanager-api")
	if err := smAPICmd.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start ServiceManager API server")
	}
	processes = append(processes, smAPICmd)
	processWg.Add(1)
	go func() { defer processWg.Done(); smAPICmd.Wait(); log.Info().Msg("ServiceManager API server exited.") }()
	log.Info().Msg("Waiting for ServiceManager API server to be ready...")
	time.Sleep(5 * time.Second) // Replace with health check
	serviceManagerAPIURL := fmt.Sprintf("http://localhost:%s", cfg.ServiceManagerAPIPort)
	log.Info().Str("url", serviceManagerAPIURL).Msg("ServiceManager API server assumed started.")

	// --- Step 3: Run ServiceManager CLI Apply ---
	log.Info().Msg("Running 'smctl apply' to ensure GCP resources...")
	applyCmdArgs := []string{
		"apply",
		"--config", cfg.ServiceManagerYAMLPath,
		"--env", cfg.TestEnvName,
		"--log-level", *logLevelStr,
	}
	if cfg.GCPProjectID != "" { // Only add --project if explicitly set (overrides env default)
		applyCmdArgs = append(applyCmdArgs, "--project", cfg.GCPProjectID)
	}
	applyCmd := exec.CommandContext(orchestratorCtx, cfg.SmctlBinaryPath, applyCmdArgs...)
	applyOutput, err := applyCmd.CombinedOutput()
	if err != nil {
		log.Fatal().Err(err).Str("output", string(applyOutput)).Msg("'smctl apply' failed")
	}
	log.Info().Str("output", string(applyOutput)).Msg("'smctl apply' completed.")

	// --- Step 4: Start IngestionService ---
	log.Info().Msg("Starting IngestionService...")
	ingestionCmd := exec.CommandContext(orchestratorCtx, cfg.IngestionServiceBinaryPath)
	ingestionCmd.Env = append(os.Environ(),
		fmt.Sprintf("MQTT_BROKER_URL=%s", mqttBrokerURL),
		// Ingestion service's internal config loaders will use these:
		fmt.Sprintf("GCP_PROJECT_ID=%s", cfg.GCPProjectID),
		fmt.Sprintf("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES=%s", cfg.EnrichedTopicID),
		fmt.Sprintf("PUBSUB_TOPIC_ID_UNIDENTIFIED_MESSAGES=%s", cfg.UnidentifiedTopicID),
		// Env vars for the IngestionService.Server wrapper's config:
		fmt.Sprintf("SERVICE_MANAGER_API_URL=%s", serviceManagerAPIURL),
		fmt.Sprintf("INGESTION_SERVICE_NAME=%s", "ingestion-service-loadtest"),
		fmt.Sprintf("INGESTION_ENVIRONMENT=%s", cfg.TestEnvName),
		fmt.Sprintf("HTTP_PORT=%s", cfg.IngestionServiceHTTPPort),
		// Add other necessary env vars for ingestion (Firestore, specific topic IDs for connectors, etc.)
	)
	logProcessOutput(ingestionCmd, "ingestionservice")
	if err := ingestionCmd.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start IngestionService")
	}
	processes = append(processes, ingestionCmd)
	processWg.Add(1)
	go func() { defer processWg.Done(); ingestionCmd.Wait(); log.Info().Msg("IngestionService exited.") }()
	log.Info().Msg("Waiting for IngestionService to be ready...")
	time.Sleep(10 * time.Second)
	log.Info().Msg("IngestionService assumed started.")

	// --- Step 5: Start PubSub Verifier ---
	log.Info().Msg("Starting PubSub Verifier...")
	verifierCmd := exec.CommandContext(orchestratorCtx, cfg.PubSubVerifierBinaryPath,
		"-project", cfg.GCPProjectID,
		"-enriched-topic", cfg.EnrichedTopicID,
		"-unidentified-topic", cfg.UnidentifiedTopicID,
		"-duration", (cfg.TestDuration + 30*time.Second).String(),
		"-output", cfg.VerifierOutputFile,
		"-run-id", cfg.TestRunID,
		"-machine-info", cfg.MachineInfo,
		"-env-name", cfg.TestEnvName,
		"-log-level", *logLevelStr,
	)
	logProcessOutput(verifierCmd, "pubsub-verifier")
	if err := verifierCmd.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start PubSub Verifier")
	}
	processes = append(processes, verifierCmd)
	processWg.Add(1)
	go func() { defer processWg.Done(); verifierCmd.Wait(); log.Info().Msg("PubSub Verifier exited.") }()
	log.Info().Msg("PubSub Verifier started.")

	// --- Step 6: Start MQTT Load Generator ---
	log.Info().Msg("Starting MQTT Load Generator...")
	loadGenCmd := exec.CommandContext(orchestratorCtx, cfg.LoadGeneratorBinaryPath,
		"-broker", mqttBrokerURL,
		"-devices", fmt.Sprintf("%d", cfg.NumDevices),
		"-rate", fmt.Sprintf("%.2f", cfg.MsgRatePerDevice),
		"-duration", cfg.TestDuration.String(),
		"-topic", "devices/{DEVICE_EUI}/data",
		"-qos", "1",
		"-clientid-prefix", "loadgen-orch",
		"-log-level", *logLevelStr,
	)
	logProcessOutput(loadGenCmd, "mqtt-load-generator")
	if err := loadGenCmd.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start MQTT Load Generator")
	}
	processes = append(processes, loadGenCmd)
	processWg.Add(1)
	go func() { defer processWg.Done(); loadGenCmd.Wait(); log.Info().Msg("MQTT Load Generator exited.") }()

	log.Info().Dur("duration", cfg.TestDuration).Msg("Load test running...")
	select {
	case <-time.After(cfg.TestDuration):
		log.Info().Msg("Load test duration completed.")
	case <-orchestratorCtx.Done():
		log.Info().Msg("Orchestrator context cancelled, test ending early.")
	}

	log.Info().Msg("Load test finished. Initiating shutdown of child processes...")
	orchestratorCancel()

	waitShutdown := time.NewTimer(20 * time.Second)
	processDone := make(chan struct{})
	go func() {
		processWg.Wait()
		close(processDone)
	}()

	select {
	case <-processDone:
		log.Info().Msg("All managed processes have exited.")
	case <-waitShutdown.C:
		log.Warn().Msg("Timeout waiting for all processes to exit gracefully. Cleanup defer will force stop.")
	}

	log.Info().Str("results_file", cfg.VerifierOutputFile).Msg("Orchestration complete. Check verifier output for results.")
}
