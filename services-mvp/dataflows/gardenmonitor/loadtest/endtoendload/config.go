package endtoendload

import (
	"encoding/json"
	"os"
	"time"

	"github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/bigquery"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/dataflows/gardenmonitor/ingestion/converter"
)

// LoadTestConfig holds all configurable parameters for the end-to-end cloud load test.
type LoadTestConfig struct {
	// Timing and Duration
	OverallTestTimeout              time.Duration `json:"overallTestTimeout"`
	LoadGenDuration                 time.Duration `json:"loadGenDuration"`
	CloudServiceStartupDelay        time.Duration `json:"cloudServiceStartupDelay"`
	GracePeriodForMessageCompletion time.Duration `json:"gracePeriodForMessageCompletion"`

	// Device and Load Simulation
	NumDevices       int     `json:"numDevices"`
	MsgRatePerDevice float64 `json:"msgRatePerDevice"`

	// Verification Thresholds
	MinMessagesMultiplier float64 `json:"minMessagesMultiplier"`

	// Service Configurations
	ConverterServiceConfig  converter.IngestionServiceConfig `json:"converterServiceConfig"`
	ProcessingServiceConfig bigquery.ServiceConfig           `json:"processingServiceConfig"`
}

// DefaultLoadTestConfig returns a configuration with sensible default values for the test.
func DefaultLoadTestConfig() *LoadTestConfig {
	return &LoadTestConfig{
		OverallTestTimeout:              5 * time.Minute,
		LoadGenDuration:                 30 * time.Second,
		CloudServiceStartupDelay:        20 * time.Second,
		GracePeriodForMessageCompletion: 30 * time.Second,
		NumDevices:                      10,
		MsgRatePerDevice:                2.0, // Each device sends 2 messages per second
		MinMessagesMultiplier:           0.8, // Expect at least 80% of messages to succeed
		ConverterServiceConfig:          converter.DefaultIngestionServiceConfig(),
		ProcessingServiceConfig: bigquery.ServiceConfig{
			NumProcessingWorkers: 10,
			// Add default batching configuration
			BatchSize:    100,
			FlushTimeout: 15 * time.Second,
		},
	}
}

// LoadTestConfigFromFile loads a test configuration from a JSON file.
func LoadTestConfigFromFile(filePath string) (*LoadTestConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Use a temporary struct that mirrors the JSON structure for easier unmarshalling.
	var rawConfig struct {
		OverallTestTimeout              int     `json:"overallTestTimeout"`
		LoadGenDuration                 int     `json:"loadGenDuration"`
		CloudServiceStartupDelay        int     `json:"cloudServiceStartupDelay"`
		GracePeriodForMessageCompletion int     `json:"gracePeriodForMessageCompletion"`
		NumDevices                      int     `json:"numDevices"`
		MsgRatePerDevice                float64 `json:"msgRatePerDevice"`
		MinMessagesMultiplier           float64 `json:"minMessagesMultiplier"`
		// Add nested struct to parse the processing service config from JSON
		ProcessingServiceConfig struct {
			NumProcessingWorkers int `json:"numProcessingWorkers"`
			BatchSize            int `json:"batchSize"`
			FlushTimeout         int `json:"flushTimeout"` // in seconds
		} `json:"processingServiceConfig"`
	}

	if err := json.Unmarshal(data, &rawConfig); err != nil {
		return nil, err
	}

	// Create a config with default values first, then overwrite with file values
	cfg := DefaultLoadTestConfig()
	if rawConfig.OverallTestTimeout > 0 {
		cfg.OverallTestTimeout = time.Duration(rawConfig.OverallTestTimeout) * time.Minute
	}
	if rawConfig.LoadGenDuration > 0 {
		cfg.LoadGenDuration = time.Duration(rawConfig.LoadGenDuration) * time.Second
	}
	if rawConfig.CloudServiceStartupDelay > 0 {
		cfg.CloudServiceStartupDelay = time.Duration(rawConfig.CloudServiceStartupDelay) * time.Second
	}
	if rawConfig.GracePeriodForMessageCompletion > 0 {
		cfg.GracePeriodForMessageCompletion = time.Duration(rawConfig.GracePeriodForMessageCompletion) * time.Second
	}
	if rawConfig.NumDevices > 0 {
		cfg.NumDevices = rawConfig.NumDevices
	}
	if rawConfig.MsgRatePerDevice > 0 {
		cfg.MsgRatePerDevice = rawConfig.MsgRatePerDevice
	}
	if rawConfig.MinMessagesMultiplier > 0 {
		cfg.MinMessagesMultiplier = rawConfig.MinMessagesMultiplier
	}

	// Overwrite nested config values if they exist in the file
	if rawConfig.ProcessingServiceConfig.NumProcessingWorkers > 0 {
		cfg.ProcessingServiceConfig.NumProcessingWorkers = rawConfig.ProcessingServiceConfig.NumProcessingWorkers
	}
	if rawConfig.ProcessingServiceConfig.BatchSize > 0 {
		cfg.ProcessingServiceConfig.BatchSize = rawConfig.ProcessingServiceConfig.BatchSize
	}
	if rawConfig.ProcessingServiceConfig.FlushTimeout > 0 {
		cfg.ProcessingServiceConfig.FlushTimeout = time.Duration(rawConfig.ProcessingServiceConfig.FlushTimeout) * time.Second
	}

	return cfg, nil
}
