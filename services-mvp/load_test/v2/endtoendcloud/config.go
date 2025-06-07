package endtoend

import (
	"encoding/json"
	"os"
	"time"

	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/enrich/connectors"
	"github.com/illmade-knight/ai-power-mvp/services-mvp/ingestion/mqtttopubsub/converter"
)

// CloudTestConfig holds all configurable parameters for the end-to-end cloud test.
type CloudTestConfig struct {
	// Timing and Duration
	OverallTestTimeout              time.Duration `json:"overallTestTimeout"`
	LoadGenDuration                 time.Duration `json:"loadGenDuration"`
	CloudServiceStartupDelay        time.Duration `json:"cloudServiceStartupDelay"`
	GracePeriodForMessageCompletion time.Duration `json:"gracePeriodForMessageCompletion"`

	// Device and Load Simulation
	NumKnownDevices   int     `json:"numKnownDevices"`
	NumUnknownDevices int     `json:"numUnknownDevices"`
	MsgRatePerDevice  float64 `json:"msgRatePerDevice"`

	MinMessagesMultiplier float64 `json:"minMessagesMultiplier"`

	// Service Configurations
	ConverterServiceConfig  converter.IngestionServiceConfig   `json:"converterServiceConfig"`
	EnrichmentServiceConfig connectors.EnrichmentServiceConfig `json:"enrichmentServiceConfig"`
}

// DefaultCloudTestConfig returns a configuration with sensible default values for the test.
func DefaultCloudTestConfig() *CloudTestConfig {
	return &CloudTestConfig{
		OverallTestTimeout:              4 * time.Minute,
		LoadGenDuration:                 20 * time.Second,
		CloudServiceStartupDelay:        30 * time.Second,
		GracePeriodForMessageCompletion: 15 * time.Second,
		NumKnownDevices:                 20,
		NumUnknownDevices:               2,
		MsgRatePerDevice:                1.0,
		MinMessagesMultiplier:           0.8,
		ConverterServiceConfig: converter.IngestionServiceConfig{
			InputChanCapacity:    100,
			NumProcessingWorkers: 5,
		},
		EnrichmentServiceConfig: connectors.EnrichmentServiceConfig{
			NumProcessingWorkers:   5,
			InputChanCapacity:      100,
			MaxOutstandingMessages: 100,
		},
	}
}

// LoadCloudTestConfigFromFile loads a test configuration from a JSON file.
// This allows for running the test with different parameter sets without changing code.
func LoadCloudTestConfigFromFile(filePath string) (*CloudTestConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var cfg CloudTestConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	cfg.OverallTestTimeout = cfg.OverallTestTimeout * time.Minute
	cfg.LoadGenDuration = cfg.LoadGenDuration * time.Second
	cfg.CloudServiceStartupDelay = cfg.CloudServiceStartupDelay * time.Second
	cfg.GracePeriodForMessageCompletion = cfg.GracePeriodForMessageCompletion * time.Second
	return &cfg, nil
}
