package servicemanager

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ServicesDefinition is an interface that abstracts the source of the complete
// service and resource configuration for the entire system.
type ServicesDefinition interface {
	GetTopLevelConfig() (*TopLevelConfig, error)
	GetDataflow(name string) (*DataflowSpec, error)
	GetService(name string) (*ServiceSpec, error)
	GetProjectID(environment string) (string, error)
}

// --- YAML Implementation ---

// YAMLServicesDefinition implements ServicesDefinition for a local YAML file.
type YAMLServicesDefinition struct {
	filePath      string
	parsedConfig  *TopLevelConfig
	dataflowIndex map[string]DataflowSpec
	serviceIndex  map[string]ServiceSpec
}

// NewYAMLServicesDefinition creates and initializes a ServicesDefinition from a YAML file path.
func NewYAMLServicesDefinition(filePath string) (*YAMLServicesDefinition, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read services definition file '%s': %w", filePath, err)
	}

	var config TopLevelConfig
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML from '%s': %w", filePath, err)
	}

	dfIndex, svcIndex := buildIndexes(&config)

	return &YAMLServicesDefinition{
		filePath:      filePath,
		parsedConfig:  &config,
		dataflowIndex: dfIndex,
		serviceIndex:  svcIndex,
	}, nil
}

// --- In-Memory Implementation ---

// InMemoryServicesDefinition implements ServicesDefinition for an in-memory config struct.
// This is useful for testing or for scenarios where the configuration is generated dynamically.
type InMemoryServicesDefinition struct {
	parsedConfig  *TopLevelConfig
	dataflowIndex map[string]DataflowSpec
	serviceIndex  map[string]ServiceSpec
}

// NewInMemoryServicesDefinition creates and initializes a ServicesDefinition from a TopLevelConfig struct.
func NewInMemoryServicesDefinition(config *TopLevelConfig) (*InMemoryServicesDefinition, error) {
	if config == nil {
		return nil, fmt.Errorf("cannot create services definition from nil config")
	}

	dfIndex, svcIndex := buildIndexes(config)

	return &InMemoryServicesDefinition{
		parsedConfig:  config,
		dataflowIndex: dfIndex,
		serviceIndex:  svcIndex,
	}, nil
}

// --- Common Logic ---

// buildIndexes is a shared helper to create lookup maps from a config struct for efficient access.
func buildIndexes(config *TopLevelConfig) (map[string]DataflowSpec, map[string]ServiceSpec) {
	dfIndex := make(map[string]DataflowSpec)
	for _, df := range config.Dataflows {
		dfIndex[df.Name] = df
	}

	svcIndex := make(map[string]ServiceSpec)
	for _, svc := range config.Services {
		svcIndex[svc.Name] = svc
	}
	return dfIndex, svcIndex
}

// GetTopLevelConfig returns the entire parsed configuration struct.
func (sd *YAMLServicesDefinition) GetTopLevelConfig() (*TopLevelConfig, error) {
	if sd.parsedConfig == nil {
		return nil, fmt.Errorf("services definition has not been loaded")
	}
	return sd.parsedConfig, nil
}
func (sd *InMemoryServicesDefinition) GetTopLevelConfig() (*TopLevelConfig, error) {
	if sd.parsedConfig == nil {
		return nil, fmt.Errorf("services definition has not been loaded")
	}
	return sd.parsedConfig, nil
}

// GetDataflow finds a dataflow by name using the pre-built index.
func (sd *YAMLServicesDefinition) GetDataflow(name string) (*DataflowSpec, error) {
	df, ok := sd.dataflowIndex[name]
	if !ok {
		return nil, fmt.Errorf("dataflow '%s' not found in services definition", name)
	}
	return &df, nil
}
func (sd *InMemoryServicesDefinition) GetDataflow(name string) (*DataflowSpec, error) {
	df, ok := sd.dataflowIndex[name]
	if !ok {
		return nil, fmt.Errorf("dataflow '%s' not found in services definition", name)
	}
	return &df, nil
}

// GetService finds a service by name using the pre-built index.
func (sd *YAMLServicesDefinition) GetService(name string) (*ServiceSpec, error) {
	svc, ok := sd.serviceIndex[name]
	if !ok {
		return nil, fmt.Errorf("service '%s' not found in services definition", name)
	}
	return &svc, nil
}
func (sd *InMemoryServicesDefinition) GetService(name string) (*ServiceSpec, error) {
	svc, ok := sd.serviceIndex[name]
	if !ok {
		return nil, fmt.Errorf("service '%s' not found in services definition", name)
	}
	return &svc, nil
}

// GetProjectID determines the correct GCP Project ID for a given environment.
func (sd *YAMLServicesDefinition) GetProjectID(environment string) (string, error) {
	if sd.parsedConfig == nil {
		return "", fmt.Errorf("services definition has not been loaded")
	}
	if envSpec, ok := sd.parsedConfig.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if sd.parsedConfig.DefaultProjectID != "" {
		return sd.parsedConfig.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default is set", environment)
}
func (sd *InMemoryServicesDefinition) GetProjectID(environment string) (string, error) {
	if sd.parsedConfig == nil {
		return "", fmt.Errorf("services definition has not been loaded")
	}
	if envSpec, ok := sd.parsedConfig.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if sd.parsedConfig.DefaultProjectID != "" {
		return sd.parsedConfig.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default is set", environment)
}
