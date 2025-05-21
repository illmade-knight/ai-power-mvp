package servicemanager

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// TopLevelConfig, EnvironmentSpec, ResourcesSpec, PubSubTopic, PubSubSubscription,
// RetryPolicySpec, BigQueryDataset, BigQueryTable, GCSBucket, LifecycleRuleSpec,
// LifecycleActionSpec, LifecycleConditionSpec structs are assumed to be defined
// in another file within this package (e.g., from the manager_config_go_structs artifact).
// If they are in the same file, these comments can be removed.

// LoadAndValidateConfig reads a YAML configuration file from the given path,
// unmarshals it into the TopLevelConfig struct, and performs basic validation.
func LoadAndValidateConfig(configPath string) (*TopLevelConfig, error) {
	// Read the YAML file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	// Unmarshal the YAML data
	var config TopLevelConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML from '%s': %w", configPath, err)
	}

	// Perform basic validation
	if config.DefaultProjectID == "" {
		// Depending on requirements, this could be a warning if environments always override it,
		// or an error if it's expected to be a global default.
		// For now, let's consider it a potential issue to flag.
		// return nil, fmt.Errorf("validation error: default_project_id is missing")
		fmt.Printf("Warning: default_project_id is not set in the configuration.\n")
	}

	// Validate presence of Pub/Sub Topics
	if len(config.Resources.PubSubTopics) == 0 {
		return nil, fmt.Errorf("validation error: no pubsub_topics defined in resources")
	}
	for i, topic := range config.Resources.PubSubTopics {
		if topic.Name == "" {
			return nil, fmt.Errorf("validation error: pubsub_topics[%d] is missing a name", i)
		}
	}

	// Validate presence of Pub/Sub Subscriptions
	if len(config.Resources.PubSubSubscriptions) == 0 {
		return nil, fmt.Errorf("validation error: no pubsub_subscriptions defined in resources")
	}
	for i, sub := range config.Resources.PubSubSubscriptions {
		if sub.Name == "" {
			return nil, fmt.Errorf("validation error: pubsub_subscriptions[%d] is missing a name", i)
		}
		if sub.Topic == "" {
			return nil, fmt.Errorf("validation error: pubsub_subscriptions[%d] (name: %s) is missing a topic", i, sub.Name)
		}
	}

	// Further validation for other resource types (BigQuery, GCS) can be added here as needed.
	// For "service definitions" check, we're ensuring that core communication infrastructure (topics/subs)
	// is defined. If "services" were an explicit top-level key in your YAML, we'd check that too.
	// Given the current YAML, checking for Pub/Sub resources covers a key aspect of service interaction.

	// Example: Check if at least one BigQuery dataset is defined if it's critical
	// if len(config.Resources.BigQueryDatasets) == 0 {
	// 	return nil, fmt.Errorf("validation error: no bigquery_datasets defined")
	// }

	// Example: Check if at least one GCS bucket is defined if it's critical
	// if len(config.Resources.GCSBuckets) == 0 {
	//  return nil, fmt.Errorf("validation error: no gcs_buckets defined")
	// }

	fmt.Printf("Configuration loaded and validated successfully from '%s'\n", configPath)
	return &config, nil
}
