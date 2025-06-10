package servicemanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// "gopkg.in/yaml.v3" // Only needed if we were to marshal test data here
)

// Assuming these structs are defined in the same package (e.g., in manager_config_go_structs.go or types.go)
// If not, they would need to be defined here or imported.
// For brevity, I'm not repeating them here but the test relies on their existence.
/*
type TopLevelConfig struct { ... }
type EnvironmentSpec struct { ... }
type ResourcesSpec struct { ... }
type PubSubTopic struct { ... }
type PubSubSubscription struct { ... }
... and so on for other resource types if they were validated ...
*/

func createTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_config.yaml")
	err := os.WriteFile(filePath, []byte(content), 0600)
	require.NoError(t, err, "Failed to write temporary YAML file")
	return filePath
}

func TestLoadAndValidateConfig(t *testing.T) {
	// Minimal valid YAML content for basic checks
	validBaseYAML := `
default_project_id: "default-project"
default_location: "europe-west1"
environments:
  test:
    project_id: "test-project"
resources:
  pubsub_topics:
    - name: "topic-a"
  pubsub_subscriptions:
    - name: "sub-a-to-topic-a"
      topic: "topic-a"
`

	// Test cases
	testCases := []struct {
		name          string
		yamlContent   string
		expectError   bool
		errorContains string // Substring to check in the error message
		checkConfig   func(t *testing.T, cfg *TopLevelConfig)
	}{
		{
			name:        "Valid configuration",
			yamlContent: validBaseYAML,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Equal(t, "default-project", cfg.DefaultProjectID)
				assert.Len(t, cfg.Resources.PubSubTopics, 1)
				assert.Equal(t, "topic-a", cfg.Resources.PubSubTopics[0].Name)
				assert.Len(t, cfg.Resources.PubSubSubscriptions, 1)
				assert.Equal(t, "sub-a-to-topic-a", cfg.Resources.PubSubSubscriptions[0].Name)
				assert.Equal(t, "topic-a", cfg.Resources.PubSubSubscriptions[0].Topic)
			},
		},
		{
			name:          "File not found",
			yamlContent:   "", // Will use a non-existent path for this test case
			expectError:   true,
			errorContains: "failed to read config file",
		},
		{
			name:          "Invalid YAML format",
			yamlContent:   "default_project_id: project\n  badly_indented: true",
			expectError:   true,
			errorContains: "failed to unmarshal YAML",
		},
		{
			name: "Missing default_project_id (warning only)",
			yamlContent: `
default_location: "europe-west1"
resources:
  pubsub_topics:
    - name: "topic-a"
  pubsub_subscriptions:
    - name: "sub-a-to-topic-a"
      topic: "topic-a"
`,
			expectError: false, // Current implementation only prints a warning
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Empty(t, cfg.DefaultProjectID)
				// Here you could also capture stdout to check for the warning, but that's more involved.
			},
		},
		{
			name: "No PubSubTopics defined",
			yamlContent: `
default_project_id: "project"
resources:
  pubsub_subscriptions:
    - name: "sub-a"
      topic: "topic-a"
`,
			expectError:   true,
			errorContains: "no pubsub_topics defined",
		},
		{
			name: "PubSubTopic missing name",
			yamlContent: `
default_project_id: "project"
resources:
  pubsub_topics:
    - labels: {env: "test"} # Name is missing
  pubsub_subscriptions:
    - name: "sub-a"
      topic: "topic-a"
`,
			expectError:   true,
			errorContains: "pubsub_topics[0] is missing a name",
		},
		{
			name: "No PubSubSubscriptions defined",
			yamlContent: `
default_project_id: "project"
resources:
  pubsub_topics:
    - name: "topic-a"
`,
			expectError:   true,
			errorContains: "no pubsub_subscriptions defined",
		},
		{
			name: "PubSubSubscription missing name",
			yamlContent: `
default_project_id: "project"
resources:
  pubsub_topics:
    - name: "topic-a"
  pubsub_subscriptions:
    - topic: "topic-a" # Name is missing
`,
			expectError:   true,
			errorContains: "pubsub_subscriptions[0] is missing a name",
		},
		{
			name: "PubSubSubscription missing topic",
			yamlContent: `
default_project_id: "project"
resources:
  pubsub_topics:
    - name: "topic-a"
  pubsub_subscriptions:
    - name: "sub-a" # Topic is missing
`,
			expectError:   true,
			errorContains: "pubsub_subscriptions[0] (name: sub-a) is missing a topic",
		},
		// Example of a more complete valid config
		{
			name: "Valid configuration with more details",
			yamlContent: `
default_project_id: "my-default-gcp-project"
default_location: "europe-west1"
environments:
  test:
    project_id: "my-test-gcp-project"
    default_location: "europe-west4"
    default_labels:
      env: "test"
  production:
    project_id: "my-prod-gcp-project"
    teardown_protection: true
resources:
  pubsub_topics:
    - name: "ingested-device-data"
      labels:
        data_type: "raw"
    - name: "processed-meter-readings"
      labels:
        data_type: "decoded"
  pubsub_subscriptions:
    - name: "archival-service-subscription"
      topic: "ingested-device-data"
      ack_deadline_seconds: 60
    - name: "processing-service-subscription"
      topic: "ingested-device-data"
      ack_deadline_seconds: 30
      retry_policy:
        minimum_backoff: "15s"
        maximum_backoff: "300s"
  bigquery_datasets: # Not validated by current LoadAndValidateConfig, but good for structure
    - name: "telemetry_data"
      location: "EU"
  gcs_buckets: # Not validated by current LoadAndValidateConfig
    - name: "iot-device-archive-bucket"
      location: "EUROPE-WEST1"
`,
			expectError: false,
			checkConfig: func(t *testing.T, cfg *TopLevelConfig) {
				require.NotNil(t, cfg)
				assert.Equal(t, "my-default-gcp-project", cfg.DefaultProjectID)
				require.NotNil(t, cfg.Environments["test"])
				assert.Equal(t, "my-test-gcp-project", cfg.Environments["test"].ProjectID)
				require.Len(t, cfg.Resources.PubSubTopics, 2)
				assert.Equal(t, "ingested-device-data", cfg.Resources.PubSubTopics[0].Name)
				require.Len(t, cfg.Resources.PubSubSubscriptions, 2)
				assert.Equal(t, "archival-service-subscription", cfg.Resources.PubSubSubscriptions[0].Name)
				require.NotNil(t, cfg.Resources.PubSubSubscriptions[1].RetryPolicy)
				assert.Equal(t, "15s", cfg.Resources.PubSubSubscriptions[1].RetryPolicy.MinimumBackoff)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var configPath string
			if tc.name == "File not found" {
				configPath = filepath.Join(t.TempDir(), "non_existent_config.yaml")
			} else {
				configPath = createTestYAMLFile(t, tc.yamlContent)
			}

			cfg, err := LoadAndValidateConfig(configPath)

			if tc.expectError {
				require.Error(t, err, "Expected an error but got nil")
				if tc.errorContains != "" {
					assert.True(t, strings.Contains(err.Error(), tc.errorContains), "Error message should contain '%s', got '%s'", tc.errorContains, err.Error())
				}
			} else {
				require.NoError(t, err, "Expected no error but got: %v", err)
				if tc.checkConfig != nil {
					tc.checkConfig(t, cfg)
				}
			}
		})
	}
}
