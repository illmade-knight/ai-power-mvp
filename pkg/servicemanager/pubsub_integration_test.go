//go:build integration

package servicemanager

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const (
	testPubSubManagerEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	testPubSubManagerEmulatorPort  = "8085/tcp"
	testPubSubManagerProjectID     = "test-sm-project" // Project MessageID for the emulator
)

// setupPubSubEmulatorForManagerTest starts a Pub/Sub emulator.
func setupPubSubEmulatorForManagerTest(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        testPubSubManagerEmulatorImage,
		ExposedPorts: []string{testPubSubManagerEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testPubSubManagerProjectID), fmt.Sprintf("--host-port=0.0.0.0:%s", strings.Split(testPubSubManagerEmulatorPort, "/")[0])},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Pub/Sub emulator container for manager test")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, testPubSubManagerEmulatorPort)
	require.NoError(t, err)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator (for manager test) container started, listening on: %s", emulatorHost)

	// Set PUBSUB_EMULATOR_HOST for client libraries to auto-detect
	// t.Setenv is preferred as it handles cleanup.
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	t.Setenv("GOOGLE_CLOUD_PROJECT", testPubSubManagerProjectID) // Ensure project MessageID is also set for clients

	return emulatorHost, func() {
		t.Log("Terminating Pub/Sub emulator (for manager test) container...")
		require.NoError(t, container.Terminate(ctx))
	}
}

// createManagerTestYAMLFile creates a temporary YAML config file for testing.
func createManagerTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "manager_test_config.yaml")
	err := os.WriteFile(filePath, []byte(content), 0600)
	require.NoError(t, err, "Failed to write temporary manager YAML file")
	return filePath
}

func TestPubSubManager_Integration_SetupAndTeardown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	emulatorHost, emulatorCleanupFunc := setupPubSubEmulatorForManagerTest(t, ctx)
	defer emulatorCleanupFunc()

	// Define a sample configuration YAML
	testTopicName1 := "sm-test-topic-1"
	testTopicName2 := "sm-test-topic-2"
	testSubName1 := "sm-test-sub-1-for-topic-1"
	testSubName2 := "sm-test-sub-2-for-topic-2"

	yamlContent := fmt.Sprintf(`
default_project_id: "%s"
default_location: "europe-west1" # Not directly used by PubSubManager, but good for completeness
environments:
  integration_test:
    project_id: "%s" # This project MessageID will be used by PubSubManager
    teardown_protection: false # Allow teardown in test
resources:
  pubsub_topics:
    - name: "%s"
      labels:
        env: "integration_test"
        purpose: "topic1"
    - name: "%s"
  pubsub_subscriptions:
    - name: "%s"
      topic: "%s"
      ack_deadline_seconds: 30
      labels:
        owner: "manager_test"
    - name: "%s"
      topic: "%s"
      ack_deadline_seconds: 15
      message_retention_duration: "86400s" # 1 day
      retry_policy:
        minimum_backoff: "5s"
        maximum_backoff: "120s"
`, testPubSubManagerProjectID, testPubSubManagerProjectID, testTopicName1, testTopicName2, testSubName1, testTopicName1, testSubName2, testTopicName2)

	configFilePath := createManagerTestYAMLFile(t, yamlContent)

	// Load the configuration
	cfg, err := LoadAndValidateConfig(configFilePath)
	require.NoError(t, err, "Failed to load and validate test config")
	require.NotNil(t, cfg, "Config should not be nil")

	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	manager := NewPubSubManager(logger)

	// Client options for connecting to the emulator
	clientOpts := []option.ClientOption{
		option.WithEndpoint(emulatorHost), // Not strictly needed if PUBSUB_EMULATOR_HOST is set, but explicit
		option.WithoutAuthentication(),
		// For Pub/Sub emulator, gRPC insecure is often implicitly handled by WithEndpoint or PUBSUB_EMULATOR_HOST
		// but can be added if issues arise:
		// option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	// --- Test Setup ---
	t.Run("SetupPubSubResources", func(t *testing.T) {
		err = manager.Setup(ctx, cfg, "integration_test", clientOpts...)
		require.NoError(t, err, "PubSubManager.Setup failed")

		// Verification client
		verifyClient, err := pubsub.NewClient(ctx, testPubSubManagerProjectID, clientOpts...)
		require.NoError(t, err, "Failed to create verification Pub/Sub client")
		defer verifyClient.Close()

		// Verify topics
		topic1 := verifyClient.Topic(testTopicName1)
		exists, err := topic1.Exists(ctx)
		require.NoError(t, err, "Error checking existence of topic1")
		assert.True(t, exists, "Topic %s should exist after setup", testTopicName1)
		topic1Config, err := topic1.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, "integration_test", topic1Config.Labels["env"])
		assert.Equal(t, "topic1", topic1Config.Labels["purpose"])

		topic2 := verifyClient.Topic(testTopicName2)
		exists, err = topic2.Exists(ctx)
		require.NoError(t, err, "Error checking existence of topic2")
		assert.True(t, exists, "Topic %s should exist after setup", testTopicName2)

		// Verify subscriptions
		sub1 := verifyClient.Subscription(testSubName1)
		exists, err = sub1.Exists(ctx)
		require.NoError(t, err, "Error checking existence of sub1")
		assert.True(t, exists, "Subscription %s should exist after setup", testSubName1)
		sub1Config, err := sub1.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, topic1.String(), sub1Config.Topic.String())
		assert.Equal(t, 30*time.Second, sub1Config.AckDeadline)
		assert.Equal(t, "manager_test", sub1Config.Labels["owner"])

		sub2 := verifyClient.Subscription(testSubName2)
		exists, err = sub2.Exists(ctx)
		require.NoError(t, err, "Error checking existence of sub2")
		assert.True(t, exists, "Subscription %s should exist after setup", testSubName2)
		sub2Config, err := sub2.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, topic2.String(), sub2Config.Topic.String())
		assert.Equal(t, 15*time.Second, sub2Config.AckDeadline)
		assert.Equal(t, 24*time.Hour, sub2Config.RetentionDuration) // 86400s
		require.NotNil(t, sub2Config.RetryPolicy)
		assert.Equal(t, 5*time.Second, sub2Config.RetryPolicy.MinimumBackoff)
		assert.Equal(t, 120*time.Second, sub2Config.RetryPolicy.MaximumBackoff)
	})

	// --- Test Teardown ---
	t.Run("TeardownPubSubResources", func(t *testing.T) {
		// Ensure teardown protection is false for this test environment in config
		cfg.Environments["integration_test"] = EnvironmentSpec{
			ProjectID:          testPubSubManagerProjectID,
			TeardownProtection: false, // Explicitly false for test
		}

		err = manager.Teardown(ctx, cfg, "integration_test", clientOpts...)
		require.NoError(t, err, "PubSubManager.Teardown failed")

		// Verification client
		verifyClient, err := pubsub.NewClient(ctx, testPubSubManagerProjectID, clientOpts...)
		require.NoError(t, err, "Failed to create verification Pub/Sub client for teardown check")
		defer verifyClient.Close()

		// Verify topics are deleted
		topic1 := verifyClient.Topic(testTopicName1)
		exists, err := topic1.Exists(ctx)
		require.NoError(t, err, "Error checking existence of topic1 after teardown")
		assert.False(t, exists, "Topic %s should NOT exist after teardown", testTopicName1)

		topic2 := verifyClient.Topic(testTopicName2)
		exists, err = topic2.Exists(ctx)
		require.NoError(t, err, "Error checking existence of topic2 after teardown")
		assert.False(t, exists, "Topic %s should NOT exist after teardown", testTopicName2)

		// Verify subscriptions are deleted
		sub1 := verifyClient.Subscription(testSubName1)
		exists, err = sub1.Exists(ctx)
		require.NoError(t, err, "Error checking existence of sub1 after teardown")
		assert.False(t, exists, "Subscription %s should NOT exist after teardown", testSubName1)

		sub2 := verifyClient.Subscription(testSubName2)
		exists, err = sub2.Exists(ctx)
		require.NoError(t, err, "Error checking existence of sub2 after teardown")
		assert.False(t, exists, "Subscription %s should NOT exist after teardown", testSubName2)
	})
}

// Note: Ensure the structs like TopLevelConfig, PubSubTopic, PubSubSubscription etc.
// are defined in the servicemanager package and accessible here.
// Also, ensure LoadAndValidateConfig is accessible.
