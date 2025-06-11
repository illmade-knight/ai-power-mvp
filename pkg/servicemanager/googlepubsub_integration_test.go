//go:build integration

package servicemanager_test

import (
	"context"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/servicemanager"
	"github.com/testcontainers/testcontainers-go"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	adapterTestEmulatorImage = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	adapterTestEmulatorPort  = "8085/tcp"
	adapterTestProjectID     = "adapter-test-project"
)

// setupEmulatorForAdapterTest starts a Pub/Sub emulator using testcontainers.
func setupEmulatorForAdapterTest(t *testing.T, ctx context.Context) (emulatorHost string, cleanupFunc func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        adapterTestEmulatorImage,
		ExposedPorts: []string{adapterTestEmulatorPort},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", adapterTestProjectID), "--host-port=0.0.0.0:8085"},
		WaitingFor:   wait.ForLog("Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err, "Failed to start Pub/Sub emulator container for adapter test")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, adapterTestEmulatorPort)
	require.NoError(t, err)
	emulatorHost = fmt.Sprintf("%s:%s", host, port.Port())
	t.Logf("Pub/Sub emulator (for adapter test) container started, listening on: %s", emulatorHost)

	// Set environment variable for any client that might auto-discover it.
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	return emulatorHost, func() {
		t.Log("Terminating Pub/Sub emulator (for adapter test) container...")
		if termErr := container.Terminate(ctx); termErr != nil {
			t.Fatalf("failed to terminate container: %s", termErr)
		}
	}
}

// createAdapterTestYAMLFile creates a temporary YAML config file for testing.
func createAdapterTestYAMLFile(t *testing.T, content string) string {
	t.Helper()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "adapter_test_config.yaml")
	err := os.WriteFile(filePath, []byte(content), 0600)
	require.NoError(t, err, "Failed to write temporary adapter YAML file")
	return filePath
}

func TestGooglePubSubAdapter_Integration_WithManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	emulatorHost, emulatorCleanupFunc := setupEmulatorForAdapterTest(t, ctx)
	defer emulatorCleanupFunc()

	// --- Configuration ---
	testTopicName := "adapter-test-topic"
	testSubName := "adapter-test-sub"
	yamlContent := fmt.Sprintf(`
default_project_id: "%s"
environments:
  integration:
    project_id: "%s"
resources:
  pubsub_topics:
    - name: "%s"
      labels: { "tested_by": "adapter" }
  pubsub_subscriptions:
    - name: "%s"
      topic: "%s"
      ack_deadline_seconds: 42
`, adapterTestProjectID, adapterTestProjectID, testTopicName, testSubName, testTopicName)

	configFilePath := createAdapterTestYAMLFile(t, yamlContent)
	cfg, err := servicemanager.LoadAndValidateConfig(configFilePath)
	require.NoError(t, err)

	// --- Client and Adapter Setup ---
	// Create a real Pub/Sub client configured to connect to the emulator.
	clientOpts := []option.ClientOption{
		option.WithEndpoint(emulatorHost),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
	realGcpClient, err := pubsub.NewClient(ctx, adapterTestProjectID, clientOpts...)
	require.NoError(t, err)
	defer realGcpClient.Close()

	// Wrap the real client with our adapter. This is what we're testing.
	adapter := servicemanager.NewPubSubClientAdapter(realGcpClient)
	require.NotNil(t, adapter)

	// Create the manager, injecting the adapter.
	logger := zerolog.New(io.Discard)
	manager, err := servicemanager.NewPubSubManager(adapter, logger)
	require.NoError(t, err)

	// --- Run Setup and Verify ---
	t.Run("SetupResources_Through_Adapter", func(t *testing.T) {
		err = manager.Setup(ctx, cfg, "integration")
		require.NoError(t, err, "Manager.Setup through adapter failed")

		// Use a separate, direct client to verify the results.
		verifyClient, err := pubsub.NewClient(ctx, adapterTestProjectID, clientOpts...)
		require.NoError(t, err)
		defer verifyClient.Close()

		// Verify Topic
		topic := verifyClient.Topic(testTopicName)
		exists, err := topic.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, exists, "Topic should have been created by the manager via the adapter")
		topicCfg, err := topic.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, "adapter", topicCfg.Labels["tested_by"])

		// Verify Subscription
		sub := verifyClient.Subscription(testSubName)
		exists, err = sub.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, exists, "Subscription should have been created by the manager via the adapter")
		subCfg, err := sub.Config(ctx)
		require.NoError(t, err)
		assert.Equal(t, topic.String(), subCfg.Topic.String())
		assert.Equal(t, 42*time.Second, subCfg.AckDeadline)
	})

	// --- Run Teardown and Verify ---
	t.Run("TeardownResources_Through_Adapter", func(t *testing.T) {
		err = manager.Teardown(ctx, cfg, "integration")
		require.NoError(t, err, "Manager.Teardown through adapter failed")

		// Use a separate, direct client to verify the results.
		verifyClient, err := pubsub.NewClient(ctx, adapterTestProjectID, clientOpts...)
		require.NoError(t, err)
		defer verifyClient.Close()

		// Verify Topic is gone
		topic := verifyClient.Topic(testTopicName)
		exists, err := topic.Exists(ctx)
		require.NoError(t, err)
		assert.False(t, exists, "Topic should have been deleted by the manager via the adapter")

		// Verify Subscription is gone
		sub := verifyClient.Subscription(testSubName)
		exists, err = sub.Exists(ctx)
		require.NoError(t, err)
		assert.False(t, exists, "Subscription should have been deleted by the manager via the adapter")
	})
}
