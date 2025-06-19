//go:build integration

package cmd_test // Using _test package for black-box testing of the CLI

import (
	"bytes"
	"context"
	"errors" // Added for errors.Is if not using testify's assert.ErrorIs
	"fmt"
	"net/http" // Added for http.StatusOK
	"os"
	"os/exec"
	"path/filepath"
	"runtime" // Added to check OS
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/docker/go-connections/nat" // Correct import for nat.Port
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	cliTestProjectID = "sm-cli-test-project"
	cliTestEnvName   = "integration-cli"

	// Emulator images
	pubsubEmulatorImageCLI   = "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators"
	bigqueryEmulatorImageCLI = "ghcr.io/goccy/bigquery-emulator:0.6.6"
	gcsEmulatorImageCLI      = "fsouza/fake-gcs-server:1.47"

	// Port strings for nat.NewPort
	pubsubEmulatorPortStringCLI   = "8085"
	bigqueryEmulatorPortStringCLI = "9050"
	gcsEmulatorPortStringCLI      = "4443"
)

var (
	cliBinaryPath      string
	tempTestConfigPath string
)

func runCLICommand(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	log.Debug().Str("cliBinaryPath_in_runCLICommand", cliBinaryPath).Msg("Path to be executed")
	cmd := exec.Command(cliBinaryPath, args...)
	var outb, errb bytes.Buffer
	cmd.Stdout = &outb
	cmd.Stderr = &errb
	log.Debug().Str("command", cliBinaryPath).Interface("args", args).Msg("Executing CLI command")
	err := cmd.Run()
	stdout := outb.String()
	stderr := errb.String()
	if stdout != "" {
		log.Debug().Str("stdout", stdout).Msg("CLI command stdout")
	}
	if stderr != "" {
		log.Debug().Str("stderr", stderr).Msg("CLI command stderr")
	}
	if err != nil {
		log.Error().Err(err).Str("stderr_on_error", stderr).Msg("CLI command execution failed")
	}
	return stdout, stderr, err
}

func createTestServiceConfigYAML(t *testing.T, projectID, envName string) string {
	t.Helper()
	bucketNameSuffix := strings.ToLower(strings.ReplaceAll(projectID, "-", ""))
	content := fmt.Sprintf(`
default_project_id: "%s"
default_location: "US-CENTRAL1"
environments:
  %s:
    project_id: "%s"
    default_location: "EUROPE-WEST1"
    teardown_protection: false
resources:
  pubsub_topics:
    - name: "cli-topic-alpha"
      labels: { "env": "%s", "app": "smctl-lifecycle-test" }
  pubsub_subscriptions:
    - name: "cli-sub-for-alpha"
      topic: "cli-topic-alpha"
      ack_deadline_seconds: 45
  bigquery_datasets:
    - name: "clitest_dataset_one"
      location: "EU"
  bigquery_tables:
    - name: "clitest_table_in_one"
      dataset: "clitest_dataset_one"
      schema_source_type: "inline"
      schema_source_identifier: '[{"name": "id", "type": "STRING"}]'
  gcs_buckets:
    - name: "sm-cli-lifecycle-bucket-%s"
      location: "US-EAST1"
      storage_class: "STANDARD"
`, projectID, envName, projectID, envName, bucketNameSuffix)

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "cli_lifecycle_test_config.yaml")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err)
	return filePath
}

func TestMain(m *testing.M) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).Level(zerolog.DebugLevel)

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal().Err(err).Msg("TestMain: Failed to get current working directory")
	}
	moduleRoot := filepath.Join(wd, "..")
	cliMainGoPath := filepath.Join(moduleRoot, "cli", "main.go")

	outputBinaryName := "test_smctl_executable"
	if runtime.GOOS == "windows" {
		outputBinaryName += ".exe"
	}
	fullOutputBinaryPath := filepath.Join(wd, outputBinaryName)

	if _, errStat := os.Stat(cliMainGoPath); os.IsNotExist(errStat) {
		log.Fatal().Str("path_checked", cliMainGoPath).Msg("TestMain: CLI main.go not found. Adjust path if necessary.")
	}

	buildCmd := exec.Command("go", "build", "-o", fullOutputBinaryPath, cliMainGoPath)
	buildCmd.Dir = moduleRoot

	output, errBuild := buildCmd.CombinedOutput()
	if errBuild != nil {
		log.Fatal().Err(errBuild).Str("output", string(output)).Str("build_dir", buildCmd.Dir).Str("output_path", fullOutputBinaryPath).Msg("TestMain: Failed to build CLI tool for integration tests")
	}

	if _, errStat := os.Stat(fullOutputBinaryPath); os.IsNotExist(errStat) {
		log.Fatal().Str("path_checked", fullOutputBinaryPath).Msg("TestMain: Built CLI executable not found after build command. Check build output and paths.")
	}

	cliBinaryPath = fullOutputBinaryPath
	log.Info().Str("path", cliBinaryPath).Msg("TestMain: CLI tool built successfully and path confirmed.")

	exitCode := m.Run()

	errRemove := os.Remove(cliBinaryPath)
	if errRemove != nil {
		log.Error().Err(errRemove).Str("path", cliBinaryPath).Msg("TestMain: Failed to remove CLI binary")
	} else {
		log.Info().Str("path", cliBinaryPath).Msg("TestMain: CLI binary removed")
	}
	os.Exit(exitCode)
}

// --- Emulator Setup Helper Functions ---

func setupPubSubEmulatorForCLI(t *testing.T, ctx context.Context, projectID string) (testcontainers.Container, string, error) {
	t.Helper()
	natPort, err := nat.NewPort("tcp", pubsubEmulatorPortStringCLI)
	if err != nil {
		return nil, "", fmt.Errorf("creating nat.Port for Pub/Sub: %w", err)
	}
	req := testcontainers.ContainerRequest{
		Image:        pubsubEmulatorImageCLI,
		ExposedPorts: []string{string(natPort)},
		Cmd:          []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--host-port=0.0.0.0:%s", pubsubEmulatorPortStringCLI)},
		WaitingFor:   wait.ForLog("INFO: Server started, listening on").WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, "", fmt.Errorf("starting Pub/Sub emulator: %w. Request: %+v", err, req)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", fmt.Errorf("getting Pub/Sub emulator host: %w", err)
	}
	mappedPort, err := container.MappedPort(ctx, natPort)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", fmt.Errorf("getting Pub/Sub emulator mapped port: %w", err)
	}
	endpoint := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	log.Info().Str("endpoint", endpoint).Str("projectID", projectID).Msg("Pub/Sub Emulator helper: started")
	return container, endpoint, nil
}

func setupBigQueryEmulatorForCLI(t *testing.T, ctx context.Context, projectID string) (testcontainers.Container, string, error) {
	t.Helper()
	natPort, err := nat.NewPort("tcp", bigqueryEmulatorPortStringCLI)
	if err != nil {
		return nil, "", fmt.Errorf("creating nat.Port for BigQueryConfig: %w", err)
	}
	datasetToPreCreate := "clitest_dataset_one"
	req := testcontainers.ContainerRequest{
		Image:        bigqueryEmulatorImageCLI,
		ExposedPorts: []string{string(natPort)},
		Cmd:          []string{fmt.Sprintf("--project=%s", projectID), fmt.Sprintf("--dataset=%s", datasetToPreCreate)},
		WaitingFor: wait.NewHTTPStrategy(fmt.Sprintf("/bigquery/v2/projects/%s/datasets/%s", projectID, datasetToPreCreate)).
			WithPort(natPort).
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
			WithStartupTimeout(120 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, "", fmt.Errorf("starting BigQueryConfig emulator: %w. Request: %+v", err, req)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", fmt.Errorf("getting BigQueryConfig emulator host: %w", err)
	}
	mappedPort, err := container.MappedPort(ctx, natPort)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", fmt.Errorf("getting BigQueryConfig emulator mapped port: %w", err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, mappedPort.Port())
	log.Info().Str("endpoint", endpoint).Str("projectID", projectID).Msg("BigQueryConfig Emulator helper: started")
	return container, endpoint, nil
}

func setupGCSEmulatorForCLI(t *testing.T, ctx context.Context) (testcontainers.Container, string, error) {
	t.Helper()
	natPort, err := nat.NewPort("tcp", gcsEmulatorPortStringCLI)
	if err != nil {
		return nil, "", fmt.Errorf("creating nat.Port for GCS: %w", err)
	}
	req := testcontainers.ContainerRequest{
		Image:        gcsEmulatorImageCLI,
		ExposedPorts: []string{string(natPort)},
		Cmd:          []string{"-scheme", "http"},
		WaitingFor: wait.NewHTTPStrategy("/storage/v1/b").
			WithPort(natPort).
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
			WithStartupTimeout(60 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, "", fmt.Errorf("starting GCS emulator: %w. Request: %+v", err, req)
	}

	host, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", fmt.Errorf("getting GCS emulator host: %w", err)
	}
	mappedPort, err := container.MappedPort(ctx, natPort)
	if err != nil {
		container.Terminate(ctx)
		return nil, "", fmt.Errorf("getting GCS emulator mapped port: %w", err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, mappedPort.Port())
	log.Info().Str("endpoint", endpoint).Msg("GCS Emulator helper: started")
	return container, endpoint, nil
}

func TestCLILifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	log.Info().Msg("Starting CLI Lifecycle Integration Test...")

	// --- Emulator Setup using helper functions ---
	pubsubContainer, pubsubEmulatorEndpoint, err := setupPubSubEmulatorForCLI(t, ctx, cliTestProjectID)
	require.NoError(t, err, "Pub/Sub emulator setup failed")
	defer func() { require.NoError(t, pubsubContainer.Terminate(ctx)) }()
	t.Setenv("PUBSUB_EMULATOR_HOST", pubsubEmulatorEndpoint)

	bqContainer, bqEmulatorEndpoint, err := setupBigQueryEmulatorForCLI(t, ctx, cliTestProjectID)
	require.NoError(t, err, "BigQueryConfig emulator setup failed")
	defer func() { require.NoError(t, bqContainer.Terminate(ctx)) }()
	t.Setenv("BIGQUERY_API_ENDPOINT", bqEmulatorEndpoint)

	gcsContainer, gcsEmulatorEndpoint, err := setupGCSEmulatorForCLI(t, ctx)
	require.NoError(t, err, "GCS emulator setup failed")
	defer func() { require.NoError(t, gcsContainer.Terminate(ctx)) }()
	t.Setenv("STORAGE_EMULATOR_HOST", gcsEmulatorEndpoint)

	t.Setenv("GCP_PROJECT_ID", cliTestProjectID)
	tempTestConfigPath = createTestServiceConfigYAML(t, cliTestProjectID, cliTestEnvName)
	log.Info().Msg("All emulators started and environment configured.")

	// --- Step 1: Validate Command ---
	t.Run("ValidateConfig", func(t *testing.T) {
		stdout, stderr, err := runCLICommand(t, "validate", "--config", tempTestConfigPath, "--log-level", "debug")
		require.NoError(t, err, "validate command failed. Stderr: %s, Stdout: %s", stderr, stdout)
		assert.Contains(t, stdout, "Configuration file validated successfully")
	})

	gcsTestBucketName := fmt.Sprintf("sm-cli-lifecycle-bucket-%s", strings.ToLower(strings.ReplaceAll(cliTestProjectID, "-", "")))

	// --- Step 2: Apply Command (Create Resources) ---
	t.Run("ApplyConfig_Create", func(t *testing.T) {
		stdout, stderr, err := runCLICommand(t, "apply", "--config", tempTestConfigPath, "--env", cliTestEnvName, "--log-level", "debug")
		require.NoError(t, err, "apply command failed. Stderr: %s, Stdout: %s", stderr, stdout)
		assert.Contains(t, stdout, "Successfully applied configuration")

		bqClientOpts := []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(bqEmulatorEndpoint),
		}
		generalClientOpts := []option.ClientOption{option.WithoutAuthentication()}

		psClient, err := pubsub.NewClient(ctx, cliTestProjectID, generalClientOpts...)
		require.NoError(t, err)
		defer psClient.Close()
		topic1 := psClient.Topic("cli-topic-alpha")
		exists, err := topic1.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, exists)
		if exists {
			cfg, _ := topic1.Config(ctx)
			assert.Equal(t, "smctl-lifecycle-test", cfg.Labels["app"])
		}
		sub1 := psClient.Subscription("cli-sub-for-alpha")
		exists, err = sub1.Exists(ctx)
		require.NoError(t, err)
		assert.True(t, exists)
		if exists {
			subCfg, _ := sub1.Config(ctx)
			assert.Equal(t, topic1.String(), subCfg.Topic.String())
			assert.Equal(t, 45*time.Second, subCfg.AckDeadline)
		}

		bqClient, err := bigquery.NewClient(ctx, cliTestProjectID, bqClientOpts...)
		require.NoError(t, err)
		defer bqClient.Close()
		dataset := bqClient.Dataset("clitest_dataset_one")
		_, err = dataset.Metadata(ctx)
		require.NoError(t, err)
		table := dataset.Table("clitest_table_in_one")
		_, err = table.Metadata(ctx)
		require.NoError(t, err)

		storageClient, err := storage.NewClient(ctx, generalClientOpts...)
		require.NoError(t, err)
		defer storageClient.Close()
		_, err = storageClient.Bucket(gcsTestBucketName).Attrs(ctx)
		require.NoError(t, err)
	})

	// --- Step 3: Apply Command (Idempotency) ---
	t.Run("ApplyConfig_Idempotency", func(t *testing.T) {
		stdout, stderr, err := runCLICommand(t, "apply", "--config", tempTestConfigPath, "--env", cliTestEnvName, "--log-level", "debug")
		require.NoError(t, err, "apply command (idempotency) failed. Stderr: %s, Stdout: %s", stderr, stdout)
		assert.Contains(t, stdout, "Successfully applied configuration")
	})

	// --- Step 4: Destroy Command ---
	t.Run("DestroyConfig", func(t *testing.T) {
		stdout, stderr, err := runCLICommand(t, "destroy", "--config", tempTestConfigPath, "--env", cliTestEnvName, "--log-level", "debug")
		require.NoError(t, err, "destroy command failed. Stderr: %s, Stdout: %s", stderr, stdout)
		assert.Contains(t, stdout, "Attempted to destroy resources")

		bqClientOpts := []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(bqEmulatorEndpoint),
		}
		generalClientOpts := []option.ClientOption{option.WithoutAuthentication()}

		psClient, err := pubsub.NewClient(ctx, cliTestProjectID, generalClientOpts...)
		require.NoError(t, err)
		defer psClient.Close()
		exists, _ := psClient.Topic("cli-topic-alpha").Exists(ctx)
		assert.False(t, exists)
		exists, _ = psClient.Subscription("cli-sub-for-alpha").Exists(ctx)
		assert.False(t, exists)

		bqClient, err := bigquery.NewClient(ctx, cliTestProjectID, bqClientOpts...)
		require.NoError(t, err)
		defer bqClient.Close()
		_, err = bqClient.Dataset("clitest_dataset_one").Table("clitest_table_in_one").Metadata(ctx)
		assert.Error(t, err)

		d1meta, err := bqClient.Dataset("clitest_dataset_one").Metadata(ctx)
		if err != nil {
			assert.True(t, strings.Contains(err.Error(), "notFound"), "Expected BQ table notFound error, got: %v", err)
		} else {
			assert.Empty(t, d1meta)
		}

		storageClient, err := storage.NewClient(ctx, generalClientOpts...)
		require.NoError(t, err)
		defer storageClient.Close()
		_, err = storageClient.Bucket(gcsTestBucketName).Attrs(ctx)
		// Use errors.Is for sentinel error checking, or testify's assert.ErrorIs
		assert.True(t, errors.Is(err, storage.ErrBucketNotExist), "Expected storage.ErrBucketNotExist, got: %v", err)
	})
}
