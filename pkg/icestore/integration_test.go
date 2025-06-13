//go:build integration

package icestore_test

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/illmade-knight/ai-power-mpv/pkg/icestore"
	//"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- Test Constants ---
const (
	testProjectID      = "icestore-test-project"
	testTopicID        = "icestore-test-topic"
	testSubscriptionID = "icestore-test-sub"
	testBucketName     = "icestore-test-bucket"
)

// --- Test-Specific Data Structures ---
// This represents the structure of the message we expect from Pub/Sub.
type TestMessage struct {
	DeviceEUI  string    `json:"device_eui"`
	LocationID string    `json:"location_id,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	Data       string    `json:"data"`
}

// ArchivableTestMessage is a wrapper that makes TestMessage compatible with the
// generic GCSBatchUploader by implementing the Batchable interface.
type ArchivableTestMessage struct {
	Key string `json:"-"` // Used internally to determine the file path.
	TestMessage
}

// GetBatchKey satisfies the icestore.Batchable interface.
func (m ArchivableTestMessage) GetBatchKey() string {
	return m.Key
}

// --- Test Main: Setup and Teardown for Emulators ---

func TestIceStorageService_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 1. Start Emulators ---
	log.Info().Msg("Setting up Pub/Sub emulator...")
	_, pubsubCleanup := setupPubSubEmulator(t, ctx)
	defer pubsubCleanup()

	log.Info().Msg("Setting up GCS emulator...")
	gcsCleanup := setupGCSEmulator(t, ctx)
	defer gcsCleanup()

	// --- 2. Initialize Service Components ---
	testLogger := log.With().Str("service", "icestore-integration-test").Logger()

	consumer, err := consumers.NewGooglePubSubConsumer(ctx, &consumers.GooglePubSubConsumerConfig{
		ProjectID:              testProjectID,
		SubscriptionID:         testSubscriptionID,
		MaxOutstandingMessages: 10,
		NumGoroutines:          2,
	}, testLogger)
	require.NoError(t, err)

	gcsClient, err := storage.NewClient(ctx, option.WithoutAuthentication(), option.WithEndpoint(os.Getenv("STORAGE_EMULATOR_HOST")))
	require.NoError(t, err)
	defer gcsClient.Close()

	// Create the bucket in the GCS emulator
	err = gcsClient.Bucket(testBucketName).Create(ctx, testProjectID, nil)
	require.NoError(t, err)

	gcsAdapter := icestore.NewGCSClientAdapter(gcsClient)
	// The uploader is now generic. We specify the type it will handle.
	uploader, err := icestore.NewGCSBatchUploader[ArchivableTestMessage](gcsAdapter, icestore.GCSBatchUploaderConfig{
		BucketName:   testBucketName,
		ObjectPrefix: "archived-data",
	}, testLogger)
	require.NoError(t, err)

	// The batcher must also be initialized with the same specific type.
	batcher := icestore.NewBatcher[ArchivableTestMessage](&icestore.BatcherConfig{
		BatchSize:    3,
		FlushTimeout: 5 * time.Second,
	}, uploader, testLogger)

	// This decoder now creates our new ArchivableTestMessage struct.
	testDecoder := func(payload []byte) (*ArchivableTestMessage, error) {
		var msg TestMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON payload: %w", err)
		}

		var batchKeyLocationPart string
		if msg.LocationID != "" {
			batchKeyLocationPart = msg.LocationID
		} else {
			batchKeyLocationPart = "deadletter"
		}

		year, month, day := 2025, 6, 15 // Use a fixed date for predictable paths
		batchKey := fmt.Sprintf("%d/%02d/%02d/%s", year, int(month), day, batchKeyLocationPart)

		return &ArchivableTestMessage{
			Key:         batchKey,
			TestMessage: msg,
		}, nil
	}

	// Assemble the service, specifying the concrete type.
	service, err := icestore.NewIceStorageService[ArchivableTestMessage](2, consumer, batcher, testDecoder, testLogger)
	require.NoError(t, err)

	// --- 3. Run the Service ---
	go func() {
		err := service.Start()
		assert.NoError(t, err, "Service.Start() should not return an error")
	}()

	// --- 4. Publish Test Messages ---
	publisherClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer publisherClient.Close()
	topic := publisherClient.Topic(testTopicID)
	defer topic.Stop()

	messagesToPublish := []TestMessage{
		{DeviceEUI: "dev-01", LocationID: "loc-a", Data: "msg1"},
		{DeviceEUI: "dev-02", LocationID: "loc-b", Data: "msg2"},
		{DeviceEUI: "dev-03", LocationID: "loc-a", Data: "msg3"},
		{DeviceEUI: "dev-04", LocationID: "loc-a", Data: "msg4"}, // Should trigger a flush for loc-a
		{DeviceEUI: "dev-05", LocationID: "loc-b", Data: "msg5"},
		{DeviceEUI: "dev-06", LocationID: "", Data: "msg6"}, // Deadletter
	}

	for _, msg := range messagesToPublish {
		data, _ := json.Marshal(msg)
		_, err := topic.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
		require.NoError(t, err)
	}
	log.Info().Int("count", len(messagesToPublish)).Msg("Published test messages")

	// --- 5. Stop the Service and Verify ---
	log.Info().Msg("Waiting for messages to be processed...")
	time.Sleep(6 * time.Second) // Wait longer than flush timeout to ensure remaining batches are flushed
	service.Stop()
	log.Info().Msg("Service stopped.")

	// Verify the contents of GCS
	archivedObjects, err := listGCSObjects(ctx, gcsClient.Bucket(testBucketName))
	require.NoError(t, err)
	require.Len(t, archivedObjects, 3, "Expected 3 files to be created (loc-a, loc-b, deadletter)")

	foundLocA, foundLocB, foundDeadletter := false, false, false
	for name, content := range archivedObjects {
		records, err := decompressAndScan(content)
		require.NoError(t, err)

		if strings.Contains(name, "loc-a") {
			foundLocA = true
			assert.Len(t, records, 3, "Expected 3 records in loc-a file")
			assertContainsDevice(t, records, "dev-01")
			assertContainsDevice(t, records, "dev-03")
			assertContainsDevice(t, records, "dev-04")
		} else if strings.Contains(name, "loc-b") {
			foundLocB = true
			assert.Len(t, records, 2, "Expected 2 records in loc-b file")
			assertContainsDevice(t, records, "dev-02")
			assertContainsDevice(t, records, "dev-05")
		} else if strings.Contains(name, "deadletter") {
			foundDeadletter = true
			assert.Len(t, records, 1, "Expected 1 record in deadletter file")
			assertContainsDevice(t, records, "dev-06")
		}
	}
	assert.True(t, foundLocA, "File for loc-a not found")
	assert.True(t, foundLocB, "File for loc-b not found")
	assert.True(t, foundDeadletter, "File for deadletter not found")

	log.Info().Msg("Integration test completed successfully!")
}

// --- Emulator Setup and Verification Helpers ---
func setupPubSubEmulator(t *testing.T, ctx context.Context) (string, func()) {
	t.Helper()
	req := testcontainers.ContainerRequest{Image: "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators", ExposedPorts: []string{"8085/tcp"}, Cmd: []string{"gcloud", "beta", "emulators", "pubsub", "start", fmt.Sprintf("--project=%s", testProjectID), "--host-port=0.0.0.0:8085"}, WaitingFor: wait.ForLog("INFO: Server started, listening on")}
	container, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true},
	)
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "8085/tcp")
	require.NoError(t, err)
	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port())
	t.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	adminClient, err := pubsub.NewClient(ctx, testProjectID)
	require.NoError(t, err)
	defer adminClient.Close()
	topic, err := adminClient.CreateTopic(ctx, testTopicID)
	require.NoError(t, err)
	_, err = adminClient.CreateSubscription(ctx, testSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
	require.NoError(t, err)
	return emulatorHost, func() { require.NoError(t, container.Terminate(ctx)) }
}

func setupGCSEmulator(t *testing.T, ctx context.Context) func() {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http"},
		// API CHANGE: Changed .WithStatusCode(200) to .WithExpectedStatusCode(200) for the newer API.
		WaitingFor: wait.ForHTTP("/health").WithPort("4443/tcp").WithExpectedStatusCode(200).WithStartupTimeout(60 * time.Second),
	}

	// API CHANGE: The LogConsumer is removed. The default logger is used.
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	t.Setenv("STORAGE_EMULATOR_HOST", endpoint)

	return func() {
		require.NoError(t, container.Terminate(ctx))
	}
}

func listGCSObjects(ctx context.Context, bucket *storage.BucketHandle) (map[string][]byte, error) {
	objects := make(map[string][]byte)
	it := bucket.Objects(ctx, nil)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		rc, err := bucket.Object(attrs.Name).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader for object %s: %w", attrs.Name, err)
		}
		content, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read object %s: %w", attrs.Name, err)
		}
		objects[attrs.Name] = content
	}
	return objects, nil
}

func decompressAndScan(data []byte) ([]ArchivableTestMessage, error) {
	gzReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()
	var records []ArchivableTestMessage
	scanner := bufio.NewScanner(gzReader)
	for scanner.Scan() {
		var record ArchivableTestMessage
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, scanner.Err()
}

func assertContainsDevice(t *testing.T, records []ArchivableTestMessage, eui string) {
	t.Helper()
	found := false
	for _, rec := range records {
		if rec.DeviceEUI == eui {
			found = true
			break
		}
	}
	assert.True(t, found, "Device EUI %s not found in records", eui)
}
