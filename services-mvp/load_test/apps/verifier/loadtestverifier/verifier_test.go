package loadtestverifier

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/mock"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test NewVerifier ---
func TestNewVerifier(t *testing.T) {
	baseLogger := zerolog.Nop()
	validConfig := VerifierConfig{
		ProjectID:           "test-project",
		EnrichedTopicID:     "enriched-topic",
		UnidentifiedTopicID: "unidentified-topic",
		TestDuration:        1 * time.Minute,
		LogLevel:            "debug",
	}

	t.Run("ValidConfig", func(t *testing.T) {
		verifier, err := NewVerifier(validConfig, baseLogger)
		require.NoError(t, err)
		require.NotNil(t, verifier)
		assert.Equal(t, validConfig.ProjectID, verifier.config.ProjectID)
		assert.Equal(t, zerolog.DebugLevel, verifier.logger.GetLevel(), "Logger level should be debug")
	})

	t.Run("InvalidLogLevel", func(t *testing.T) {
		cfgWithInvalidLog := validConfig
		cfgWithInvalidLog.LogLevel = "invalid-level"
		verifier, err := NewVerifier(cfgWithInvalidLog, baseLogger)
		require.NoError(t, err) // NewVerifier currently doesn't error on invalid log level, just warns
		require.NotNil(t, verifier)
		assert.Equal(t, zerolog.Nop().GetLevel(), verifier.logger.GetLevel(), "Logger level should default if invalid")
	})

	t.Run("MissingProjectID", func(t *testing.T) {
		cfg := validConfig
		cfg.ProjectID = ""
		_, err := NewVerifier(cfg, baseLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "projectID is required")
	})

	t.Run("MissingEnrichedTopicID", func(t *testing.T) {
		cfg := validConfig
		cfg.EnrichedTopicID = ""
		_, err := NewVerifier(cfg, baseLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "enrichedTopicID is required")
	})

	t.Run("MissingUnidentifiedTopicID", func(t *testing.T) {
		cfg := validConfig
		cfg.UnidentifiedTopicID = ""
		_, err := NewVerifier(cfg, baseLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unidentifiedTopicID is required")
	})

	t.Run("InvalidTestDuration", func(t *testing.T) {
		cfg := validConfig
		cfg.TestDuration = 0
		_, err := NewVerifier(cfg, baseLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "testDuration must be positive")

		cfg.TestDuration = -1 * time.Second
		_, err = NewVerifier(cfg, baseLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "testDuration must be positive")
	})
}

// --- Test TestRunResults.SaveResults ---
func TestTestRunResults_SaveResults(t *testing.T) {
	logger := zerolog.Nop()
	results := TestRunResults{
		TestRunID:             "test-save-run",
		TotalMessagesVerified: 1,
		Records: []MessageVerificationRecord{
			{ClientMessageID: "msg-1", LatencyMillis: 100},
		},
	}

	tempDir := t.TempDir()
	outputFilePath := filepath.Join(tempDir, "results.json")

	t.Run("SuccessfulSave", func(t *testing.T) {
		err := results.SaveResults(outputFilePath, logger)
		require.NoError(t, err)

		_, err = os.Stat(outputFilePath)
		assert.NoError(t, err, "Output file should exist")

		data, err := os.ReadFile(outputFilePath)
		require.NoError(t, err)
		var readResults TestRunResults
		err = json.Unmarshal(data, &readResults)
		require.NoError(t, err)
		assert.Equal(t, results.TestRunID, readResults.TestRunID)
		assert.Len(t, readResults.Records, 1)
		assert.Equal(t, results.Records[0].ClientMessageID, readResults.Records[0].ClientMessageID)
	})

	t.Run("SaveToInvalidPath", func(t *testing.T) {
		invalidPath := filepath.Join(tempDir, "nonexistent_dir", "results.json")
		err := results.SaveResults(invalidPath, logger)
		assert.Error(t, err, "Should error when path is invalid")
	})
}

// --- Test processMessage ---
func TestProcessMessage(t *testing.T) {
	logger := zerolog.Nop()
	var recordsLock sync.Mutex

	originalTime := time.Now().UTC().Add(-10 * time.Second) // 10 seconds ago
	pubsubPublishTime := originalTime.Add(100 * time.Millisecond)
	clientMsgID := uuid.NewString()
	deviceEUI := "TESTEUI001"

	validPayload := IngestedMessagePayload{
		DeviceEUI:        deviceEUI,
		MessageTimestamp: originalTime,
		ClientMessageID:  clientMsgID,
	}
	validPayloadBytes, _ := json.Marshal(validPayload)

	t.Run("ValidMessageEnriched", func(t *testing.T) {
		var records []MessageVerificationRecord
		enrichedCount := 0
		unidentifiedCount := 0
		// Create a real *pubsub.Message
		msg := &pubsub.Message{
			ID:          "msg1",
			Data:        validPayloadBytes,
			PublishTime: pubsubPublishTime,
			// Ack() will be called on this real message. We can't easily mock it here
			// without an interface or more complex mocking.
		}

		processMessage(msg, "enriched", logger, &records, &enrichedCount, &unidentifiedCount, &recordsLock)

		// We can't easily verify msg.Ack() was called without more advanced techniques or interfaces.
		// For this unit test, we assume it's called and focus on other logic.
		require.Len(t, records, 1)
		assert.Equal(t, 1, enrichedCount)
		assert.Equal(t, 0, unidentifiedCount)

		record := records[0]
		assert.Equal(t, clientMsgID, record.ClientMessageID)
		assert.Equal(t, deviceEUI, record.DeviceEUI)
		assert.Equal(t, "enriched", record.TopicSource)
		assert.Equal(t, originalTime, record.OriginalClientTimestamp)
		assert.WithinDuration(t, time.Now().UTC(), record.VerifierReceiveTime, 2*time.Second) // Increased tolerance slightly
		assert.Equal(t, pubsubPublishTime, record.PubSubPublishTime)
		expectedProcessingLatency := pubsubPublishTime.Sub(originalTime).Milliseconds()
		assert.Equal(t, expectedProcessingLatency, record.ProcessingLatencyMillis)
		assert.GreaterOrEqual(t, record.LatencyMillis, record.ProcessingLatencyMillis)
	})

	t.Run("ValidMessageUnidentified", func(t *testing.T) {
		var records []MessageVerificationRecord
		enrichedCount := 0
		unidentifiedCount := 0
		msg := &pubsub.Message{
			ID:          "msg2",
			Data:        validPayloadBytes,
			PublishTime: pubsubPublishTime,
		}

		processMessage(msg, "unidentified", logger, &records, &enrichedCount, &unidentifiedCount, &recordsLock)

		require.Len(t, records, 1)
		assert.Equal(t, 0, enrichedCount)
		assert.Equal(t, 1, unidentifiedCount)
		assert.Equal(t, "unidentified", records[0].TopicSource)
	})

	t.Run("InvalidJSONPayload", func(t *testing.T) {
		var records []MessageVerificationRecord
		enrichedCount := 0
		unidentifiedCount := 0
		msg := &pubsub.Message{
			ID:          "msg3",
			Data:        []byte("this is not json"),
			PublishTime: pubsubPublishTime,
		}

		processMessage(msg, "enriched", logger, &records, &enrichedCount, &unidentifiedCount, &recordsLock)

		assert.Len(t, records, 0, "No record should be created for invalid JSON")
		assert.Equal(t, 0, enrichedCount)
		assert.Equal(t, 0, unidentifiedCount)
	})

	t.Run("ZeroOriginalClientTimestamp", func(t *testing.T) {
		var records []MessageVerificationRecord
		enrichedCount := 0
		unidentifiedCount := 0
		payloadWithZeroTime := IngestedMessagePayload{
			DeviceEUI:        deviceEUI,
			MessageTimestamp: time.Time{}, // Zero time
			ClientMessageID:  clientMsgID,
		}
		payloadBytes, _ := json.Marshal(payloadWithZeroTime)
		msg := &pubsub.Message{
			ID:          "msg4",
			Data:        payloadBytes,
			PublishTime: pubsubPublishTime,
		}

		processMessage(msg, "enriched", logger, &records, &enrichedCount, &unidentifiedCount, &recordsLock)

		assert.Len(t, records, 0, "No record should be created if OriginalClientTimestamp is zero")
		assert.Equal(t, 0, enrichedCount)
	})

	t.Run("EmptyClientMessageID", func(t *testing.T) {
		var records []MessageVerificationRecord
		enrichedCount := 0
		unidentifiedCount := 0
		payloadWithEmptyID := IngestedMessagePayload{
			DeviceEUI:        deviceEUI,
			MessageTimestamp: originalTime,
			ClientMessageID:  "", // Empty ClientMessageID
		}
		payloadBytes, _ := json.Marshal(payloadWithEmptyID)
		msg := &pubsub.Message{
			ID:          "msg5",
			Data:        payloadBytes,
			PublishTime: pubsubPublishTime,
		}

		processMessage(msg, "enriched", logger, &records, &enrichedCount, &unidentifiedCount, &recordsLock)

		require.Len(t, records, 1, "Record should still be created even with empty ClientMessageID")
		assert.Equal(t, "", records[0].ClientMessageID)
		assert.Equal(t, 1, enrichedCount)
	})
}

// --- Mocks for Pub/Sub Client, Topic, Subscription (for TODO tests) ---
// These are kept for future tests of createTemporarySubscription, deleteSubscription, and Run.
// To make them fully effective, the functions they test would ideally accept interfaces.

type MockPubSubClient struct {
	mock.Mock
	projectID string
}

func (m *MockPubSubClient) Topic(id string) *pubsub.Topic {
	args := m.Called(id)
	if args.Get(0) == nil {
		// This is complex because pubsub.Topic is a concrete type.
		// For a unit test, one might return a Topic that has its own methods mocked,
		// or the function using Topic needs to accept an interface.
		return nil // Or a more sophisticated mock if methods on Topic are called
	}
	return args.Get(0).(*pubsub.Topic)
}

func (m *MockPubSubClient) CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	args := m.Called(ctx, id, cfg)
	sub, _ := args.Get(0).(*pubsub.Subscription)
	return sub, args.Error(1)
}

func (m *MockPubSubClient) Project() string {
	return m.projectID
}

func (m *MockPubSubClient) Close() error { // Added to satisfy a potential interface if Verifier.pubsubClient becomes one
	args := m.Called()
	return args.Error(0)
}

type MockPubSubTopic struct {
	mock.Mock
	id string
	// We would need to embed *pubsub.Topic or mock all methods used by SUT
	// For Exists():
	// realTopic *pubsub.Topic // to delegate calls if needed
}

func (m *MockPubSubTopic) Exists(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}
func (m *MockPubSubTopic) ID() string { return m.id }

// MockPubSubSubscription implements parts of pubsub.Subscription
type MockPubSubSubscription struct {
	mock.Mock
	id string
	// realSub *pubsub.Subscription // to delegate calls if needed
}

func (m *MockPubSubSubscription) ID() string {
	return m.id
}

func (m *MockPubSubSubscription) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPubSubSubscription) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
	args := m.Called(ctx, f)
	// To simulate message delivery, the mock could call f with a test message.
	// Example:
	// if !args.Bool(1) { // Control whether to deliver a message
	//  testMsgData, _ := json.Marshal(IngestedMessagePayload{DeviceEUI: "simulated-device"})
	//	testMsg := &pubsub.Message{MessageID: "sim-msg-1", Data: testMsgData, PublishTime: time.Now()}
	//	f(ctx, testMsg)
	// }
	return args.Error(0)
}

// --- Test createTemporarySubscription ---
func TestCreateTemporarySubscription(t *testing.T) {
	//logger := zerolog.Nop()
	//ctx := context.Background()
	//projectID := "test-project"
	//topicID := "test-topic"

	t.Run("SuccessfulCreation", func(t *testing.T) {
		// This test remains challenging for pure unit testing due to concrete types.
		// It's better tested with an emulator or by refactoring the SUT.
		t.Log("Skipping TestCreateTemporarySubscription pure unit test due to concrete Pub/Sub types. Test with emulator or refactor SUT.")
	})
	// TODO: Add tests for createTemporarySubscription with topic not existing, creation failure, using an interface approach.
}

// --- Test deleteSubscription ---
func TestDeleteSubscription(t *testing.T) {
	//logger := zerolog.Nop()
	//ctx := context.Background()

	t.Run("SuccessfulDeletion", func(t *testing.T) {
		// This test also remains challenging for pure unit testing.
		t.Log("Skipping TestDeleteSubscription pure unit test due to concrete Pub/Sub types. Test with emulator or refactor SUT.")
	})
	// TODO: Add tests for deleteSubscription with nil sub, deletion failure, using an interface approach.
}

// --- Test Verifier.Run ---
func TestVerifier_Run(t *testing.T) {
	// This test requires significant mocking of Pub/Sub interactions.
	// The ideal approach involves refactoring Verifier.Run and its helpers
	// to use interfaces for Pub/Sub client, topic, and subscription operations.
	t.Log("TODO: Implement unit tests for Verifier.Run using a more mockable design (interfaces for Pub/Sub interactions).")
}
