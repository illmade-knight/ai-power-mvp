package mqttconverter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks ---

// MockPublishedMessage stores the data that was passed to the publisher's Publish method.
type MockPublishedMessage struct {
	Topic      string
	Payload    []byte
	Attributes map[string]string
}

// MockPublisher is a mock implementation of the MessagePublisher interface for testing.
// It is safe for concurrent use.
type MockPublisher struct {
	mu           sync.Mutex
	Messages     []MockPublishedMessage
	stopCalled   bool
	PublishError error // Set this to simulate a publish error
}

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{
		Messages: make([]MockPublishedMessage, 0),
	}
}

func (m *MockPublisher) Publish(_ context.Context, mqttTopic string, payload []byte, attributes map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.PublishError != nil {
		return m.PublishError
	}

	m.Messages = append(m.Messages, MockPublishedMessage{
		Topic:      mqttTopic,
		Payload:    payload,
		Attributes: attributes,
	})
	return nil
}

func (m *MockPublisher) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
}

func (m *MockPublisher) GetMessageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Messages)
}

// MockExtractor is a mock implementation of the AttributeExtractor interface.
type MockExtractor struct {
	AttributesToReturn map[string]string
	ErrorToReturn      error
}

func (m *MockExtractor) Extract(_ []byte) (map[string]string, error) {
	if m.ErrorToReturn != nil {
		return nil, m.ErrorToReturn
	}
	return m.AttributesToReturn, nil
}

// --- Tests ---

// setupTestService is a helper to initialize the service with mocks for testing.
func setupTestService(t *testing.T, extractor AttributeExtractor) (*IngestionService, *MockPublisher) {
	t.Helper()

	cfg := DefaultIngestionServiceConfig()
	logger := zerolog.Nop() // Disable logging during tests
	publisher := NewMockPublisher()

	// We pass nil for the MQTTClientConfig because we are not testing MQTT connectivity,
	// only the service's internal processing logic.
	service := NewIngestionService(publisher, extractor, logger, cfg, nil)
	require.NotNil(t, service, "NewIngestionService should not return nil")

	return service, publisher
}

// TestProcessSingleMessage ensures the core logic of processing one message works correctly.
func TestProcessSingleMessage(t *testing.T) {
	t.Run("With Extractor Success", func(t *testing.T) {
		// Arrange
		expectedAttrs := map[string]string{"device_eui": "test-eui", "source": "test"}
		extractor := &MockExtractor{AttributesToReturn: expectedAttrs}
		service, publisher := setupTestService(t, extractor)

		payload := []byte(`{"data":"test"}`)
		msg := InMessage{Payload: payload, Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		assert.Equal(t, 1, publisher.GetMessageCount(), "Publisher should have received one message")
		publishedMsg := publisher.Messages[0]
		assert.Equal(t, msg.Topic, publishedMsg.Topic)
		assert.Equal(t, msg.Payload, publishedMsg.Payload)
		assert.Equal(t, expectedAttrs, publishedMsg.Attributes)
	})

	t.Run("Without Extractor (nil)", func(t *testing.T) {
		// Arrange
		service, publisher := setupTestService(t, nil) // No extractor
		payload := []byte(`{"data":"test"}`)
		msg := InMessage{Payload: payload, Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		assert.Equal(t, 1, publisher.GetMessageCount())
		publishedMsg := publisher.Messages[0]
		assert.Equal(t, msg.Topic, publishedMsg.Topic)
		assert.Equal(t, msg.Payload, publishedMsg.Payload)
		assert.Empty(t, publishedMsg.Attributes, "Attributes should be empty when no extractor is provided")
	})

	t.Run("With Extractor Error", func(t *testing.T) {
		// Arrange
		extractor := &MockExtractor{ErrorToReturn: errors.New("extraction failed")}
		service, publisher := setupTestService(t, extractor)
		payload := []byte(`bad-json`)
		msg := InMessage{Payload: payload, Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		assert.Equal(t, 1, publisher.GetMessageCount(), "Message should still be published even if extraction fails")
		publishedMsg := publisher.Messages[0]
		assert.Equal(t, msg.Topic, publishedMsg.Topic)
		assert.Equal(t, msg.Payload, publishedMsg.Payload)
		assert.Empty(t, publishedMsg.Attributes, "Attributes should be empty when extraction fails")
	})

	t.Run("With Publisher Error", func(t *testing.T) {
		// Arrange
		service, publisher := setupTestService(t, nil)
		publisher.PublishError = errors.New("pubsub is down")
		payload := []byte(`{"data":"test"}`)
		msg := InMessage{Payload: payload, Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		assert.Equal(t, 0, publisher.GetMessageCount(), "Message should not be published if publisher returns an error")
		// Check if the error was sent to the ErrorChan
		select {
		case err := <-service.ErrorChan:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "pubsub is down")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected an error on the ErrorChan but got none")
		}
	})
}

// TestService_E2E simulates the full internal pipeline from message arrival to publishing.
func TestService_E2E(t *testing.T) {
	// Arrange
	extractor := &MockExtractor{AttributesToReturn: map[string]string{"processed": "true"}}
	service, publisher := setupTestService(t, extractor)

	// Start the service's processing workers
	err := service.Start()
	require.NoError(t, err)

	// The service is now running its workers in the background.
	// We will simulate a message arriving from Paho.

	// Act
	// Simulate an incoming message by pushing it to the input channel.
	// This mimics what handleIncomingPahoMessage does.
	payload := []byte(`{"message": "e2e-test"}`)
	topic := "e2e/topic"
	msg := InMessage{
		Payload:   payload,
		Topic:     topic,
		MessageID: "123",
	}
	service.MessagesChan <- msg

	// Assert
	// The message is processed asynchronously by a worker. We need to wait
	// until we are sure it has been processed.
	// `require.Eventually` is perfect for this. It will check the condition
	// repeatedly for a short period.
	require.Eventually(t, func() bool {
		return publisher.GetMessageCount() == 1
	}, 1*time.Second, 10*time.Millisecond, "Expected one message to be published")

	// Verify the content of the published message
	publishedMsg := publisher.Messages[0]
	assert.Equal(t, topic, publishedMsg.Topic)
	assert.Equal(t, payload, publishedMsg.Payload)
	assert.Equal(t, extractor.AttributesToReturn, publishedMsg.Attributes)

	// Cleanup
	service.Stop()
	assert.True(t, publisher.stopCalled, "Publisher's Stop method should be called during service shutdown")
}

// TestService_Stop ensures that the service shuts down gracefully.
func TestService_Stop(t *testing.T) {
	// Arrange
	service, publisher := setupTestService(t, nil)
	err := service.Start()
	require.NoError(t, err)

	// Fill the channel with a few messages
	for i := 0; i < 5; i++ {
		service.MessagesChan <- InMessage{Payload: []byte(fmt.Sprintf("msg-%d", i))}
	}

	// Act
	service.Stop() // Stop the service

	// Assert
	// 1. The input channel should be closed. Trying to send to it will panic.
	assert.Panics(t, func() {
		service.MessagesChan <- InMessage{Payload: []byte("after-stop")}
	}, "Sending to a closed channel should panic")

	// 2. All messages that were in the channel before Stop() was called should be processed.
	assert.Equal(t, 5, publisher.GetMessageCount(), "All buffered messages should be processed on shutdown")

	// 3. The publisher's Stop method should have been called.
	assert.True(t, publisher.stopCalled, "Publisher's Stop method should be called")
}
