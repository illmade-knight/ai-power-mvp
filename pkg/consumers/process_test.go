package consumers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Implementations ---

// mockMessageConsumer is a mock implementation of the MessageConsumer interface.
type mockMessageConsumer struct {
	sync.Mutex
	messagesChan chan types.ConsumedMessage
	doneChan     chan struct{}
	startCalled  bool
	stopCalled   bool
	startError   error
}

func newMockConsumer() *mockMessageConsumer {
	return &mockMessageConsumer{
		messagesChan: make(chan types.ConsumedMessage, 10),
		doneChan:     make(chan struct{}),
	}
}

func (m *mockMessageConsumer) Messages() <-chan types.ConsumedMessage {
	return m.messagesChan
}

func (m *mockMessageConsumer) Start(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()
	m.startCalled = true
	return m.startError
}

func (m *mockMessageConsumer) Stop() error {
	m.Lock()
	defer m.Unlock()
	m.stopCalled = true
	// In a real scenario, Stop would trigger the closing of doneChan.
	// For this mock, we close it manually to signal completion.
	close(m.doneChan)
	return nil
}

func (m *mockMessageConsumer) Done() <-chan struct{} {
	return m.doneChan
}

// mockMessageProcessor is a mock implementation of the MessageProcessor interface.
type mockMessageProcessor[T any] struct {
	sync.Mutex
	inputChan   chan *types.BatchedMessage[T]
	received    []*types.BatchedMessage[T]
	startCalled bool
	stopCalled  bool
}

func newMockProcessor[T any]() *mockMessageProcessor[T] {
	return &mockMessageProcessor[T]{
		inputChan: make(chan *types.BatchedMessage[T], 10),
	}
}

func (m *mockMessageProcessor[T]) Input() chan<- *types.BatchedMessage[T] {
	return m.inputChan
}

func (m *mockMessageProcessor[T]) Start() {
	m.Lock()
	defer m.Unlock()
	m.startCalled = true
}

func (m *mockMessageProcessor[T]) Stop() {
	m.Lock()
	defer m.Unlock()
	m.stopCalled = true
	close(m.inputChan)
}

// --- Test Cases ---

type testPayload struct {
	Data string
}

// TestProcessingService_Lifecycle verifies the Start and Stop sequences.
func TestProcessingService_Lifecycle(t *testing.T) {
	consumer := newMockConsumer()
	processor := newMockProcessor[testPayload]()
	transformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return &testPayload{Data: string(msg.Payload)}, false, nil
	}

	service, err := NewProcessingService[testPayload](1, consumer, processor, transformer, zerolog.Nop())
	require.NoError(t, err)

	// Start the service
	err = service.Start()
	require.NoError(t, err)

	// Verify components were started
	assert.True(t, consumer.startCalled, "consumer.Start() should be called")
	assert.True(t, processor.startCalled, "processor.Start() should be called")

	// Stop the service
	service.Stop()
	assert.True(t, consumer.stopCalled, "consumer.Stop() should be called")
	assert.True(t, processor.stopCalled, "processor.Stop() should be called")
}

// TestProcessingService_ProcessMessage_Success verifies a message is successfully
// transformed and sent to the processor.
func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	consumer := newMockConsumer()
	processor := newMockProcessor[testPayload]()
	transformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return &testPayload{Data: "transformed"}, false, nil
	}

	service, err := NewProcessingService[testPayload](1, consumer, processor, transformer, zerolog.Nop())
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	// Send a message to the consumer
	nackCalled := false
	msg := types.ConsumedMessage{
		ID:      "test-msg-1",
		Payload: []byte("original"),
		Ack:     func() {}, // Provide a no-op Ack for completeness
		Nack:    func() { nackCalled = true },
	}
	consumer.messagesChan <- msg

	// Read from the processor's input to confirm receipt
	receivedMsg := <-processor.inputChan
	assert.Equal(t, "transformed", receivedMsg.Payload.Data)
	assert.Equal(t, "test-msg-1", receivedMsg.OriginalMessage.ID)

	// The processor acks the message via the batcher logic, so the service itself
	// should not call Ack or Nack on a successful handoff.
	assert.False(t, nackCalled, "Nack should not be called on success")
}

// TestProcessingService_ProcessMessage_TransformerError verifies that a Nack is called
// when the transformer returns an error.
func TestProcessingService_ProcessMessage_TransformerError(t *testing.T) {
	consumer := newMockConsumer()
	processor := newMockProcessor[testPayload]()
	transformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	service, err := NewProcessingService[testPayload](1, consumer, processor, transformer, zerolog.Nop())
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	ackCalled := false
	nackCalled := false
	msg := types.ConsumedMessage{
		ID:   "test-msg-err",
		Ack:  func() { ackCalled = true },
		Nack: func() { nackCalled = true },
	}
	consumer.messagesChan <- msg

	// Give the worker a moment to process and Nack
	time.Sleep(50 * time.Millisecond)

	assert.True(t, nackCalled, "Nack should be called on transformer error")
	assert.False(t, ackCalled, "Ack should not be called")
	assert.Len(t, processor.inputChan, 0, "Processor should not receive any message")
}

// TestProcessingService_ProcessMessage_Skip verifies that an Ack is called
// when the transformer signals to skip a message.
func TestProcessingService_ProcessMessage_Skip(t *testing.T) {
	consumer := newMockConsumer()
	processor := newMockProcessor[testPayload]()
	transformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		// Signal to skip this message
		return nil, true, nil
	}

	service, err := NewProcessingService[testPayload](1, consumer, processor, transformer, zerolog.Nop())
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	ackCalled := false
	nackCalled := false
	msg := types.ConsumedMessage{
		ID:   "test-msg-skip",
		Ack:  func() { ackCalled = true },
		Nack: func() { nackCalled = true },
	}
	consumer.messagesChan <- msg

	// Give the worker a moment to process and Ack
	time.Sleep(50 * time.Millisecond)

	assert.True(t, ackCalled, "Ack should be called when skipping a message")
	assert.False(t, nackCalled, "Nack should not be called")
	assert.Len(t, processor.inputChan, 0, "Processor should not receive any message on skip")
}
