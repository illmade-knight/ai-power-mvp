package consumers

import (
	"context"
	"sync"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
)

// ====================================================================================
// This file contains mocks for the interfaces defined in this package.
// These mocks are intended for use in unit tests of services that depend on
// the consumer pipeline.
// ====================================================================================

// MockMessageConsumer is a mock implementation of the MessageConsumer interface.
// It is designed to be used in unit tests to simulate a message source.
type MockMessageConsumer struct {
	msgChan  chan types.ConsumedMessage
	doneChan chan struct{}
	stopOnce sync.Once
}

// NewMockMessageConsumer creates a new mock consumer with a buffered channel.
func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		msgChan:  make(chan types.ConsumedMessage, bufferSize),
		doneChan: make(chan struct{}),
	}
}

// Messages returns the read-only channel for consuming messages.
func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage {
	return m.msgChan
}

// Start simulates the startup of a real consumer. It respects context cancellation
// to ensure that services can shut it down gracefully.
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	go func() {
		// Wait for the service to signal a shutdown by canceling the context.
		<-ctx.Done()
		// Trigger the mock's own stop procedure.
		m.Stop()
	}()
	return nil
}

// Stop gracefully closes the message and done channels. It uses sync.Once
// to ensure that this operation is safe to call multiple times, preventing panics.
func (m *MockMessageConsumer) Stop() error {
	m.stopOnce.Do(func() {
		close(m.msgChan)
		close(m.doneChan)
	})
	return nil
}

// Done returns the channel that signals when the consumer has fully stopped.
func (m *MockMessageConsumer) Done() <-chan struct{} {
	return m.doneChan
}

// Push is a test helper method to inject a message into the mock consumer's channel,
// simulating a message being received from a broker.
func (m *MockMessageConsumer) Push(msg types.ConsumedMessage) {
	// Use a select to prevent the test from hanging if the message channel is full or closed.
	select {
	case m.msgChan <- msg:
	default:
		// In a real test, you might log this or handle it if it's unexpected.
	}
}
