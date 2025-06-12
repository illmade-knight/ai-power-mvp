package bqstore_test

import (
	"context"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"sync"
)

// ====================================================================================
// Test Mocks & Helpers
// ====================================================================================

type testPayload struct {
	ID   int
	Data string
}

// MockDataBatchInserter is a mock implementation of bqstore.DataBatchInserter.
// It has been updated to include an InsertBatchFn field to allow for configurable
// success/failure behavior in tests.
type MockDataBatchInserter[T any] struct {
	mu            sync.Mutex
	receivedItems [][]*T
	callCount     int
	InsertBatchFn func(ctx context.Context, items []*T) error
}

func (m *MockDataBatchInserter[T]) InsertBatch(ctx context.Context, items []*T) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	m.receivedItems = append(m.receivedItems, items)

	// If a custom function is provided by the test, use it.
	if m.InsertBatchFn != nil {
		return m.InsertBatchFn(ctx, items)
	}

	// Default behavior is to succeed.
	return nil
}

func (m *MockDataBatchInserter[T]) Close() error { return nil }
func (m *MockDataBatchInserter[T]) GetReceivedItems() [][]*T {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedItems
}
func (m *MockDataBatchInserter[T]) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// MockMessageConsumer has been updated to correctly handle context cancellation.
type MockMessageConsumer struct {
	msgChan  chan types.ConsumedMessage
	doneChan chan struct{}
	stopOnce sync.Once
}

func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		msgChan:  make(chan types.ConsumedMessage, bufferSize),
		doneChan: make(chan struct{}),
	}
}
func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage { return m.msgChan }

// Start now launches a goroutine that respects the context cancellation,
// which is crucial for the service's graceful shutdown procedure.
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	go func() {
		// This goroutine waits for the parent context (from the service) to be canceled.
		<-ctx.Done()
		// When the context is canceled, it calls the mock consumer's Stop method.
		m.Stop()
	}()
	return nil
}

// Stop now uses sync.Once to prevent a panic from closing channels multiple times.
func (m *MockMessageConsumer) Stop() error {
	m.stopOnce.Do(func() {
		close(m.msgChan)
		close(m.doneChan)
	})
	return nil
}
func (m *MockMessageConsumer) Done() <-chan struct{} { return m.doneChan }
func (m *MockMessageConsumer) Push(msg types.ConsumedMessage) {
	// Use a select to avoid blocking if the channel is full or closed.
	select {
	case m.msgChan <- msg:
	default:
		// In a test, we might want to know if this happens.
	}
}
