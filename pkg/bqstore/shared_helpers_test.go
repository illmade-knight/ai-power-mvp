package bqstore_test

import (
	"context"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"sync"
)

// --- Test-Specific Data Structures ---

// testPayload is a sample data structure used across tests.
type testPayload struct {
	ID   int    `bigquery:"id"`
	Data string `bigquery:"data"`
}

// --- Mock Implementations ---

// MockDataBatchInserter is a mock implementation of the DataBatchInserter interface
// used for testing the BatchInserter and ProcessingService.
type MockDataBatchInserter[T any] struct {
	mu            sync.Mutex
	InsertBatchFn func(ctx context.Context, items []*T) error
	CloseFn       func() error
	callCount     int
	receivedItems [][]*T
}

func (m *MockDataBatchInserter[T]) InsertBatch(ctx context.Context, items []*T) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	// Store a copy of the received items for verification.
	itemsCopy := make([]*T, len(items))
	copy(itemsCopy, items)
	m.receivedItems = append(m.receivedItems, itemsCopy)

	if m.InsertBatchFn != nil {
		return m.InsertBatchFn(ctx, items)
	}
	return nil
}

func (m *MockDataBatchInserter[T]) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

func (m *MockDataBatchInserter[T]) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func (m *MockDataBatchInserter[T]) GetReceivedItems() [][]*T {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedItems
}

// MockMessageConsumer is a mock implementation of the MessageConsumer interface.
type MockMessageConsumer struct {
	mu         sync.Mutex
	messagesCh chan types.ConsumedMessage
	doneCh     chan struct{}
	stopped    bool
}

// NewMockMessageConsumer creates an instance of the mock consumer.
func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		messagesCh: make(chan types.ConsumedMessage, bufferSize),
		doneCh:     make(chan struct{}),
	}
}
func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage {
	return m.messagesCh
}
func (m *MockMessageConsumer) Start(ctx context.Context) error { return nil }
func (m *MockMessageConsumer) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stopped {
		close(m.messagesCh)
		close(m.doneCh)
		m.stopped = true
	}
	return nil
}
func (m *MockMessageConsumer) Done() <-chan struct{} { return m.doneCh }
func (m *MockMessageConsumer) Push(msg types.ConsumedMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stopped {
		m.messagesCh <- msg
	}
}
