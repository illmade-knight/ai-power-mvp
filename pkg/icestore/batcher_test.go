package icestore

import (
	"context"
	"errors"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ====================================================================================
// Mocks for Testing
// ====================================================================================

// mockUploader is a mock implementation of the DataBatchUploader interface.
// It allows us to assert that UploadBatch is called with the correct data
// and to simulate success or failure.
type mockUploader[T any] struct {
	mock.Mock
}

func (m *mockUploader[T]) UploadBatch(ctx context.Context, items []*T) error {
	// Register the call with the mock framework, including the arguments.
	args := m.Called(ctx, items)
	// Return the first value, which is the error to be returned by the mock.
	return args.Error(0)
}

func (m *mockUploader[T]) Close() error {
	args := m.Called()
	return args.Error(0)
}

// ====================================================================================
// Test Cases for Batcher
// ====================================================================================

// newTestLogger creates a disabled logger to avoid noisy test output.
func newTestLogger() zerolog.Logger {
	return zerolog.New(io.Discard)
}

func TestBatcher_FlushByBatchSize(t *testing.T) {
	// Arrange
	uploader := new(mockUploader[ArchivalData])
	config := &BatcherConfig{
		BatchSize:    3,
		FlushTimeout: 1 * time.Minute, // Long timeout to ensure size-based flush
	}
	logger := newTestLogger()

	// We expect UploadBatch to be called once with a slice of 3 items.
	// We configure the mock to return `nil` (no error).
	uploader.On("UploadBatch", mock.Anything, mock.AnythingOfType("[]*icestore.ArchivalData")).Return(nil).Run(func(args mock.Arguments) {
		// Custom assertion to check the size of the batch passed to UploadBatch.
		items := args.Get(1).([]*ArchivalData)
		assert.Len(t, items, 3)
	})

	batcher := NewBatcher[ArchivalData](config, uploader, logger)
	batcher.Start()
	defer batcher.Stop() // Ensure cleanup

	// Act
	var ackCount, nackCount int
	var mu sync.Mutex
	ackFunc := func() { mu.Lock(); ackCount++; mu.Unlock() }
	nackFunc := func() { mu.Lock(); nackCount++; mu.Unlock() }

	// Send exactly 3 messages to trigger the flush by size.
	for i := 0; i < 3; i++ {
		msg := types.ConsumedMessage{Ack: ackFunc, Nack: nackFunc}
		payload := &ArchivalData{} // Dummy payload
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{OriginalMessage: msg, Payload: payload}
	}

	// Assert
	// Allow some time for the batch to be processed.
	time.Sleep(100 * time.Millisecond)
	uploader.AssertExpectations(t) // Verify that UploadBatch was called as expected.

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 3, ackCount, "Expected 3 messages to be Acked")
	assert.Equal(t, 0, nackCount, "Expected 0 messages to be Nacked")
}

func TestBatcher_FlushByTimeout(t *testing.T) {
	// Arrange
	uploader := new(mockUploader[ArchivalData])
	config := &BatcherConfig{
		BatchSize:    10,                    // Large batch size
		FlushTimeout: 50 * time.Millisecond, // Short timeout
	}
	logger := newTestLogger()

	// Expect UploadBatch to be called with a slice of exactly 2 items due to timeout.
	uploader.On("UploadBatch", mock.Anything, mock.AnythingOfType("[]*icestore.ArchivalData")).Return(nil).Run(func(args mock.Arguments) {
		items := args.Get(1).([]*ArchivalData)
		assert.Len(t, items, 2)
	})

	batcher := NewBatcher[ArchivalData](config, uploader, logger)
	batcher.Start()
	defer batcher.Stop()

	// Act
	var ackCount, nackCount int
	var mu sync.Mutex
	ackFunc := func() { mu.Lock(); ackCount++; mu.Unlock() }
	nackFunc := func() { mu.Lock(); nackCount++; mu.Unlock() }

	// Send 2 messages, fewer than the batch size.
	for i := 0; i < 2; i++ {
		msg := types.ConsumedMessage{Ack: ackFunc, Nack: nackFunc}
		payload := &ArchivalData{}
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{OriginalMessage: msg, Payload: payload}
	}

	// Assert
	// Wait for longer than the flush timeout.
	time.Sleep(100 * time.Millisecond)
	uploader.AssertExpectations(t)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, ackCount, "Expected 2 messages to be Acked")
	assert.Equal(t, 0, nackCount, "Expected 0 messages to be Nacked")
}

func TestBatcher_FlushOnStop(t *testing.T) {
	// Arrange
	uploader := new(mockUploader[ArchivalData])
	config := &BatcherConfig{
		BatchSize:    10,
		FlushTimeout: 1 * time.Minute,
	}
	logger := newTestLogger()

	// Expect UploadBatch to be called on Stop with the pending 2 items.
	uploader.On("UploadBatch", mock.Anything, mock.AnythingOfType("[]*icestore.ArchivalData")).Return(nil).Run(func(args mock.Arguments) {
		items := args.Get(1).([]*ArchivalData)
		assert.Len(t, items, 2)
	})
	// Expect Close to be called on the uploader during Stop.
	uploader.On("Close").Return(nil)

	batcher := NewBatcher[ArchivalData](config, uploader, logger)
	batcher.Start()

	// Act
	var ackCount, nackCount int
	var mu sync.Mutex
	ackFunc := func() { mu.Lock(); ackCount++; mu.Unlock() }
	nackFunc := func() { mu.Lock(); nackCount++; mu.Unlock() }

	// Send 2 messages.
	for i := 0; i < 2; i++ {
		msg := types.ConsumedMessage{Ack: ackFunc, Nack: nackFunc}
		payload := &ArchivalData{}
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{OriginalMessage: msg, Payload: payload}
	}

	// Give a moment for items to enter the channel before stopping.
	time.Sleep(20 * time.Millisecond)
	batcher.Stop() // Trigger flush on stop

	// Assert
	uploader.AssertExpectations(t)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, ackCount, "Expected 2 messages to be Acked")
	assert.Equal(t, 0, nackCount, "Expected 0 messages to be Nacked")
}

func TestBatcher_NacksOnUploadFailure(t *testing.T) {
	// Arrange
	uploader := new(mockUploader[ArchivalData])
	config := &BatcherConfig{
		BatchSize:    2,
		FlushTimeout: 1 * time.Minute,
	}
	logger := newTestLogger()
	uploadError := errors.New("simulated upload failure")

	// Configure the mock uploader to return an error.
	uploader.On("UploadBatch", mock.Anything, mock.Anything).Return(uploadError)

	batcher := NewBatcher[ArchivalData](config, uploader, logger)
	batcher.Start()
	defer batcher.Stop()

	// Act
	var ackCount, nackCount int
	var mu sync.Mutex
	ackFunc := func() { mu.Lock(); ackCount++; mu.Unlock() }
	nackFunc := func() { mu.Lock(); nackCount++; mu.Unlock() }

	// Send 2 messages to trigger the flush.
	for i := 0; i < 2; i++ {
		msg := types.ConsumedMessage{Ack: ackFunc, Nack: nackFunc}
		payload := &ArchivalData{}
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{OriginalMessage: msg, Payload: payload}
	}

	// Assert
	time.Sleep(100 * time.Millisecond)
	uploader.AssertExpectations(t)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 0, ackCount, "Expected 0 messages to be Acked on failure")
	assert.Equal(t, 2, nackCount, "Expected 2 messages to be Nacked on failure")
}
