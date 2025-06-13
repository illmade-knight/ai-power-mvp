package icestore

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

// mockFinalUploader is a mock implementation of the non-generic DataUploader interface.
type mockFinalUploader struct {
	sync.Mutex
	InsertBatchFn func(ctx context.Context, items []*ArchivalData) error
	callCount     int
	receivedItems [][]*ArchivalData
}

func (m *mockFinalUploader) UploadBatch(ctx context.Context, items []*ArchivalData) error {
	m.Lock()
	defer m.Unlock()
	m.callCount++
	m.receivedItems = append(m.receivedItems, items)
	if m.InsertBatchFn != nil {
		return m.InsertBatchFn(ctx, items)
	}
	return nil
}
func (m *mockFinalUploader) Close() error { return nil }
func (m *mockFinalUploader) GetCallCount() int {
	m.Lock()
	defer m.Unlock()
	return m.callCount
}
func (m *mockFinalUploader) GetReceivedItems() [][]*ArchivalData {
	m.Lock()
	defer m.Unlock()
	return m.receivedItems
}

// --- Batcher Test Cases (Non-Generic) ---

func TestBatcher_BatchSizeTrigger(t *testing.T) {
	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	config := &BatcherConfig{
		BatchSize:    3,
		FlushTimeout: 1 * time.Second,
	}

	batcher := NewBatcher(config, mockUploader, logger)
	batcher.Start()
	defer batcher.Stop()

	for i := 0; i < 3; i++ {
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{
			Payload: &ArchivalData{},
		}
	}

	time.Sleep(100 * time.Millisecond) // Allow time for flush

	assert.Equal(t, 1, mockUploader.GetCallCount(), "UploadBatch should be called once")
	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 3, "The batch should contain 3 items")
}

func TestBatcher_FlushTimeoutTrigger(t *testing.T) {
	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	config := &BatcherConfig{
		BatchSize:    10,
		FlushTimeout: 100 * time.Millisecond,
	}

	batcher := NewBatcher(config, mockUploader, logger)
	batcher.Start()
	defer batcher.Stop()

	for i := 0; i < 2; i++ {
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{
			Payload: &ArchivalData{},
		}
	}

	time.Sleep(150 * time.Millisecond) // Wait for timeout

	assert.Equal(t, 1, mockUploader.GetCallCount(), "UploadBatch should be called once due to timeout")
	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 2, "The batch should contain 2 items")
}

func TestBatcher_StopFlushesFinalBatch(t *testing.T) {
	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	config := &BatcherConfig{
		BatchSize:    10,
		FlushTimeout: 5 * time.Second,
	}

	batcher := NewBatcher(config, mockUploader, logger)
	batcher.Start()

	for i := 0; i < 4; i++ {
		batcher.Input() <- &types.BatchedMessage[ArchivalData]{
			Payload: &ArchivalData{},
		}
	}
	time.Sleep(50 * time.Millisecond)

	batcher.Stop() // Should trigger final flush

	assert.Equal(t, 1, mockUploader.GetCallCount(), "UploadBatch should be called on stop")
	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 4, "The final batch should contain 4 items")
}

func TestBatcher_AckNackLogic(t *testing.T) {
	createMessage := func() (*types.BatchedMessage[ArchivalData], *sync.WaitGroup, *bool, *bool) {
		var wg sync.WaitGroup
		wg.Add(1)
		ackCalled, nackCalled := false, false
		msg := &types.BatchedMessage[ArchivalData]{
			Payload: &ArchivalData{},
			OriginalMessage: types.ConsumedMessage{
				Ack: func() {
					ackCalled = true
					wg.Done()
				},
				Nack: func() {
					nackCalled = true
					wg.Done()
				},
			},
		}
		return msg, &wg, &ackCalled, &nackCalled
	}

	t.Run("acks messages on successful upload", func(t *testing.T) {
		logger := zerolog.Nop()
		mockUploader := &mockFinalUploader{
			InsertBatchFn: func(ctx context.Context, items []*ArchivalData) error {
				return nil // Success
			},
		}
		config := &BatcherConfig{BatchSize: 1, FlushTimeout: time.Second}
		batcher := NewBatcher(config, mockUploader, logger)
		batcher.Start()
		defer batcher.Stop()

		msg, wg, ack, nack := createMessage()
		batcher.Input() <- msg

		wg.Wait() // Wait for Ack/Nack to be called

		assert.True(t, *ack, "Should have acked the message")
		assert.False(t, *nack, "Should not have nacked the message")
	})

	t.Run("nacks messages on failed upload", func(t *testing.T) {
		logger := zerolog.Nop()
		mockUploader := &mockFinalUploader{
			InsertBatchFn: func(ctx context.Context, items []*ArchivalData) error {
				return errors.New("gcs upload failed") // Failure
			},
		}
		config := &BatcherConfig{BatchSize: 1, FlushTimeout: time.Second}
		batcher := NewBatcher(config, mockUploader, logger)
		batcher.Start()
		defer batcher.Stop()

		msg, wg, ack, nack := createMessage()
		batcher.Input() <- msg

		wg.Wait() // Wait for Ack/Nack to be called

		assert.False(t, *ack, "Should not have acked the message")
		assert.True(t, *nack, "Should have nacked the message")
	})
}
