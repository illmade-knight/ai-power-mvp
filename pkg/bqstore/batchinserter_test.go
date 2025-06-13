package bqstore_test

import (
	"context"
	"errors"
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- batch_inserter_test.go ---

func TestBatchInserter_BatchSizeTrigger(t *testing.T) {
	logger := zerolog.Nop()
	mockInserter := &MockDataBatchInserter[testPayload]{}

	config := &bqstore.BatchInserterConfig{
		BatchSize:    3,
		FlushTimeout: 1 * time.Second,
	}

	batcher := bqstore.NewBatcher[testPayload](config, mockInserter, logger)
	batcher.Start()
	defer batcher.Stop()

	// Send 3 messages, which should trigger an immediate flush.
	for i := 0; i < 3; i++ {
		batcher.Input() <- &types.BatchedMessage[testPayload]{
			Payload: &testPayload{ID: i},
		}
	}

	// Wait for the flush to happen.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, mockInserter.GetCallCount(), "InsertBatch should be called once")
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1, "Should have received one batch")
	assert.Len(t, receivedBatches[0], 3, "The batch should contain 3 items")
}

func TestBatchInserter_FlushTimeoutTrigger(t *testing.T) {
	logger := zerolog.Nop()
	mockInserter := &MockDataBatchInserter[testPayload]{}

	config := &bqstore.BatchInserterConfig{
		BatchSize:    10,
		FlushTimeout: 100 * time.Millisecond,
	}

	batcher := bqstore.NewBatcher[testPayload](config, mockInserter, logger)
	batcher.Start()
	defer batcher.Stop()

	// Send 2 messages, fewer than the batch size.
	for i := 0; i < 2; i++ {
		batcher.Input() <- &types.BatchedMessage[testPayload]{
			Payload: &testPayload{ID: i},
		}
	}

	// Wait for longer than the flush timeout.
	time.Sleep(150 * time.Millisecond)

	assert.Equal(t, 1, mockInserter.GetCallCount(), "InsertBatch should be called once due to timeout")
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 2, "The batch should contain 2 items")
}

func TestBatchInserter_StopFlushesFinalBatch(t *testing.T) {
	logger := zerolog.Nop()
	mockInserter := &MockDataBatchInserter[testPayload]{}

	config := &bqstore.BatchInserterConfig{
		BatchSize:    10,
		FlushTimeout: 5 * time.Second, // Long timeout to ensure it doesn't trigger
	}

	batcher := bqstore.NewBatcher[testPayload](config, mockInserter, logger)
	batcher.Start()

	// Send a partial batch.
	for i := 0; i < 4; i++ {
		batcher.Input() <- &types.BatchedMessage[testPayload]{
			Payload: &testPayload{ID: i},
		}
	}
	time.Sleep(50 * time.Millisecond) // Give time for messages to be read from channel

	// Stop the batcher, which should trigger the final flush.
	batcher.Stop()

	assert.Equal(t, 1, mockInserter.GetCallCount(), "InsertBatch should be called on stop")
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	assert.Len(t, receivedBatches[0], 4, "The final batch should contain 4 items")
}

func TestBatchInserter_AckNackLogic(t *testing.T) {
	t.Run("acks messages on successful insert", func(t *testing.T) {
		logger := zerolog.Nop()
		mockInserter := &MockDataBatchInserter[testPayload]{
			InsertBatchFn: func(ctx context.Context, items []*testPayload) error {
				return nil // Success
			},
		}
		config := &bqstore.BatchInserterConfig{BatchSize: 2, FlushTimeout: time.Second}
		batcher := bqstore.NewBatcher[testPayload](config, mockInserter, logger)
		batcher.Start()
		defer batcher.Stop()

		var ackCount, nackCount int
		var mu sync.Mutex
		createMessage := func(id int) *types.BatchedMessage[testPayload] {
			return &types.BatchedMessage[testPayload]{
				Payload: &testPayload{ID: id},
				OriginalMessage: types.ConsumedMessage{
					Ack:  func() { mu.Lock(); ackCount++; mu.Unlock() },
					Nack: func() { mu.Lock(); nackCount++; mu.Unlock() },
				},
			}
		}

		batcher.Input() <- createMessage(1)
		batcher.Input() <- createMessage(2)

		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 2, ackCount, "Should have acked 2 messages")
		assert.Equal(t, 0, nackCount, "Should have nacked 0 messages")
	})

	t.Run("nacks messages on failed insert", func(t *testing.T) {
		logger := zerolog.Nop()
		mockInserter := &MockDataBatchInserter[testPayload]{
			InsertBatchFn: func(ctx context.Context, items []*testPayload) error {
				return errors.New("bigquery insert failed") // Failure
			},
		}
		config := &bqstore.BatchInserterConfig{BatchSize: 2, FlushTimeout: time.Second}
		batcher := bqstore.NewBatcher[testPayload](config, mockInserter, logger)
		batcher.Start()
		defer batcher.Stop()

		var ackCount, nackCount int
		var mu sync.Mutex
		createMessage := func(id int) *types.BatchedMessage[testPayload] {
			return &types.BatchedMessage[testPayload]{
				Payload: &testPayload{ID: id},
				OriginalMessage: types.ConsumedMessage{
					Ack:  func() { mu.Lock(); ackCount++; mu.Unlock() },
					Nack: func() { mu.Lock(); nackCount++; mu.Unlock() },
				},
			}
		}

		batcher.Input() <- createMessage(1)
		batcher.Input() <- createMessage(2)

		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 0, ackCount, "Should have acked 0 messages")
		assert.Equal(t, 2, nackCount, "Should have nacked 2 messages")
	})
}
