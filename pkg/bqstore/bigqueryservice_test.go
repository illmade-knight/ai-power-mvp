package bqstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	//"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// Tests for BigQueryService (Refactored)
// ====================================================================================

// TestBigQueryService_ProcessesMessagesSuccessfully verifies the "happy path" where a message
// is consumed, transformed, and its payload is passed to the batch inserter.
func TestBigQueryService_ProcessesMessagesSuccessfully(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: 10 * time.Second}
	batcher := bqstore.NewBatcher[testPayload](batcherCfg, mockInserter, logger)

	// REFACTORED: Define a MessageTransformer instead of a PayloadDecoder.
	testTransformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		var p testPayload
		err := json.Unmarshal(msg.Payload, &p)
		return &p, false, err
	}

	service, err := bqstore.NewBigQueryService[testPayload](1, mockConsumer, batcher, testTransformer, logger)
	require.NoError(t, err)

	// Act
	err = service.Start()
	require.NoError(t, err)

	payload, err := json.Marshal(&testPayload{ID: 101, Data: "hello"})
	require.NoError(t, err)

	var acked bool
	// The service should not call Ack directly, but the batcher will. We check that it's
	// eventually called by the mock inserter's successful flush.
	mockInserter.InsertBatchFn = func(ctx context.Context, items []*testPayload) error {
		acked = true
		return nil
	}

	msg := types.ConsumedMessage{
		ID:      "test-msg-1",
		Payload: payload,
		Ack:     func() {}, // The batcher will call the OriginalMessage.Ack
		Nack:    func() {},
	}
	mockConsumer.Push(msg)

	time.Sleep(50 * time.Millisecond)
	service.Stop() // Stop flushes the final batch.

	// Assert
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1, "Inserter should have been called once")
	require.Len(t, receivedBatches[0], 1, "Batch should have one item")
	assert.Equal(t, 101, receivedBatches[0][0].ID)
	assert.True(t, acked, "Message should have been acked after a successful batch insert")
}

// TestBigQueryService_HandlesTransformerError verifies that a message is Nacked if the
// transformer returns an error.
func TestBigQueryService_HandlesTransformerError(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: time.Second}
	batcher := bqstore.NewBatcher[testPayload](batcherCfg, mockInserter, logger)

	// REFACTORED: The transformer now returns an error.
	errorTransformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, false, errors.New("bad data")
	}

	service, err := bqstore.NewBigQueryService[testPayload](1, mockConsumer, batcher, errorTransformer, logger)
	require.NoError(t, err)

	// Act
	err = service.Start()
	require.NoError(t, err)

	var nacked bool
	msg := types.ConsumedMessage{
		ID:      "test-msg-2",
		Payload: []byte("this is not valid json"),
		Nack:    func() { nacked = true },
	}
	mockConsumer.Push(msg)

	time.Sleep(50 * time.Millisecond)
	service.Stop()

	// Assert
	assert.True(t, nacked, "Message should have been nacked on transform failure")
	assert.Equal(t, 0, mockInserter.GetCallCount(), "Inserter should not be called for a failed message")
}

// TestBigQueryService_SkipsMessage verifies that a message is Acked if the transformer
// signals to skip it.
func TestBigQueryService_SkipsMessage(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: time.Second}
	batcher := bqstore.NewBatcher[testPayload](batcherCfg, mockInserter, logger)

	// This transformer signals to skip the message.
	skipTransformer := func(msg types.ConsumedMessage) (*testPayload, bool, error) {
		return nil, true, nil
	}

	service, err := bqstore.NewBigQueryService[testPayload](1, mockConsumer, batcher, skipTransformer, logger)
	require.NoError(t, err)

	// Act
	err = service.Start()
	require.NoError(t, err)

	var acked bool
	msg := types.ConsumedMessage{
		ID:      "test-msg-3",
		Payload: []byte("some data to be skipped"),
		Ack:     func() { acked = true },
	}
	mockConsumer.Push(msg)

	time.Sleep(50 * time.Millisecond)
	service.Stop()

	// Assert
	assert.True(t, acked, "Message should have been acked on skip")
	assert.Equal(t, 0, mockInserter.GetCallCount(), "Inserter should not be called for a skipped message")
}
