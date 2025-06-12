package bqstore_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// Tests for BigQueryService
// ====================================================================================

func TestBigQueryService_ProcessesMessagesSuccessfully(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	// Setting BatchSize to 10 and a long timeout ensures the flush is only triggered by Stop().
	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: 10 * time.Second}
	batcher := bqstore.NewBatchInserter[testPayload](batcherCfg, mockInserter, logger)

	testDecoder := func(payload []byte) (*testPayload, error) {
		var p testPayload
		err := json.Unmarshal(payload, &p)
		return &p, err
	}

	service, err := bqstore.NewBigQueryService[testPayload](1, mockConsumer, batcher, testDecoder, logger)
	require.NoError(t, err)

	// Act
	err = service.Start()
	require.NoError(t, err)

	payload, err := json.Marshal(&testPayload{ID: 101, Data: "hello"})
	require.NoError(t, err)

	var acked bool
	msg := types.ConsumedMessage{
		ID:      "test-msg-1",
		Payload: payload,
		Ack:     func() { acked = true },
	}
	mockConsumer.Push(msg)

	// FIX: Add a brief sleep to prevent a race condition where Stop() is called
	// before the service's worker goroutine can process the message.
	time.Sleep(50 * time.Millisecond)

	// Stop the service, which will flush the final batch.
	service.Stop()

	// Assert
	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1, "Inserter should have been called once")
	require.Len(t, receivedBatches[0], 1, "Batch should have one item")
	assert.Equal(t, 101, receivedBatches[0][0].ID)
	assert.True(t, acked, "Message should have been acked")
}

func TestBigQueryService_HandlesDecoderError(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: time.Second}
	batcher := bqstore.NewBatchInserter[testPayload](batcherCfg, mockInserter, logger)

	errorDecoder := func(payload []byte) (*testPayload, error) {
		return nil, errors.New("bad data")
	}

	service, err := bqstore.NewBigQueryService[testPayload](1, mockConsumer, batcher, errorDecoder, logger)
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
	assert.True(t, nacked, "Message should have been nacked on decode failure")
	assert.Equal(t, 0, mockInserter.GetCallCount(), "Inserter should not be called for a failed message")
}
