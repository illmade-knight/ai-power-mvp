package bqstore_test

import (
	"encoding/json"
	"errors"
	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- processing_service_test.go ---
// Note: The mock implementations for MockDataBatchInserter and MockMessageConsumer
// have been removed from this file and should be placed in a shared
// test helper file (e.g., test_helpers_test.go) within this package.

func TestProcessingService_ProcessesMessages(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: time.Second}
	batcher := bqstore.NewBatchInserter[testPayload](batcherCfg, mockInserter, logger)

	serviceCfg := &bqstore.ServiceConfig{NumProcessingWorkers: 1}
	testDecoder := func(payload []byte) (*testPayload, error) {
		var p testPayload
		err := json.Unmarshal(payload, &p)
		return &p, err
	}

	service, err := bqstore.NewProcessingService[testPayload](serviceCfg, mockConsumer, batcher, testDecoder, logger)
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)

	payload, err := json.Marshal(&testPayload{ID: 101, Data: "hello"})
	require.NoError(t, err)

	var acked bool
	msg := types.ConsumedMessage{
		Payload: payload,
		Ack:     func() { acked = true },
	}

	mockConsumer.Push(msg)

	// Stop the service which will cause the batcher to flush
	service.Stop()

	receivedBatches := mockInserter.GetReceivedItems()
	require.Len(t, receivedBatches, 1, "Inserter should have been called once")
	require.Len(t, receivedBatches[0], 1, "Batch should have one item")
	assert.Equal(t, 101, receivedBatches[0][0].ID)
	assert.Equal(t, "hello", receivedBatches[0][0].Data)
	assert.True(t, acked, "Message should have been acked")
}

func TestProcessingService_HandlesDecoderError(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := NewMockMessageConsumer(10)
	mockInserter := &MockDataBatchInserter[testPayload]{}

	batcherCfg := &bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: time.Second}
	batcher := bqstore.NewBatchInserter[testPayload](batcherCfg, mockInserter, logger)

	serviceCfg := &bqstore.ServiceConfig{NumProcessingWorkers: 1}
	// This decoder will always fail
	errorDecoder := func(payload []byte) (*testPayload, error) {
		return nil, errors.New("bad data")
	}

	service, err := bqstore.NewProcessingService[testPayload](serviceCfg, mockConsumer, batcher, errorDecoder, logger)
	require.NoError(t, err)

	err = service.Start()
	require.NoError(t, err)

	var nacked bool
	msg := types.ConsumedMessage{
		Payload: []byte("this is not valid json"),
		Nack:    func() { nacked = true },
	}

	mockConsumer.Push(msg)
	time.Sleep(100 * time.Millisecond) // Allow time for processing

	assert.True(t, nacked, "Message should have been nacked on decode failure")
	assert.Equal(t, 0, mockInserter.GetCallCount(), "Inserter should not be called for a failed message")

	service.Stop()
}
