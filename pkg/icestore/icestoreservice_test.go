package icestore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Cases ---

// TestIceStorageService_Success verifies the happy path, including ID propagation.
func TestIceStorageService_Success(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	mockConsumer := NewMockMessageConsumer(10)

	batcher := NewBatcher(&BatcherConfig{BatchSize: 1}, mockUploader, logger)

	service, err := NewIceStorageService(1, mockConsumer, batcher, ArchivalTransformer, logger)
	require.NoError(t, err)

	// Act
	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	var acked bool
	ackChan := make(chan struct{})
	mockUploader.InsertBatchFn = func(ctx context.Context, items []*ArchivalData) error {
		// This simulates the batcher calling Ack() on the original message.
		acked = true
		close(ackChan)
		return nil
	}

	msg := types.ConsumedMessage{
		ID:      "test-id-123", // Give the message a specific ID.
		Payload: []byte(`{"data":"test"}`),
		Ack:     func() {},
	}
	mockConsumer.Push(msg)

	select {
	case <-ackChan:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message to be processed")
	}

	// Assert
	assert.True(t, acked, "Message should have been acked")
	receivedBatches := mockUploader.GetReceivedItems()
	require.Len(t, receivedBatches, 1)
	require.Len(t, receivedBatches[0], 1)
	assert.Equal(t, "test-id-123", receivedBatches[0][0].ID, "ArchivalData.ID should match the original message ID")
}

// TestIceStorageService_TransformerError verifies a Nack on transformation failure.
func TestIceStorageService_TransformerError(t *testing.T) {
	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	mockConsumer := NewMockMessageConsumer(10)
	batcher := NewBatcher(&BatcherConfig{BatchSize: 1}, mockUploader, logger)

	// CORRECTED: Use a dedicated mock transformer that is guaranteed to return an error.
	errorTransformer := func(msg types.ConsumedMessage) (*ArchivalData, bool, error) {
		return nil, false, errors.New("transformation failed")
	}

	service, err := NewIceStorageService(1, mockConsumer, batcher, errorTransformer, logger)
	require.NoError(t, err)
	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	var nacked bool
	nackChan := make(chan struct{})
	msg := types.ConsumedMessage{
		ID:      "ice-msg-err",
		Payload: []byte(`any-payload`),
		Nack: func() {
			nacked = true
			close(nackChan)
		},
	}
	mockConsumer.Push(msg)

	select {
	case <-nackChan:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message to be nacked")
	}

	assert.True(t, nacked, "Message should have been nacked")
	assert.Equal(t, 0, mockUploader.GetCallCount(), "Uploader should not be called")
}

// TestIceStorageService_Skip verifies that a null payload is Acked and skipped.
func TestIceStorageService_Skip(t *testing.T) {
	logger := zerolog.Nop()
	mockUploader := &mockFinalUploader{}
	mockConsumer := NewMockMessageConsumer(10)
	batcher := NewBatcher(&BatcherConfig{BatchSize: 1}, mockUploader, logger)

	service, err := NewIceStorageService(1, mockConsumer, batcher, ArchivalTransformer, logger)
	require.NoError(t, err)
	err = service.Start()
	require.NoError(t, err)
	defer service.Stop()

	var acked bool
	ackChan := make(chan struct{})
	msg := types.ConsumedMessage{
		ID:      "ice-msg-skip",
		Payload: []byte("null"), // "null" payload should trigger a skip.
		Ack: func() {
			acked = true
			close(ackChan)
		},
	}
	mockConsumer.Push(msg)

	select {
	case <-ackChan:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message to be acked")
	}

	assert.True(t, acked, "Message should have been acked")
	assert.Equal(t, 0, mockUploader.GetCallCount(), "Uploader should not be called")
}
