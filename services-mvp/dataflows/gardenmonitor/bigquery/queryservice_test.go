package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mocks ---

// MockDecodedDataBatchInserter is a mock for the DecodedDataBatchInserter interface.
type MockDecodedDataBatchInserter struct {
	mock.Mock
}

func (m *MockDecodedDataBatchInserter) InsertBatch(ctx context.Context, readings []*GardenMonitorPayload) error {
	args := m.Called(ctx, readings)
	return args.Error(0)
}

func (m *MockDecodedDataBatchInserter) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockMessageConsumer is a mock for the MessageConsumer interface.
type MockMessageConsumer struct {
	mock.Mock
	MessagesChan chan ConsumedMessage
	DoneChan     chan struct{}
}

func (m *MockMessageConsumer) Messages() <-chan ConsumedMessage {
	return m.MessagesChan
}

func (m *MockMessageConsumer) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMessageConsumer) Stop() error {
	args := m.Called()
	if m.MessagesChan != nil {
		close(m.MessagesChan)
	}
	if m.DoneChan != nil {
		close(m.DoneChan)
	}
	return args.Error(0)
}

func (m *MockMessageConsumer) Done() <-chan struct{} {
	return m.DoneChan
}

func NewMockMessageConsumer() *MockMessageConsumer {
	return &MockMessageConsumer{
		MessagesChan: make(chan ConsumedMessage, 10),
		DoneChan:     make(chan struct{}),
	}
}

// --- Helper for creating a test service ---

func setupTestService(t *testing.T, config *ServiceConfig, mockInserter *MockDecodedDataBatchInserter) (*ProcessingService, *MockMessageConsumer) {
	t.Helper()

	mockConsumer := NewMockMessageConsumer()
	logger := zerolog.Nop()

	batchInserterConfig := &BatchInserterConfig{
		BatchSize:    config.BatchSize,
		FlushTimeout: config.FlushTimeout,
	}
	batchInserter := NewBatchInserter(batchInserterConfig, mockInserter, logger)

	service := &ProcessingService{
		config:        config,
		consumer:      mockConsumer,
		batchInserter: batchInserter,
		logger:        logger,
	}
	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())
	service.shutdownCtx = shutdownCtx
	service.shutdownFunc = shutdownFunc

	mockConsumer.On("Start", mock.Anything).Return(nil).Once()
	mockConsumer.On("Stop").Return(nil).Once()

	return service, mockConsumer
}

// --- Tests for ProcessingService ---

func TestProcessingService_ProcessMessage_Success(t *testing.T) {
	// This test verifies that messages are ACKED only after a successful batch insert.
	mockInserter := new(MockDecodedDataBatchInserter)
	config := &ServiceConfig{
		NumProcessingWorkers: 1,
		BatchSize:            2,
		FlushTimeout:         1 * time.Minute,
	}
	service, mockConsumer := setupTestService(t, config, mockInserter)

	payload1 := &GardenMonitorPayload{UID: "dev-01"}
	payload2 := &GardenMonitorPayload{UID: "dev-02"}
	message1, _ := json.Marshal(GardenMonitorMessage{Payload: payload1})
	message2, _ := json.Marshal(GardenMonitorMessage{Payload: payload2})

	var ackWg sync.WaitGroup
	ackWg.Add(2)
	ackCount := 0
	ackLock := sync.Mutex{}

	consumedMsg1 := ConsumedMessage{ID: "msg-1", Payload: message1, Ack: func() { ackLock.Lock(); ackCount++; ackLock.Unlock(); ackWg.Done() }}
	consumedMsg2 := ConsumedMessage{ID: "msg-2", Payload: message2, Ack: func() { ackLock.Lock(); ackCount++; ackLock.Unlock(); ackWg.Done() }}

	// Mock expectations for a successful insert
	mockInserter.On("InsertBatch", mock.Anything, []*GardenMonitorPayload{payload1, payload2}).Return(nil).Once()
	mockInserter.On("Close").Return(nil).Once()

	// Start service & send messages
	err := service.Start()
	require.NoError(t, err)
	mockConsumer.MessagesChan <- consumedMsg1
	mockConsumer.MessagesChan <- consumedMsg2

	// Wait for the Acks to be called
	ackWg.Wait()

	service.Stop()

	// Assertions
	assert.Equal(t, 2, ackCount, "Both messages should have been Ack'd after successful insert")
	mockInserter.AssertExpectations(t)
	mockConsumer.AssertExpectations(t)
}

func TestProcessingService_ProcessMessage_InserterError(t *testing.T) {
	// This test verifies that messages are NACKED if the batch insert fails.
	mockInserter := new(MockDecodedDataBatchInserter)
	config := &ServiceConfig{
		NumProcessingWorkers: 1,
		BatchSize:            2,
		FlushTimeout:         1 * time.Minute,
	}
	service, mockConsumer := setupTestService(t, config, mockInserter)

	payload1 := &GardenMonitorPayload{UID: "dev-fail-01"}
	payload2 := &GardenMonitorPayload{UID: "dev-fail-02"}
	message1, _ := json.Marshal(GardenMonitorMessage{Payload: payload1})
	message2, _ := json.Marshal(GardenMonitorMessage{Payload: payload2})

	var nackWg sync.WaitGroup
	nackWg.Add(2)
	nackCount := 0
	nackLock := sync.Mutex{}

	consumedMsg1 := ConsumedMessage{ID: "msg-1", Payload: message1, Nack: func() { nackLock.Lock(); nackCount++; nackLock.Unlock(); nackWg.Done() }}
	consumedMsg2 := ConsumedMessage{ID: "msg-2", Payload: message2, Nack: func() { nackLock.Lock(); nackCount++; nackLock.Unlock(); nackWg.Done() }}

	// Mock expectations for a failed insert
	mockInserter.On("InsertBatch", mock.Anything, mock.Anything).Return(errors.New("bigquery insert failed")).Once()
	mockInserter.On("Close").Return(nil).Once()

	// Start service & send messages
	err := service.Start()
	require.NoError(t, err)
	mockConsumer.MessagesChan <- consumedMsg1
	mockConsumer.MessagesChan <- consumedMsg2

	// Wait for the Nacks to be called
	nackWg.Wait()

	service.Stop()

	// Assertions
	assert.Equal(t, 2, nackCount, "Both messages should have been Nack'd after failed insert")
	mockInserter.AssertExpectations(t)
	mockConsumer.AssertExpectations(t)
}

func TestProcessingService_ProcessMessage_UnmarshalError(t *testing.T) {
	// This test verifies that invalid messages are NACKED immediately.
	mockInserter := new(MockDecodedDataBatchInserter)
	config := &ServiceConfig{NumProcessingWorkers: 1, BatchSize: 2, FlushTimeout: 1 * time.Second}
	service, mockConsumer := setupTestService(t, config, mockInserter)

	invalidPayloadBytes := []byte(`{"invalid"}`)
	var nackWg sync.WaitGroup
	nackWg.Add(1)
	nackCalled := false
	consumedMsg := ConsumedMessage{ID: "msg-invalid", Payload: invalidPayloadBytes, Ack: func() {}, Nack: func() { nackCalled = true; nackWg.Done() }}

	mockInserter.On("Close").Return(nil).Once()

	// Start service & send message
	err := service.Start()
	require.NoError(t, err)
	mockConsumer.MessagesChan <- consumedMsg
	nackWg.Wait()

	service.Stop()

	// Assertions
	assert.True(t, nackCalled, "Nack should have been called")
	mockInserter.AssertNotCalled(t, "InsertBatch", mock.Anything, mock.Anything)
	mockConsumer.AssertExpectations(t)
}
