package icestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock MessageConsumer ---
type MockMessageConsumer struct {
	MessagesChan chan ConsumedMessage
	StopFunc     func() error
	StartFunc    func(ctx context.Context) error
	DoneChan     chan struct{}
	startCalled  bool
	stopCalled   bool
}

func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		MessagesChan: make(chan ConsumedMessage, bufferSize),
		DoneChan:     make(chan struct{}),
	}
}
func (m *MockMessageConsumer) Messages() <-chan ConsumedMessage { return m.MessagesChan }
func (m *MockMessageConsumer) Start(ctx context.Context) error {
	m.startCalled = true
	if m.StartFunc != nil {
		return m.StartFunc(ctx)
	}
	return nil
}
func (m *MockMessageConsumer) Stop() error {
	m.stopCalled = true
	if m.StopFunc != nil {
		return m.StopFunc()
	}
	// Close DoneChan to signal completion if not already closed by test
	select {
	case <-m.DoneChan: // Already closed
	default:
		close(m.DoneChan)
	}
	return nil
}
func (m *MockMessageConsumer) Done() <-chan struct{} { return m.DoneChan }

// --- Mock RawDataArchiver ---
type MockRawDataArchiver struct {
	ArchiveFunc  func(ctx context.Context, payload []byte, ts time.Time) error
	StopFunc     func() error
	archiveCalls []struct {
		Payload []byte
		Ts      time.Time
	}
	stopCalled bool
	mu         sync.Mutex
}

func NewMockRawDataArchiver() *MockRawDataArchiver {
	return &MockRawDataArchiver{
		archiveCalls: make([]struct {
			Payload []byte
			Ts      time.Time
		}, 0),
	}
}
func (m *MockRawDataArchiver) Archive(ctx context.Context, payload []byte, ts time.Time) error {
	m.mu.Lock()
	m.archiveCalls = append(m.archiveCalls, struct {
		Payload []byte
		Ts      time.Time
	}{Payload: payload, Ts: ts})
	m.mu.Unlock()
	if m.ArchiveFunc != nil {
		return m.ArchiveFunc(ctx, payload, ts)
	}
	return nil
}
func (m *MockRawDataArchiver) Stop() error {
	m.stopCalled = true
	if m.StopFunc != nil {
		return m.StopFunc()
	}
	return nil
}
func (m *MockRawDataArchiver) GetArchiveCalls() []struct {
	Payload []byte
	Ts      time.Time
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	callsCopy := make([]struct {
		Payload []byte
		Ts      time.Time
	}, len(m.archiveCalls))
	copy(callsCopy, m.archiveCalls)
	return callsCopy
}

func TestArchivalService_StartStop(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background() // Context for NewArchivalService

	mockEnrichedConsumer := NewMockMessageConsumer(10)
	mockUnidentifiedConsumer := NewMockMessageConsumer(10)
	mockArchiver := NewMockRawDataArchiver()

	cfg, err := LoadArchivalServiceConfigFromEnv()
	require.NoError(t, err)
	cfg.NumArchivalWorkers = 1

	// Temporarily set env vars for consumer config loading if NewArchivalService uses them directly
	// This is a bit of a workaround as NewArchivalService creates its own consumers.
	// A better approach might be to allow injecting mock consumers directly into NewArchivalService.
	// For now, we'll override them after creation.
	originalEnrichedSub := os.Getenv(cfg.EnrichedMessagesSubscriptionEnvVar)
	originalUnidentifiedSub := os.Getenv(cfg.UnidentifiedMessagesSubscriptionEnvVar)
	os.Setenv(cfg.EnrichedMessagesSubscriptionEnvVar, "mock-enriched-sub")
	os.Setenv(cfg.UnidentifiedMessagesSubscriptionEnvVar, "mock-unidentified-sub")
	os.Setenv("GCP_PROJECT_ID", "test-project-for-archival-service-startstop") // Ensure it's set
	defer func() {
		os.Setenv(cfg.EnrichedMessagesSubscriptionEnvVar, originalEnrichedSub)
		os.Setenv(cfg.UnidentifiedMessagesSubscriptionEnvVar, originalUnidentifiedSub)
		os.Unsetenv("GCP_PROJECT_ID")
	}()

	service, err := NewArchivalService(ctx, cfg, mockArchiver, logger)
	require.NoError(t, err)
	// Override consumers with mocks AFTER NewArchivalService
	service.enrichedConsumer = mockEnrichedConsumer
	service.unidentifiedConsumer = mockUnidentifiedConsumer

	err = service.Start()
	require.NoError(t, err)
	assert.True(t, mockEnrichedConsumer.startCalled, "EnrichedConsumer.Start should be called")
	assert.True(t, mockUnidentifiedConsumer.startCalled, "UnidentifiedConsumer.Start should be called")

	// Simulate consumers stopping and closing their channels
	close(mockEnrichedConsumer.MessagesChan)
	// To properly test worker shutdown, ensure DoneChan is closed by the mock when Stop is called
	// or when MessagesChan is closed and it simulates its internal goroutine exiting.
	// For this test, Stop() will close DoneChan.

	service.Stop() // This calls Stop on mocks, which should close their DoneChans

	assert.True(t, mockEnrichedConsumer.stopCalled, "EnrichedConsumer.Stop should be called")
	assert.True(t, mockUnidentifiedConsumer.stopCalled, "UnidentifiedConsumer.Stop should be called")
	assert.True(t, mockArchiver.stopCalled, "Archiver.Stop should be called")
}

func TestArchivalService_ProcessMessagesFromConsumers(t *testing.T) {
	logger := zerolog.Nop()
	// No overall context needed for this test as service manages its own shutdown context

	mockEnrichedConsumer := NewMockMessageConsumer(10)
	mockUnidentifiedConsumer := NewMockMessageConsumer(10)
	mockArchiver := NewMockRawDataArchiver()

	var ackedMessages sync.Map
	var nackedMessages sync.Map

	cfg, err := LoadArchivalServiceConfigFromEnv()
	require.NoError(t, err)
	cfg.NumArchivalWorkers = 2

	// Set necessary env vars for NewArchivalService's internal consumer creation,
	// even though we override them immediately after.
	originalEnrichedSub := os.Getenv(cfg.EnrichedMessagesSubscriptionEnvVar)
	originalUnidentifiedSub := os.Getenv(cfg.UnidentifiedMessagesSubscriptionEnvVar)
	os.Setenv(cfg.EnrichedMessagesSubscriptionEnvVar, "mock-enriched-sub-process")
	os.Setenv(cfg.UnidentifiedMessagesSubscriptionEnvVar, "mock-unidentified-sub-process")
	os.Setenv("GCP_PROJECT_ID", "test-project-for-archival-service-process")
	defer func() {
		os.Setenv(cfg.EnrichedMessagesSubscriptionEnvVar, originalEnrichedSub)
		os.Setenv(cfg.UnidentifiedMessagesSubscriptionEnvVar, originalUnidentifiedSub)
		os.Unsetenv("GCP_PROJECT_ID")
	}()

	service, err := NewArchivalService(context.Background(), cfg, mockArchiver, logger)
	require.NoError(t, err)
	service.enrichedConsumer = mockEnrichedConsumer         // Override with mock
	service.unidentifiedConsumer = mockUnidentifiedConsumer // Override with mock

	err = service.Start()
	require.NoError(t, err)

	enrichedPayload1 := []byte(`{"device_eui":"enriched001","location_id":"loc1"}`)
	enrichedTime1 := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)
	enrichedMsg1 := ConsumedMessage{
		ID: "enriched_msg_1", Payload: enrichedPayload1, PublishTime: enrichedTime1,
		Ack:  func() { ackedMessages.Store("enriched_msg_1", true) },  // Renamed
		Nack: func() { nackedMessages.Store("enriched_msg_1", true) }, // Renamed
	}

	unidentifiedPayload1 := []byte(`{"device_eui":"unidentified001","processing_error":"metadata_missing"}`)
	unidentifiedTime1 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	unidentifiedMsg1 := ConsumedMessage{
		ID: "unidentified_msg_1", Payload: unidentifiedPayload1, PublishTime: unidentifiedTime1,
		Ack:  func() { ackedMessages.Store("unidentified_msg_1", true) },  // Renamed
		Nack: func() { nackedMessages.Store("unidentified_msg_1", true) }, // Renamed
	}

	errorPayload := []byte(`{"device_eui":"errorDevice001","location_id":"locError"}`)
	errorTime := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	errorMsg := ConsumedMessage{
		ID: "error_msg_1", Payload: errorPayload, PublishTime: errorTime,
		Ack:  func() { ackedMessages.Store("error_msg_1", true) },  // Renamed
		Nack: func() { nackedMessages.Store("error_msg_1", true) }, // Renamed
	}

	mockArchiver.ArchiveFunc = func(ctx context.Context, payload []byte, ts time.Time) error {
		// Simulate archiver error for the specific errorPayload
		if bytes.Equal(payload, errorPayload) {
			return errors.New("simulated archive error")
		}
		// For other messages, the default mock behavior (just appending to archiveCalls) is fine.
		return nil
	}

	// Send messages
	mockEnrichedConsumer.MessagesChan <- enrichedMsg1
	mockUnidentifiedConsumer.MessagesChan <- unidentifiedMsg1
	mockEnrichedConsumer.MessagesChan <- errorMsg

	// Wait for messages to be processed.
	// Since Stop() waits for workers, we can call it and then assert.
	// Close mock consumer channels to signal workers that no more messages are coming from these mocks
	// (simulates consumers stopping their message flow).
	close(mockEnrichedConsumer.MessagesChan)
	close(mockUnidentifiedConsumer.MessagesChan)

	service.Stop() // This will wait for workers via wg.Wait()

	// Assertions
	archivedCalls := mockArchiver.GetArchiveCalls()
	// All 3 messages should attempt to be archived.
	assert.Len(t, archivedCalls, 3, "Expected 3 calls to archiver.Archive")

	var foundEnriched, foundUnidentified, foundErrorMsg bool
	for _, call := range archivedCalls {
		if bytes.Equal(call.Payload, enrichedPayload1) && call.Ts.Equal(enrichedTime1) {
			foundEnriched = true
		}
		if bytes.Equal(call.Payload, unidentifiedPayload1) && call.Ts.Equal(unidentifiedTime1) {
			foundUnidentified = true
		}
		if bytes.Equal(call.Payload, errorPayload) && call.Ts.Equal(errorTime) {
			foundErrorMsg = true
		}
	}
	assert.True(t, foundEnriched, "Enriched message 1 not found in archive calls")
	assert.True(t, foundUnidentified, "Unidentified message 1 not found in archive calls")
	assert.True(t, foundErrorMsg, "Error message (that caused archive error) not found in archive calls")

	_, enrichedAcked := ackedMessages.Load("enriched_msg_1")
	assert.True(t, enrichedAcked, "Enriched message 1 should be Acked")

	_, unidentifiedAcked := ackedMessages.Load("unidentified_msg_1")
	assert.True(t, unidentifiedAcked, "Unidentified message 1 should be Acked")

	_, errorMsgNacked := nackedMessages.Load("error_msg_1")
	assert.True(t, errorMsgNacked, "Error message should be Nacked")
	_, errorMsgAcked := ackedMessages.Load("error_msg_1")
	assert.False(t, errorMsgAcked, "Error message should NOT be Acked")
}
