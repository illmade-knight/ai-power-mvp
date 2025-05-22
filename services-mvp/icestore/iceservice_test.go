package icestore

import (
	"context"
	"errors"
	// "os" // Removed as t.Setenv is used
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock" // Added testify/mock import
	"github.com/stretchr/testify/require"
)

// --- Mock MessageConsumer ---
type MockMessageConsumer struct {
	mock.Mock    // Embed testify/mock
	MessagesChan chan ConsumedMessage
	DoneChan     chan struct{}
	startCalled  bool // These can be removed if using testify/mock for all assertions
	stopCalled   bool // These can be removed if using testify/mock for all assertions
}

func NewMockMessageConsumer(bufferSize int) *MockMessageConsumer {
	return &MockMessageConsumer{
		MessagesChan: make(chan ConsumedMessage, bufferSize),
		DoneChan:     make(chan struct{}),
	}
}

// Messages now uses testify/mock for expectation if needed, but primarily returns the channel.
// The mock setup in the test will use .Return(m.MessagesChan).
func (m *MockMessageConsumer) Messages() <-chan ConsumedMessage {
	args := m.Called()
	if retChan, ok := args.Get(0).(<-chan ConsumedMessage); ok {
		return retChan
	}
	// Fallback or panic if not configured, but tests should configure it.
	// For simplicity, we can let tests directly use m.MessagesChan if Messages() is not explicitly mocked.
	// However, to be strict with testify/mock, Messages() should be an expected call.
	return m.MessagesChan // This is fine if tests directly send to this and service uses the interface method
}

func (m *MockMessageConsumer) Start(ctx context.Context) error {
	args := m.Called(ctx)
	m.startCalled = true // Keep for direct assertion if preferred over mock.AssertCalled
	return args.Error(0)
}

func (m *MockMessageConsumer) Stop() error {
	args := m.Called()
	m.stopCalled = true // Keep for direct assertion
	select {
	case <-m.DoneChan:
	default:
		close(m.DoneChan)
	}
	return args.Error(0)
}
func (m *MockMessageConsumer) Done() <-chan struct{} {
	args := m.Called()
	if retChan, ok := args.Get(0).(<-chan struct{}); ok {
		return retChan
	}
	return m.DoneChan
}

var _ MessageConsumer = &MockMessageConsumer{} // Ensure it satisfies the interface

// --- Mock RawDataArchiver ---
type MockRawDataArchiver struct {
	mock.Mock    // Embed testify/mock
	archiveCalls []struct {
		Payload []byte
		Ts      time.Time
	}
	// stopCalled bool // Can be tracked by testify/mock
	mu sync.Mutex
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
	// For tests that don't set specific expectations on Archive but want to track calls
	m.mu.Lock()
	m.archiveCalls = append(m.archiveCalls, struct {
		Payload []byte
		Ts      time.Time
	}{Payload: payload, Ts: ts})
	m.mu.Unlock()

	args := m.Called(ctx, payload, ts) // For testify/mock expectations
	return args.Error(0)
}
func (m *MockRawDataArchiver) Stop() error {
	args := m.Called()
	return args.Error(0)
}

// Corrected return type for GetArchiveCalls
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

var _ RawDataArchiver = &MockRawDataArchiver{} // Ensure it satisfies the interface

func TestArchivalService_StartStop(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()

	mockEnrichedConsumer := NewMockMessageConsumer(10)
	mockUnidentifiedConsumer := NewMockMessageConsumer(10)
	mockArchiver := NewMockRawDataArchiver() // Use testify/mock version

	// Setup mock expectations for consumers
	mockEnrichedConsumer.On("Start", mock.Anything).Return(nil).Once()
	mockEnrichedConsumer.On("Messages").Return((<-chan ConsumedMessage)(mockEnrichedConsumer.MessagesChan)) // Cast to satisfy interface return type
	mockEnrichedConsumer.On("Stop").Return(nil).Once()
	mockEnrichedConsumer.On("Done").Return(mockEnrichedConsumer.DoneChan).Once()

	mockUnidentifiedConsumer.On("Start", mock.Anything).Return(nil).Once()
	mockUnidentifiedConsumer.On("Messages").Return((<-chan ConsumedMessage)(mockUnidentifiedConsumer.MessagesChan))
	mockUnidentifiedConsumer.On("Stop").Return(nil).Once()
	mockUnidentifiedConsumer.On("Done").Return(mockUnidentifiedConsumer.DoneChan).Once()

	mockArchiver.On("Stop").Return(nil).Once()

	t.Setenv("ARCHIVAL_ENRICHED_SUB_ENV_VAR", "TEST_ENRICHED_SUB_STARTSTOP")
	t.Setenv("ARCHIVAL_UNIDENTIFIED_SUB_ENV_VAR", "TEST_UNIDENTIFIED_SUB_STARTSTOP")
	t.Setenv("TEST_ENRICHED_SUB_STARTSTOP", "mock-enriched-sub-ss-val")
	t.Setenv("TEST_UNIDENTIFIED_SUB_STARTSTOP", "mock-unidentified-sub-ss-val")
	t.Setenv("GCP_PROJECT_ID", "test-project-for-archival-service-startstop")

	cfg, err := LoadArchivalServiceConfigFromEnv()
	require.NoError(t, err)
	cfg.NumArchivalWorkers = 1

	service, err := NewArchivalService(ctx, cfg, mockArchiver, logger)
	require.NoError(t, err)
	service.enrichedConsumer = mockEnrichedConsumer
	service.unidentifiedConsumer = mockUnidentifiedConsumer

	err = service.Start()
	require.NoError(t, err)

	// Simulate consumers stopping and closing their channels
	close(mockEnrichedConsumer.MessagesChan)
	close(mockUnidentifiedConsumer.MessagesChan) // Ensure both are closed if StartStop tests worker exit on closed channels

	service.Stop()

	mockEnrichedConsumer.AssertExpectations(t)
	mockUnidentifiedConsumer.AssertExpectations(t)
	mockArchiver.AssertExpectations(t)
}
func TestArchivalService_ProcessMessagesFromConsumers(t *testing.T) {
	logger := zerolog.Nop() // Or use a real logger for debugging: zerolog.New(os.Stderr).With().Timestamp().Logger()

	mockEnrichedConsumer := NewMockMessageConsumer(10)
	mockUnidentifiedConsumer := NewMockMessageConsumer(10)
	mockArchiver := NewMockRawDataArchiver()

	var ackedMessages sync.Map
	var nackedMessages sync.Map

	t.Setenv("ARCHIVAL_ENRICHED_SUB_ENV_VAR", "TEST_ENRICHED_SUB_PROCESS")
	t.Setenv("ARCHIVAL_UNIDENTIFIED_SUB_ENV_VAR", "TEST_UNIDENTIFIED_SUB_PROCESS")
	t.Setenv("TEST_ENRICHED_SUB_PROCESS", "mock-enriched-sub-process-val")
	t.Setenv("TEST_UNIDENTIFIED_SUB_PROCESS", "mock-unidentified-sub-process-val")
	t.Setenv("GCP_PROJECT_ID", "test-project-for-archival-service-process")

	cfg, err := LoadArchivalServiceConfigFromEnv() // Assumes this function is available from your other package code
	require.NoError(t, err)
	cfg.NumArchivalWorkers = 1 // Use 1 worker for more deterministic testing of message processing order

	// Assumes NewArchivalService is available from your other package code
	// and ArchivalService has fields enrichedConsumer and unidentifiedConsumer that can be set.
	service, err := NewArchivalService(context.Background(), cfg, mockArchiver, logger)
	require.NoError(t, err)
	service.enrichedConsumer = mockEnrichedConsumer
	service.unidentifiedConsumer = mockUnidentifiedConsumer

	// Setup mock expectations for consumers
	mockEnrichedConsumer.On("Start", mock.Anything).Return(nil).Once()
	mockEnrichedConsumer.On("Messages").Return((<-chan ConsumedMessage)(mockEnrichedConsumer.MessagesChan))
	mockEnrichedConsumer.On("Stop").Return(nil).Once()
	mockEnrichedConsumer.On("Done").Return(mockEnrichedConsumer.DoneChan) // Or .Once() if applicable

	mockUnidentifiedConsumer.On("Start", mock.Anything).Return(nil).Once()
	mockUnidentifiedConsumer.On("Messages").Return((<-chan ConsumedMessage)(mockUnidentifiedConsumer.MessagesChan))
	mockUnidentifiedConsumer.On("Stop").Return(nil).Once()
	mockUnidentifiedConsumer.On("Done").Return(mockUnidentifiedConsumer.DoneChan) // Or .Once() if applicable

	// Setup mock expectations for archiver
	mockArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("simulated archive error")).Once()
	mockArchiver.On("Stop").Return(nil).Once()

	err = service.Start()
	require.NoError(t, err)

	enrichedPayload1 := []byte(`{"device_eui":"enriched001","location_id":"loc1"}`)
	enrichedTime1 := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Second)
	enrichedMsg1 := ConsumedMessage{ // Assumes ConsumedMessage struct is defined in your application code
		ID: "enriched_msg_1", Payload: enrichedPayload1, PublishTime: enrichedTime1,
		Ack:  func() { ackedMessages.Store("enriched_msg_1", true); t.Logf("ACKED enriched_msg_1") },
		Nack: func() { nackedMessages.Store("enriched_msg_1", true); t.Logf("NACKED enriched_msg_1") },
	}

	unidentifiedPayload1 := []byte(`{"device_eui":"unidentified001","processing_error":"metadata_missing"}`)
	unidentifiedTime1 := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
	unidentifiedMsg1 := ConsumedMessage{
		ID: "unidentified_msg_1", Payload: unidentifiedPayload1, PublishTime: unidentifiedTime1,
		Ack:  func() { ackedMessages.Store("unidentified_msg_1", true); t.Logf("ACKED unidentified_msg_1") },
		Nack: func() { nackedMessages.Store("unidentified_msg_1", true); t.Logf("NACKED unidentified_msg_1") },
	}

	errorPayload := []byte(`{"device_eui":"errorDevice001","location_id":"locError"}`)
	errorTime := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	errorMsg := ConsumedMessage{
		ID: "error_msg_1", Payload: errorPayload, PublishTime: errorTime,
		Ack:  func() { ackedMessages.Store("error_msg_1", true); t.Logf("ACKED error_msg_1") },
		Nack: func() { nackedMessages.Store("error_msg_1", true); t.Logf("NACKED error_msg_1") },
	}

	// Send messages
	mockEnrichedConsumer.MessagesChan <- enrichedMsg1
	mockUnidentifiedConsumer.MessagesChan <- unidentifiedMsg1
	mockEnrichedConsumer.MessagesChan <- errorMsg // This one will cause archiver to error

	// Wait until all messages have been processed and their Ack/Nack status is confirmed.
	require.Eventually(t, func() bool {
		_, e1Acked := ackedMessages.Load("enriched_msg_1")
		_, u1Acked := ackedMessages.Load("unidentified_msg_1") // Key for the reported error
		_, errMsgNacked := nackedMessages.Load("error_msg_1")
		_, errMsgWasAcked := ackedMessages.Load("error_msg_1") // Check error message was not accidentally Acked

		// Optional: Log current state for debugging if Eventually fails or when running with -v
		// t.Logf("Eventually check: e1Acked: %v, u1Acked: %v, errMsgNacked: %v, !errMsgWasAcked: %v", e1Acked, u1Acked, errMsgNacked, !errMsgWasAcked)

		return e1Acked && u1Acked && errMsgNacked && !errMsgWasAcked
	}, 3*time.Second, 100*time.Millisecond, "Messages did not achieve their expected Ack/Nack states in time. Ensure service correctly calls Ack/Nack and Stop waits for processing.")

	// Channels can be closed after Ack/Nack is confirmed by Eventually.
	close(mockEnrichedConsumer.MessagesChan)
	close(mockUnidentifiedConsumer.MessagesChan)

	service.Stop() // Ensure your service.Stop() properly waits for all ongoing processing (including Ack/Nack calls) to complete.

	// Assertions for mock calls
	mockEnrichedConsumer.AssertExpectations(t)
	mockUnidentifiedConsumer.AssertExpectations(t)
	mockArchiver.AssertExpectations(t)

	// Final state assertions (these should pass if require.Eventually passed)
	_, enrichedAcked := ackedMessages.Load("enriched_msg_1")
	assert.True(t, enrichedAcked, "Enriched message 1 should be Acked")

	_, unidentifiedAcked := ackedMessages.Load("unidentified_msg_1")
	assert.True(t, unidentifiedAcked, "Unidentified message 1 should be Acked")

	_, errorMsgNacked := nackedMessages.Load("error_msg_1")
	assert.True(t, errorMsgNacked, "Error message should be Nacked")
	_, errorMsgAcked := ackedMessages.Load("error_msg_1")
	assert.False(t, errorMsgAcked, "Error message should NOT be Acked")
}
