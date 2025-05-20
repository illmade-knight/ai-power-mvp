package xdevice

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock MessageConsumer ---
// Renamed from MockXDeviceMessageConsumer
type MockProcessingMessageConsumer struct {
	mock.Mock
	MessagesChan chan ConsumedMessage
	DoneChan     chan struct{}
}

// Renamed from NewMockXDeviceMessageConsumer
func NewMockProcessingMessageConsumer(bufferSize int) *MockProcessingMessageConsumer {
	return &MockProcessingMessageConsumer{
		MessagesChan: make(chan ConsumedMessage, bufferSize),
		DoneChan:     make(chan struct{}),
	}
}
func (m *MockProcessingMessageConsumer) Messages() <-chan ConsumedMessage {
	args := m.Called()
	if ch, ok := args.Get(0).(<-chan ConsumedMessage); ok {
		return ch
	}
	return m.MessagesChan
}
func (m *MockProcessingMessageConsumer) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
func (m *MockProcessingMessageConsumer) Stop() error {
	args := m.Called()
	select {
	case <-m.DoneChan:
	default:
		close(m.DoneChan)
	}
	return args.Error(0)
}
func (m *MockProcessingMessageConsumer) Done() <-chan struct{} {
	args := m.Called()
	if ch, ok := args.Get(0).(<-chan struct{}); ok {
		return ch
	}
	return m.DoneChan
}

// Ensure MockProcessingMessageConsumer satisfies MessageConsumer
var _ MessageConsumer = &MockProcessingMessageConsumer{}

// --- MockDecodedDataInserter ---
// Renamed from MockXDeviceDecodedDataInserter
type MockProcessingDecodedDataInserter struct {
	mock.Mock
}

func (m *MockProcessingDecodedDataInserter) Insert(ctx context.Context, reading MeterReading) error {
	args := m.Called(ctx, reading)
	return args.Error(0)
}
func (m *MockProcessingDecodedDataInserter) Close() error {
	args := m.Called()
	return args.Error(0)
}

var _ DecodedDataInserter = &MockProcessingDecodedDataInserter{}

// Renamed from TestXDeviceProcessingService_StartStop
func TestProcessingService_StartStop(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := NewMockProcessingMessageConsumer(10)   // Updated mock name
	mockInserter := new(MockProcessingDecodedDataInserter) // Updated mock name

	// Setup mock expectations
	mockConsumer.On("Start", mock.Anything).Return(nil)
	mockConsumer.On("Messages").Return(mockConsumer.MessagesChan)
	mockConsumer.On("Stop").Return(nil)
	mockConsumer.On("Done").Return(mockConsumer.DoneChan)
	mockInserter.On("Close").Return(nil)

	// Use ServiceConfig (which was ProcessingServiceConfig before)
	cfg := &ServiceConfig{
		UpstreamSubscriptionEnvVar: "TEST_SUB_PROCESSING_INPUT_SS", // Unique env var name for this test
		NumProcessingWorkers:       1,
	}

	// Use t.Setenv for test-scoped environment variables
	t.Setenv("GCP_PROJECT_ID", "test-project-for-processing-service-ss")
	t.Setenv(cfg.UpstreamSubscriptionEnvVar, "test-subscription-ss")
	// No defer needed for t.Setenv, it's automatically cleaned up

	// Use NewProcessingService (which was NewXDeviceProcessingService)
	realService, err := NewProcessingService(context.Background(), cfg, mockInserter, logger)
	require.NoError(t, err)
	realService.consumer = mockConsumer // Replace with mock

	err = realService.Start()
	require.NoError(t, err)

	close(mockConsumer.MessagesChan)
	realService.Stop()

	mockConsumer.AssertCalled(t, "Start", mock.Anything)
	mockConsumer.AssertCalled(t, "Stop")
	mockInserter.AssertCalled(t, "Close")
}

// Renamed from TestXDeviceProcessingService_ProcessConsumedMessage_SuccessfulDecodeAndInsert
func TestProcessingService_ProcessConsumedMessage_SuccessfulDecodeAndInsert(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := NewMockProcessingMessageConsumer(1)
	mockInserter := new(MockProcessingDecodedDataInserter)

	cfg := &ServiceConfig{NumProcessingWorkers: 1, UpstreamSubscriptionEnvVar: "DUMMY_SUB_ENV_PROC_OK_SUCCESS"} // Unique env var
	t.Setenv("GCP_PROJECT_ID", "test-project-proc-ok-success")
	t.Setenv(cfg.UpstreamSubscriptionEnvVar, "dummy-sub-proc-ok-success")

	service, _ := NewProcessingService(context.Background(), cfg, mockInserter, logger)
	service.consumer = mockConsumer

	validHexPayload, _ := createValidTestHexPayload("X001", 10.5, 1.1, 2.2, 220.0, 219.5)

	upstreamMsg := ConsumedUpstreamMessage{
		DeviceEUI:        "EUI_X001",
		RawPayload:       validHexPayload,
		OriginalMQTTTime: time.Now().Add(-5 * time.Minute),
		ClientID:         "ClientX",
		LocationID:       "LocationY",
		DeviceCategory:   "Sensor",
	}
	payloadBytes, _ := json.Marshal(upstreamMsg)

	var ackCalled bool
	consumedMsg := ConsumedMessage{
		ID: "msg1_proc_ok", Payload: payloadBytes, PublishTime: upstreamMsg.OriginalMQTTTime,
		Ack:  func() { ackCalled = true },
		Nack: func() { t.Error("Nack should not be called for successful processing") },
	}

	mockConsumer.On("Start", mock.Anything).Return(nil)
	mockConsumer.On("Messages").Return(mockConsumer.MessagesChan)
	mockConsumer.On("Stop").Return(nil)
	mockConsumer.On("Done").Return(mockConsumer.DoneChan)
	mockInserter.On("Insert", mock.Anything, mock.MatchedBy(func(mr MeterReading) bool {
		return mr.UID == "X001" && mr.Reading == 10.5 && mr.DeviceEUI == "EUI_X001" && mr.ClientID == "ClientX" && mr.DeviceType == "XDevice"
	})).Return(nil).Once()
	mockInserter.On("Close").Return(nil).Once() // Added .Once() here

	err := service.Start()
	require.NoError(t, err)

	mockConsumer.MessagesChan <- consumedMsg
	close(mockConsumer.MessagesChan)
	service.Stop()

	mockInserter.AssertExpectations(t)
	assert.True(t, ackCalled, "Message should be Acked on successful processing and insert")
}

// Renamed from TestXDeviceProcessingService_ProcessConsumedMessage_DecodeError
func TestProcessingService_ProcessConsumedMessage_DecodeError(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := NewMockProcessingMessageConsumer(1)
	mockInserter := new(MockProcessingDecodedDataInserter)

	cfg := &ServiceConfig{NumProcessingWorkers: 1, UpstreamSubscriptionEnvVar: "DUMMY_SUB_ENV_PROC_DEC_ERR_V2"} // Unique env var
	t.Setenv("GCP_PROJECT_ID", "test-project-proc-dec-err-v2")
	t.Setenv(cfg.UpstreamSubscriptionEnvVar, "dummy-sub-proc-dec-err-v2")

	service, _ := NewProcessingService(context.Background(), cfg, mockInserter, logger)
	service.consumer = mockConsumer

	upstreamMsg := ConsumedUpstreamMessage{
		DeviceEUI:  "EUI_X002_DEC_ERR",
		RawPayload: "invalid-hex-payload",
	}
	payloadBytes, _ := json.Marshal(upstreamMsg)

	var nackCalled bool
	consumedMsg := ConsumedMessage{
		ID: "msg2_proc_dec_err", Payload: payloadBytes, PublishTime: time.Now(),
		Ack:  func() { t.Error("Ack should not be called on decode error") },
		Nack: func() { nackCalled = true },
	}

	mockConsumer.On("Start", mock.Anything).Return(nil)
	mockConsumer.On("Messages").Return(mockConsumer.MessagesChan)
	mockConsumer.On("Stop").Return(nil)
	mockConsumer.On("Done").Return(mockConsumer.DoneChan)
	mockInserter.On("Close").Return(nil)

	err := service.Start()
	require.NoError(t, err)

	mockConsumer.MessagesChan <- consumedMsg
	close(mockConsumer.MessagesChan)
	service.Stop()

	mockInserter.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything)
	assert.True(t, nackCalled, "Message should be Nacked on decode error")
}

// Renamed from TestXDeviceProcessingService_ProcessConsumedMessage_UnmarshalError
func TestProcessingService_ProcessConsumedMessage_UnmarshalError(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := NewMockProcessingMessageConsumer(1)
	mockInserter := new(MockProcessingDecodedDataInserter)

	cfg := &ServiceConfig{NumProcessingWorkers: 1, UpstreamSubscriptionEnvVar: "DUMMY_SUB_ENV_PROC_UNM_ERR_V2"} // Unique env var
	t.Setenv("GCP_PROJECT_ID", "test-project-proc-unm-err-v2")
	t.Setenv(cfg.UpstreamSubscriptionEnvVar, "dummy-sub-proc-unm-err-v2")

	service, _ := NewProcessingService(context.Background(), cfg, mockInserter, logger)
	service.consumer = mockConsumer

	invalidPayloadBytes := []byte("this is not a valid ConsumedUpstreamMessage JSON")

	var nackCalled bool
	consumedMsg := ConsumedMessage{
		ID: "msg3_proc_unm_err", Payload: invalidPayloadBytes, PublishTime: time.Now(),
		Ack:  func() { t.Error("Ack should not be called on unmarshal error") },
		Nack: func() { nackCalled = true },
	}

	mockConsumer.On("Start", mock.Anything).Return(nil)
	mockConsumer.On("Messages").Return(mockConsumer.MessagesChan)
	mockConsumer.On("Stop").Return(nil)
	mockConsumer.On("Done").Return(mockConsumer.DoneChan)
	mockInserter.On("Close").Return(nil)

	err := service.Start()
	require.NoError(t, err)

	mockConsumer.MessagesChan <- consumedMsg
	close(mockConsumer.MessagesChan)
	service.Stop()

	mockInserter.AssertNotCalled(t, "Insert", mock.Anything, mock.Anything)
	assert.True(t, nackCalled, "Message should be Nacked on unmarshal error")
}
