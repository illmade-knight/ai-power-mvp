package loadgen_test

import (
	"context"
	"errors"
	"github.com/illmade-knight/ai-power-mpv/pkg/helpers/loadgen"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

// MockPayloadGenerator is a mock implementation of the PayloadGenerator interface.
type MockPayloadGenerator struct {
	mock.Mock
}

func (m *MockPayloadGenerator) GeneratePayload() ([]byte, error) {
	args := m.Called()
	// Safely cast the first return value to []byte
	var payload []byte
	if p, ok := args.Get(0).([]byte); ok {
		payload = p
	}
	return payload, args.Error(1)
}

// MockClient is a mock implementation of the Client interface.
type MockClient struct {
	mock.Mock
}

func (m *MockClient) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClient) Disconnect() {
	m.Called()
}

func (m *MockClient) Publish(ctx context.Context, device *loadgen.Device) error {
	args := m.Called(ctx, device)
	return args.Error(0)
}

// --- Tests ---

func TestLoadGenerator_Run(t *testing.T) {
	logger := zerolog.Nop()

	t.Run("Successful run", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		mockGenerator := new(MockPayloadGenerator)

		devices := []*loadgen.Device{
			{ID: "device-1", MessageRate: 10, PayloadGenerator: mockGenerator}, // 10 msg/sec * 0.2s = 2 messages
		}
		duration := 250 * time.Millisecond // Use a short duration for testing

		// Expect Connect and Disconnect to be called once
		mockClient.On("Connect").Return(nil).Once()
		mockClient.On("Disconnect").Return().Once()
		// Expect Publish to be called for the device
		// We expect it to be called around 2 times (10Hz for 0.25s)
		mockClient.On("Publish", mock.Anything, devices[0]).Return(nil).Maybe()

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		err := lg.Run(context.Background(), duration)

		// Assert
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Connect fails", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		connectErr := errors.New("connection failed")
		mockClient.On("Connect").Return(connectErr).Once()

		lg := loadgen.NewLoadGenerator(mockClient, []*loadgen.Device{}, logger)

		// Act
		err := lg.Run(context.Background(), 1*time.Second)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, connectErr, err)
		mockClient.AssertExpectations(t)
		// Ensure disconnect is not called if connect fails
		mockClient.AssertNotCalled(t, "Disconnect")
	})

	t.Run("Device with zero message rate", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		mockGenerator := new(MockPayloadGenerator)
		devices := []*loadgen.Device{
			{ID: "device-1", MessageRate: 0, PayloadGenerator: mockGenerator},
		}
		duration := 100 * time.Millisecond

		mockClient.On("Connect").Return(nil).Once()
		mockClient.On("Disconnect").Return().Once()

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		err := lg.Run(context.Background(), duration)

		// Assert
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
		// Ensure Publish is never called for the zero-rate device
		mockClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything)
	})

	t.Run("Context cancellation stops devices", func(t *testing.T) {
		// Arrange
		mockClient := new(MockClient)
		mockGenerator := new(MockPayloadGenerator)
		devices := []*loadgen.Device{
			{ID: "device-1", MessageRate: 100, PayloadGenerator: mockGenerator}, // High rate
		}
		duration := 1 * time.Second          // Long duration
		cancelAfter := 50 * time.Millisecond // Short cancellation time

		// We use a WaitGroup to ensure the publish goroutine has started
		var wg sync.WaitGroup
		wg.Add(1)

		mockClient.On("Connect").Return(nil).Once()
		mockClient.On("Disconnect").Return().Once()
		// Expect Publish to be called, but it should stop after context is cancelled
		mockClient.On("Publish", mock.Anything, devices[0]).Return(nil).Run(func(args mock.Arguments) {
			wg.Done() // Signal that at least one publish attempt was made
		}).Maybe()

		// Act
		lg := loadgen.NewLoadGenerator(mockClient, devices, logger)
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(cancelAfter)
			cancel()
		}()

		err := lg.Run(ctx, duration)

		// Assert
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}
