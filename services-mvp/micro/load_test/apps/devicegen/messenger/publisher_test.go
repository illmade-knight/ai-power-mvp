package messenger

import (
	"context"
	"errors"
	"fmt"
	// "os" // No longer needed for NewPublisher test if logger is Nop
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang" // Keep for mqtt.ClientOptions and mqtt.Token
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mock MQTT Client, Token, and Factory ---

// MockMQTTToken implements the mqtt.Token interface for testing.
type MockMQTTToken struct {
	mock.Mock
	err error // Store error to be returned by Error()
}

func (t *MockMQTTToken) Wait() bool {
	args := t.Called()
	return args.Bool(0)
}
func (t *MockMQTTToken) WaitTimeout(timeout time.Duration) bool {
	args := t.Called(timeout)
	return args.Bool(0)
}
func (t *MockMQTTToken) Error() error {
	t.Called()
	return t.err
}
func (t *MockMQTTToken) Done() <-chan struct{} {
	args := t.Called()
	ch, ok := args.Get(0).(<-chan struct{})
	if !ok || ch == nil {
		// Return a dummy closed channel if not set, to avoid nil panic
		dummyCh := make(chan struct{})
		close(dummyCh)
		return dummyCh
	}
	return ch
}

// MockMQTTClient implements our messenger.MQTTClient interface.
// Its methods Connect() and Publish() now return mqtt.Token.
type MockMQTTClient struct {
	mock.Mock
}

func (m *MockMQTTClient) IsConnected() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockMQTTClient) Connect() mqtt.Token { // Returns mqtt.Token interface
	args := m.Called()
	returnedArg := args.Get(0) // Get the first return argument

	if returnedArg == nil {
		// If .Return(nil) or .Return((mqtt.Token)(nil)) was used,
		// or if .Return() was not called for an interface type,
		// then returnedArg will be nil. In this case, return a nil mqtt.Token.
		return nil
	}

	// If returnedArg is not nil, it should be a concrete type that implements mqtt.Token.
	token, ok := returnedArg.(mqtt.Token)
	if !ok {
		// This means .Return() was called with a non-nil value that is not an mqtt.Token.
		// This is a test setup error.
		panic(fmt.Sprintf("MockMQTTClient.Connect: returned value is not an mqtt.Token: got %T", returnedArg))
	}
	return token
}

func (m *MockMQTTClient) Disconnect(quiesce uint) {
	m.Called(quiesce)
}

func (m *MockMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token { // Returns mqtt.Token
	// Pass the received arguments to m.Called()
	args := m.Called(topic, qos, retained, payload)
	returnedArg := args.Get(0) // Get the first return argument

	if returnedArg == nil {
		// If .Return(nil) or .Return((mqtt.Token)(nil)) was used,
		// or if .Return() was not called for an interface type,
		// then returnedArg will be nil. In this case, return a nil mqtt.Token.
		return nil
	}

	// If returnedArg is not nil, it should be a concrete type that implements mqtt.Token.
	token, ok := returnedArg.(mqtt.Token)
	if !ok {
		// This means .Return() was called with a non-nil value that is not an mqtt.Token.
		// This is a test setup error.
		panic(fmt.Sprintf("MockMQTTClient.Publish: returned value is not an mqtt.Token: got %T", returnedArg))
	}
	return token
}

// MockMQTTClientFactory implements the MQTTClientFactory interface for testing.
type MockMQTTClientFactory struct {
	mock.Mock
}

// NewClient is the method from MQTTClientFactory.
func (f *MockMQTTClientFactory) NewClient(opts *mqtt.ClientOptions) MQTTClient {
	args := f.Called(opts)
	returnedArg := args.Get(0) // Get the first return argument

	if returnedArg == nil {
		// If .Return(nil) or .Return((MQTTClient)(nil)) was used,
		// or if .Return() was not called for an interface type,
		// then returnedArg will be nil. In this case, return a nil MQTTClient.
		return nil
	}

	client, ok := returnedArg.(MQTTClient) // Expecting our MQTTClient interface
	if !ok {
		// This means .Return() was called with a non-nil value that is not an MQTTClient.
		// This is a test setup error.
		panic(fmt.Sprintf("mock factory NewClient was expected to return MQTTClient but got %T", returnedArg))
	}
	return client
}

// TestNewPublisher tests the NewPublisher function with the factory.
func TestNewPublisher(t *testing.T) {
	expectedTopicPattern := "devices/{DEVICE_EUI}/up"
	expectedBrokerURL := "tcp://localhost:1883"
	expectedClientID := "test-publisher-client"
	expectedQOS := 1
	expectedLogger := zerolog.Nop()
	mockFactory := new(MockMQTTClientFactory)

	publisher := NewPublisher(expectedTopicPattern, expectedBrokerURL, expectedClientID, expectedQOS, expectedLogger, mockFactory)
	assert.NotNil(t, publisher)

	mp, ok := publisher.(*mqttPublisher)
	assert.True(t, ok, "NewPublisher should return a *mqttPublisher")

	assert.Equal(t, expectedTopicPattern, mp.topicPattern)
	assert.Equal(t, expectedBrokerURL, mp.brokerURL)
	assert.Equal(t, expectedClientID, mp.clientID)
	assert.Equal(t, expectedQOS, mp.QOS)
	assert.Equal(t, expectedLogger, mp.logger)
	assert.Equal(t, mockFactory, mp.clientFactory, "Client factory not stored correctly")

	// Test with nil factory, expecting default Paho factory to be used
	publisherNilFactory := NewPublisher(expectedTopicPattern, expectedBrokerURL, expectedClientID, expectedQOS, expectedLogger, nil)
	assert.NotNil(t, publisherNilFactory)
	mpNilFactory, ok := publisherNilFactory.(*mqttPublisher)
	assert.True(t, ok)
	assert.IsType(t, &PahoMQTTClientFactory{}, mpNilFactory.clientFactory, "Default factory should be PahoMQTTClientFactory when nil is passed")

}

func TestMqttPublisher_connectMQTT(t *testing.T) {
	logger := zerolog.Nop()
	const testBrokerURL = "tcp://127.0.0.1:1883"

	t.Run("Successful connection", func(t *testing.T) {
		mockFactory := new(MockMQTTClientFactory)
		mockClient := new(MockMQTTClient)
		mockToken := new(MockMQTTToken)

		p := &mqttPublisher{
			brokerURL:     testBrokerURL,
			clientID:      "connect-test-success",
			logger:        logger,
			clientFactory: mockFactory,
		}

		mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClient).Once()
		mockClient.On("Connect").Return(mockToken).Once()
		mockToken.On("WaitTimeout", 10*time.Second).Return(true).Once()
		mockToken.On("Error").Return(nil).Once() // Error called once in the if condition
		mockClient.On("IsConnected").Return(true).Once()

		err := p.connectMQTT()

		assert.NoError(t, err)
		assert.Equal(t, mockClient, p.mqttClient)
		mockFactory.AssertExpectations(t)
		mockClient.AssertExpectations(t)
		mockToken.AssertExpectations(t)
	})

	t.Run("Factory returns nil client", func(t *testing.T) {
		mockFactory := new(MockMQTTClientFactory)
		p := &mqttPublisher{
			brokerURL:     testBrokerURL,
			clientID:      "factory-nil-client",
			logger:        logger,
			clientFactory: mockFactory,
		}
		mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return((MQTTClient)(nil)).Once()

		err := p.connectMQTT()
		assert.Error(t, err)
		assert.Equal(t, "client factory returned a nil client", err.Error())
		mockFactory.AssertExpectations(t)
	})

	t.Run("Client Connect returns nil token", func(t *testing.T) {
		mockFactory := new(MockMQTTClientFactory)
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			brokerURL:     testBrokerURL,
			clientID:      "client-nil-token",
			logger:        logger,
			clientFactory: mockFactory,
		}
		mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClient).Once()
		mockClient.On("Connect").Return((mqtt.Token)(nil)).Once() // This setup should now work correctly.

		err := p.connectMQTT()
		assert.Error(t, err) // Expect an error because connectToken will be nil.
		assert.Equal(t, "mqtt client Connect() returned a nil token", err.Error())
		mockFactory.AssertExpectations(t)
		mockClient.AssertExpectations(t)
		// No calls to mockToken methods are expected if connectToken is nil.
	})

	t.Run("Connection timeout", func(t *testing.T) {
		mockFactory := new(MockMQTTClientFactory)
		mockClient := new(MockMQTTClient)
		mockToken := new(MockMQTTToken) // This is the token Connect() will return
		p := &mqttPublisher{
			brokerURL:     testBrokerURL,
			clientID:      "connect-test-timeout",
			logger:        logger,
			clientFactory: mockFactory,
		}
		// For this path, the error returned by connectMQTT will be from the IsConnected check,
		// not from the token, because WaitTimeout returning false short-circuits the token error check.
		expectedConnectMQTTError := errors.New("mqtt client failed to connect without explicit token error")
		// mockToken.err is not relevant here as mockToken.Error() won't be called in this path.

		mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClient).Once()
		mockClient.On("Connect").Return(mockToken).Once()
		// Key: WaitTimeout returns false.
		mockToken.On("WaitTimeout", 10*time.Second).Return(false).Once()
		// IMPORTANT: mockToken.Error() is NOT called in the SUT if WaitTimeout returns false
		// because of the `&&` short-circuit. So, no expectation for mockToken.Error().

		// The code then proceeds to `if !p.mqttClient.IsConnected()`.
		mockClient.On("IsConnected").Return(false).Once() // Simulate client not being connected

		err := p.connectMQTT()
		assert.Error(t, err)
		assert.Equal(t, expectedConnectMQTTError, err)

		mockFactory.AssertExpectations(t)
		mockClient.AssertExpectations(t)
		mockToken.AssertExpectations(t) // Verifies WaitTimeout was called, Error was not.
	})

	t.Run("Connection error after WaitTimeout true", func(t *testing.T) {
		mockFactory := new(MockMQTTClientFactory)
		mockClient := new(MockMQTTClient)
		mockToken := new(MockMQTTToken)
		p := &mqttPublisher{
			brokerURL:     testBrokerURL,
			clientID:      "connect-test-err-after-wait",
			logger:        logger,
			clientFactory: mockFactory,
		}
		connectError := errors.New("some connection error")
		mockToken.err = connectError

		mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClient).Once()
		mockClient.On("Connect").Return(mockToken).Once()
		mockToken.On("WaitTimeout", 10*time.Second).Return(true).Once()
		mockToken.On("Error").Return(connectError).Times(2)

		err := p.connectMQTT()
		assert.Error(t, err)
		assert.Equal(t, connectError, err)
		mockFactory.AssertExpectations(t)
		mockClient.AssertExpectations(t)
		mockToken.AssertExpectations(t)
	})

	t.Run("Connection fails IsConnected check", func(t *testing.T) {
		mockFactory := new(MockMQTTClientFactory)
		mockClient := new(MockMQTTClient)
		mockToken := new(MockMQTTToken)
		p := &mqttPublisher{
			brokerURL:     testBrokerURL,
			clientID:      "connect-test-isconnfail",
			logger:        logger,
			clientFactory: mockFactory,
		}
		mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClient).Once()
		mockClient.On("Connect").Return(mockToken).Once()
		mockToken.On("WaitTimeout", 10*time.Second).Return(true).Once()
		mockToken.On("Error").Return(nil).Once()
		mockClient.On("IsConnected").Return(false).Once()

		err := p.connectMQTT()
		assert.Error(t, err)
		assert.Equal(t, "mqtt client failed to connect without explicit token error", err.Error())
		mockFactory.AssertExpectations(t)
		mockClient.AssertExpectations(t)
		mockToken.AssertExpectations(t)
	})
}

func TestMqttPublisher_disconnectMQTT(t *testing.T) {
	logger := zerolog.Nop()
	t.Run("Disconnect when connected", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{logger: logger, mqttClient: mockClient}
		mockClient.On("IsConnected").Return(true).Once()
		mockClient.On("Disconnect", uint(250)).Return().Once()
		p.disconnectMQTT()
		mockClient.AssertExpectations(t)
	})
	t.Run("Skip disconnect if not connected", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{logger: logger, mqttClient: mockClient}
		mockClient.On("IsConnected").Return(false).Once()
		p.disconnectMQTT()
		mockClient.AssertExpectations(t)
		mockClient.AssertNotCalled(t, "Disconnect", mock.Anything)
	})
	t.Run("Skip disconnect if client is nil", func(t *testing.T) {
		pNilClient := &mqttPublisher{logger: logger, mqttClient: nil}
		assert.NotPanics(t, func() { pNilClient.disconnectMQTT() })
	})
}

func TestMqttPublisher_publishMessages(t *testing.T) {
	logger := zerolog.Nop()
	devEUI := "TESTDEVICE001"
	device := NewDevice(devEUI, 1.0, func(in string) []byte { return []byte("payload_for_" + in) })
	baseTopicPattern := "devices/{DEVICE_EUI}/data"
	expectedTopic := strings.ReplaceAll(baseTopicPattern, "{DEVICE_EUI}", devEUI)
	qos := byte(1)

	t.Run("Publish successfully for a short duration", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			topicPattern: baseTopicPattern,
			logger:       logger,
			mqttClient:   mockClient,
			QOS:          int(qos),
		}
		publishDuration := 2500 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), publishDuration)
		defer cancel()
		mockPublishToken := new(MockMQTTToken)
		mockPublishToken.err = nil

		mockClient.On("IsConnected").Return(true).Times(2)
		// Ensure the mock.AnythingOfTypeArgument matches the actual payload type
		mockClient.On("Publish", expectedTopic, qos, false, mock.AnythingOfType("[]uint8")).
			Return(mockPublishToken).Times(2)
		mockPublishToken.On("Error").Return(nil).Times(2) // Error called once per successful publish

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); p.publishMessages(device, publishDuration, ctx) }()
		wg.Wait()
		mockClient.AssertExpectations(t)
		mockPublishToken.AssertExpectations(t)
	})

	t.Run("Publish stops if client disconnects mid-way", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			topicPattern: baseTopicPattern,
			logger:       logger,
			mqttClient:   mockClient,
			QOS:          int(qos),
		}
		publishDuration := 3500 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), publishDuration)
		defer cancel()
		mockPublishTokenSuccess := new(MockMQTTToken)
		mockPublishTokenSuccess.err = nil

		mockClient.On("IsConnected").Return(true).Once()
		mockClient.On("Publish", expectedTopic, qos, false, mock.AnythingOfType("[]uint8")).
			Return(mockPublishTokenSuccess).Once()
		mockPublishTokenSuccess.On("Error").Return(nil).Once()
		mockClient.On("IsConnected").Return(false)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); p.publishMessages(device, publishDuration, ctx) }()
		wg.Wait()
		mockClient.AssertExpectations(t)
		mockPublishTokenSuccess.AssertExpectations(t)
	})

	t.Run("Context cancelled early", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			topicPattern: baseTopicPattern,
			logger:       logger,
			mqttClient:   mockClient,
			QOS:          int(qos),
		}
		veryShortDuration := 50 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), veryShortDuration)
		defer cancel()
		mockClient.On("IsConnected").Return(true).Maybe()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); p.publishMessages(device, 10*time.Second, ctx) }()
		wg.Wait()
		mockClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockClient.AssertExpectations(t)
	})

	t.Run("Publish returns nil token", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			topicPattern: baseTopicPattern,
			logger:       logger,
			mqttClient:   mockClient,
			QOS:          int(qos),
		}
		publishDuration := 1500 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), publishDuration)
		defer cancel()

		mockClient.On("IsConnected").Return(true).Once()
		mockClient.On("Publish", expectedTopic, qos, false, mock.AnythingOfType("[]uint8")).
			Return((mqtt.Token)(nil)).Once() // This setup should now work correctly.

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); p.publishMessages(device, publishDuration, ctx) }()
		wg.Wait()
		mockClient.AssertExpectations(t)
	})

	t.Run("Publish error occurs", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			topicPattern: baseTopicPattern,
			logger:       logger,
			mqttClient:   mockClient,
			QOS:          int(qos),
		}
		publishDuration := 1500 * time.Millisecond // Expect 1 message attempt
		ctx, cancel := context.WithTimeout(context.Background(), publishDuration)
		defer cancel()
		mockErrorToken := new(MockMQTTToken)
		publishError := errors.New("failed to publish")
		mockErrorToken.err = publishError

		mockClient.On("IsConnected").Return(true).Once()
		mockClient.On("Publish", expectedTopic, qos, false, mock.AnythingOfType("[]uint8")).
			Return(mockErrorToken).Once()
		// If pubToken.Error() is non-nil, it's called twice: once in the 'if' and once for logging.
		mockErrorToken.On("Error").Return(publishError).Times(2)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); p.publishMessages(device, publishDuration, ctx) }()
		wg.Wait()
		mockClient.AssertExpectations(t)
		mockErrorToken.AssertExpectations(t)
	})

	t.Run("Zero message rate, no publishing", func(t *testing.T) {
		mockClient := new(MockMQTTClient)
		p := &mqttPublisher{
			topicPattern: baseTopicPattern,
			logger:       logger,
			mqttClient:   mockClient,
			QOS:          int(qos),
		}
		zeroRateDevice := NewDevice("ZERORATE001", 0, func(in string) []byte { return []byte("payload") })
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); p.publishMessages(zeroRateDevice, 1*time.Second, ctx) }()
		wg.Wait()
		mockClient.AssertNotCalled(t, "IsConnected")
		mockClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockClient.AssertExpectations(t)
	})
}

func TestMqttPublisher_Publish_Concurrent(t *testing.T) {
	logger := zerolog.Nop()
	mockFactory := new(MockMQTTClientFactory)
	mockClientGlobal := new(MockMQTTClient)
	mockConnectToken := new(MockMQTTToken)
	mockPublishToken := new(MockMQTTToken)

	configuredTopicPattern := "devices/{DEVICE_EUI}/up"
	configuredBrokerURL := "tcp://127.0.0.1:1883"
	configuredClientID := "concurrent-publish-test"
	configuredQOS := 0

	p := NewPublisher(configuredTopicPattern, configuredBrokerURL, configuredClientID, configuredQOS, logger, mockFactory).(*mqttPublisher)

	mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClientGlobal).Once()
	mockConnectToken.err = nil
	mockClientGlobal.On("Connect").Return(mockConnectToken).Once()
	mockConnectToken.On("WaitTimeout", 10*time.Second).Return(true).Once()
	mockConnectToken.On("Error").Return(nil).Once() // From connectMQTT success path

	mockClientGlobal.On("IsConnected").Return(true).Times(5)
	mockClientGlobal.On("Disconnect", uint(250)).Return().Once()

	dev1EUI := "CONCDEV001"
	dev2EUI := "CONCDEV002"
	devices := []*Device{
		NewDevice(dev1EUI, 1.0, func(in string) []byte { return []byte("p1-" + in) }),
		NewDevice(dev2EUI, 2.0, func(in string) []byte { return []byte("p2-" + in) }),
	}
	testDuration := 1100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	mockPublishToken.err = nil
	expectedTopicDev1 := strings.ReplaceAll(configuredTopicPattern, "{DEVICE_EUI}", dev1EUI)
	mockClientGlobal.On("Publish", expectedTopicDev1, byte(configuredQOS), false, mock.AnythingOfType("[]uint8")).
		Return(mockPublishToken).Times(1)
	expectedTopicDev2 := strings.ReplaceAll(configuredTopicPattern, "{DEVICE_EUI}", dev2EUI)
	mockClientGlobal.On("Publish", expectedTopicDev2, byte(configuredQOS), false, mock.AnythingOfType("[]uint8")).
		Return(mockPublishToken).Times(2)
	// Error() in publishMessages for successful publishes:
	mockPublishToken.On("Error").Return(nil).Times(3) // Total 3 successful publish calls (once per call to Error())

	err := p.Publish(devices, testDuration, ctx)
	assert.NoError(t, err)

	mockFactory.AssertExpectations(t)
	mockClientGlobal.AssertExpectations(t)
	mockConnectToken.AssertExpectations(t)
	mockPublishToken.AssertExpectations(t)
}

func TestMqttPublisher_Publish_ConnectFails(t *testing.T) {
	logger := zerolog.Nop()
	mockFactory := new(MockMQTTClientFactory)
	mockClient := new(MockMQTTClient)
	mockToken := new(MockMQTTToken)

	const connectFailBrokerURL = "tcp://127.0.0.1:1883"
	p := NewPublisher("topic", connectFailBrokerURL, "cfail", 0, logger, mockFactory).(*mqttPublisher)

	connectError := errors.New("simulated connection failure")
	mockToken.err = connectError

	mockFactory.On("NewClient", mock.AnythingOfType("*mqtt.ClientOptions")).Return(mockClient).Once()
	mockClient.On("Connect").Return(mockToken).Once()
	mockToken.On("WaitTimeout", 10*time.Second).Return(true).Once()
	mockToken.On("Error").Return(connectError).Times(2)

	devices := []*Device{NewDevice("ANYDEV", 1.0, func(in string) []byte { return []byte("any") })}
	testDuration := 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	err := p.Publish(devices, testDuration, ctx)
	assert.Error(t, err)
	assert.Equal(t, connectError, err)

	mockFactory.AssertExpectations(t)
	mockClient.AssertExpectations(t)
	mockToken.AssertExpectations(t)
	mockClient.AssertNotCalled(t, "IsConnected")
	mockClient.AssertNotCalled(t, "Disconnect", mock.Anything)
	mockClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
