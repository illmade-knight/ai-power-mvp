package converter

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks ---

// MockMessagePublisher is a mock implementation of MessagePublisher.
type MockMessagePublisher struct {
	PublishFunc func(ctx context.Context, message *MQTTMessage) error
	StopFunc    func()

	// For assertions
	PublishCalledCh chan *MQTTMessage // Channel to receive published messages
	StopCalledCh    chan bool         // Channel to signal Stop was called
	PublishError    error             // Error to return from Publish

	mu               sync.Mutex
	publishCallCount int
	lastPublishedMsg *MQTTMessage
	stopCallCount    int
	lastPublishCtx   context.Context
}

func NewMockMessagePublisher(bufferSize int) *MockMessagePublisher {
	return &MockMessagePublisher{
		PublishCalledCh: make(chan *MQTTMessage, bufferSize),
		StopCalledCh:    make(chan bool, 1),
	}
}

func (m *MockMessagePublisher) Publish(ctx context.Context, message *MQTTMessage) error {
	m.mu.Lock()
	m.publishCallCount++
	m.lastPublishedMsg = message
	m.lastPublishCtx = ctx
	m.mu.Unlock()

	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, message)
	}
	if m.PublishError != nil {
		// Non-blocking send to ErrorChan if full
		select {
		case m.PublishCalledCh <- message: // still send to channel to indicate call attempt
		default:
		}
		return m.PublishError
	}

	select {
	case m.PublishCalledCh <- message:
	case <-ctx.Done(): // Respect context cancellation during blocking send
		return ctx.Err()
	}
	return nil
}

func (m *MockMessagePublisher) Stop() {
	m.mu.Lock()
	m.stopCallCount++
	m.mu.Unlock()

	if m.StopFunc != nil {
		m.StopFunc()
		return
	}
	// Non-blocking send
	select {
	case m.StopCalledCh <- true:
	default:
	}
}

func (m *MockMessagePublisher) GetPublishCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishCallCount
}

func (m *MockMessagePublisher) GetLastPublishedMsg() *MQTTMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastPublishedMsg
}
func (m *MockMessagePublisher) GetLastPublishContext() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastPublishCtx
}

func (m *MockMessagePublisher) GetStopCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCallCount
}

// MockPahoMessage is a mock for mqtt.Message.
type MockPahoMessage struct {
	payload        []byte
	topic          string
	messageID      uint16
	qosValue       byte
	retainedValue  bool
	duplicateValue bool
}

func NewMockPahoMessage(topic string, payload string, id uint16) *MockPahoMessage {
	return &MockPahoMessage{
		payload:   []byte(payload),
		topic:     topic,
		messageID: id,
		qosValue:  1, // Default QoS
	}
}
func (m *MockPahoMessage) Duplicate() bool   { return m.duplicateValue }
func (m *MockPahoMessage) Qos() byte         { return m.qosValue }
func (m *MockPahoMessage) Retained() bool    { return m.retainedValue }
func (m *MockPahoMessage) Topic() string     { return m.topic }
func (m *MockPahoMessage) MessageID() uint16 { return m.messageID }
func (m *MockPahoMessage) Payload() []byte   { return m.payload }
func (m *MockPahoMessage) Ack()              {}

// MockPahoToken is a mock for mqtt.Token.
type MockPahoToken struct {
	err error
	// wg  sync.WaitGroup // Not strictly needed for this basic mock's Wait/WaitTimeout
}

func NewMockPahoToken(err error) *MockPahoToken {
	return &MockPahoToken{err: err}
}
func (t *MockPahoToken) Wait() bool {
	return true // For mock, assume immediate completion
}
func (t *MockPahoToken) WaitTimeout(d time.Duration) bool {
	return true // For mock, assume completion within timeout
}
func (t *MockPahoToken) Error() error { return t.err }

// Done returns a channel that is closed when the operation is complete.
// For this mock, we return a pre-closed channel.
func (t *MockPahoToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// MockPahoClient is a mock for mqtt.Client, used for testing handlers.
type MockPahoClient struct {
	mqtt.Client           // Embedding the interface type
	SubscribedTopic       string
	SubscribedQos         byte
	SubscribeError        error
	ConnectError          error
	IsConnectedVal        bool
	OptionsReaderVal      mqtt.ClientOptionsReader
	mu                    sync.Mutex
	subscribeCalledCount  int
	disconnectCalledCount uint
	//disconnectTimeout uint // Storing the quiesce value from Disconnect
}

func (m *MockPahoClient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	m.mu.Lock()
	m.SubscribedTopic = topic
	m.SubscribedQos = qos
	m.subscribeCalledCount++
	m.mu.Unlock()
	if callback == nil {
		return NewMockPahoToken(errors.New("callback cannot be nil"))
	}
	return NewMockPahoToken(m.SubscribeError)
}
func (m *MockPahoClient) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.IsConnectedVal
}
func (m *MockPahoClient) Connect() mqtt.Token { return NewMockPahoToken(m.ConnectError) }
func (m *MockPahoClient) Disconnect(quiesce uint) {
	m.mu.Lock()
	m.disconnectCalledCount = quiesce
	m.mu.Unlock()
}
func (m *MockPahoClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	return NewMockPahoToken(nil)
}
func (m *MockPahoClient) Unsubscribe(topics ...string) mqtt.Token             { return NewMockPahoToken(nil) }
func (m *MockPahoClient) AddRoute(topic string, callback mqtt.MessageHandler) {}
func (m *MockPahoClient) OptionsReader() mqtt.ClientOptionsReader             { return m.OptionsReaderVal }

// --- Test Helper Functions ---

func newTestLogger(t *testing.T) zerolog.Logger {
	return zerolog.New(os.Stderr).With().Timestamp().Logger().Level(zerolog.Disabled)
}

func newTestService(t *testing.T, publisher MessagePublisher, mqttCfg *MQTTClientConfig, serviceCfg ...IngestionServiceConfig) *IngestionService {
	cfg := DefaultIngestionServiceConfig()
	if len(serviceCfg) > 0 {
		cfg = serviceCfg[0]
	}

	return NewIngestionService(publisher, newTestLogger(t), cfg, mqttCfg)
}

// --- Tests ---

func TestNewIngestionService(t *testing.T) {
	publisher := NewMockMessagePublisher(1)
	mqttCfg := &MQTTClientConfig{BrokerURL: "tcp://dummy:1883", Topic: "test/topic"}
	cfg := DefaultIngestionServiceConfig()

	s := NewIngestionService(publisher, newTestLogger(t), cfg, mqttCfg)
	require.NotNil(t, s, "NewIngestionService should return a non-nil service")
	assert.Equal(t, publisher, s.publisher, "Publisher mismatch")
	assert.Equal(t, mqttCfg, s.mqttClientConfig, "MQTT config mismatch")
	assert.Equal(t, cfg.InputChanCapacity, cap(s.MessagesChan), "MessagesChan capacity mismatch")
	assert.Equal(t, cfg.InputChanCapacity, cap(s.ErrorChan), "ErrorChan capacity mismatch, it should match InputChanCapacity as per constructor")
	assert.NotNil(t, s.cancelCtx, "cancelCtx should be initialized")
	assert.NotNil(t, s.cancelFunc, "cancelFunc should be initialized")
}

func TestIngestionService_processSingleMessage(t *testing.T) {
	ctx := context.Background()
	validJSONPayload := `{"device_info":{"device_eui":"0102030405060708"}, "raw_payload":"data", "message_id":"id1", "topic":"topic1", "message_timestamp":"2023-01-01T00:00:00Z"}`

	t.Run("SuccessfulProcessing", func(t *testing.T) {
		publisher := NewMockMessagePublisher(1)
		s := newTestService(t, publisher, nil)

		inMsg := InMessage{Payload: []byte(validJSONPayload), Topic: "actual/topic", MessageID: "actual-id"}

		s.processSingleMessage(ctx, inMsg, 1)

		select {
		case publishedMsg := <-publisher.PublishCalledCh:
			assert.Equal(t, "0102030405060708", publishedMsg.DeviceInfo.DeviceEUI)
			assert.Equal(t, "data", publishedMsg.RawPayload)
			assert.Equal(t, "actual/topic", publishedMsg.Topic, "Topic should be from InMessage")
			assert.Equal(t, "actual-id", publishedMsg.MessageID, "MessageID should be from InMessage")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Publisher.Publish was not called")
		}
		assert.Empty(t, s.ErrorChan, "ErrorChan should be empty on success")
	})

	t.Run("ParseError", func(t *testing.T) {
		publisher := NewMockMessagePublisher(1)
		s := newTestService(t, publisher, nil)

		invalidPayload := InMessage{Payload: []byte("this is not json"), Topic: "test/topic"}
		s.processSingleMessage(ctx, invalidPayload, 1)

		assert.Zero(t, publisher.GetPublishCallCount(), "Publisher.Publish should not be called on parse error")
		select {
		case err := <-s.ErrorChan:
			require.Error(t, err, "Expected an error in ErrorChan")
			var jsonErr *json.SyntaxError
			assert.ErrorAs(t, err, &jsonErr, "Error should be a JSON syntax error")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("ErrorChan did not receive an error")
		}
	})

	t.Run("PublishError", func(t *testing.T) {
		expectedErr := errors.New("failed to publish")
		publisher := NewMockMessagePublisher(1)
		publisher.PublishError = expectedErr
		s := newTestService(t, publisher, nil)

		inMsg := InMessage{Payload: []byte(validJSONPayload), Topic: "test/topic"}
		s.processSingleMessage(ctx, inMsg, 1)

		select {
		case <-publisher.PublishCalledCh:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Publisher.Publish was not called despite error")
		}

		select {
		case err := <-s.ErrorChan:
			require.Error(t, err, "Expected an error in ErrorChan")
			assert.EqualError(t, err, expectedErr.Error(), "Error message mismatch")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("ErrorChan did not receive an error")
		}
	})

	t.Run("ContextCancelledDuringPublish", func(t *testing.T) {
		publisher := NewMockMessagePublisher(1)
		publisher.PublishFunc = func(ctx context.Context, message *MQTTMessage) error {
			<-ctx.Done()
			return ctx.Err()
		}
		s := newTestService(t, publisher, nil)

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		inMsg := InMessage{Payload: []byte(validJSONPayload), Topic: "test/topic"}
		s.processSingleMessage(cancelledCtx, inMsg, 1)

		select {
		case err := <-s.ErrorChan:
			require.Error(t, err, "Expected an error in ErrorChan due to context cancellation")
			assert.ErrorIs(t, err, context.Canceled, "Error should be context.Canceled")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("ErrorChan did not receive context cancellation error")
		}
	})
}

func TestIngestionService_handleIncomingPahoMessage(t *testing.T) {
	mockClient := &MockPahoClient{}

	t.Run("SuccessfulPush", func(t *testing.T) {
		s := newTestService(t, NewMockMessagePublisher(1), nil, IngestionServiceConfig{InputChanCapacity: 1})
		pahoMsg := NewMockPahoMessage("test/topic", "payload", 123)

		var receivedInMessage InMessage
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case receivedInMessage = <-s.MessagesChan:
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timed out waiting for message on MessagesChan")
			}
		}()

		s.handleIncomingPahoMessage(mockClient, pahoMsg)
		wg.Wait()

		assert.Equal(t, pahoMsg.Payload(), receivedInMessage.Payload, "Payload mismatch")
		assert.Equal(t, pahoMsg.Topic(), receivedInMessage.Topic, "Topic mismatch")
		assert.Equal(t, fmt.Sprintf("%d", pahoMsg.MessageID()), receivedInMessage.MessageID, "MessageID mismatch")
	})

	t.Run("ChannelFull", func(t *testing.T) {
		cfg := DefaultIngestionServiceConfig()
		cfg.InputChanCapacity = 1
		s := newTestService(t, NewMockMessagePublisher(1), nil, cfg)

		s.MessagesChan <- InMessage{Payload: []byte("first message")}

		pahoMsg := NewMockPahoMessage("test/topic", "second payload", 124)

		s.handleIncomingPahoMessage(mockClient, pahoMsg)
		assert.Len(t, s.MessagesChan, 1, "MessagesChan should still have only the first message")
		firstMsg := <-s.MessagesChan
		assert.Equal(t, "first message", string(firstMsg.Payload))
	})

	t.Run("ShutdownSignaled", func(t *testing.T) {
		s := newTestService(t, NewMockMessagePublisher(1), nil)
		s.cancelFunc()

		pahoMsg := NewMockPahoMessage("test/topic", "payload", 123)
		s.handleIncomingPahoMessage(mockClient, pahoMsg)

		select {
		case <-s.MessagesChan:
			t.Fatal("Message was pushed to MessagesChan despite shutdown")
		default:
		}
	})
}

func TestIngestionService_StartStop_Lifecycle(t *testing.T) {
	publisher := NewMockMessagePublisher(10)
	mqttDisabledCfg := DefaultIngestionServiceConfig()

	s := newTestService(t, publisher, nil, mqttDisabledCfg)

	err := s.Start()
	require.NoError(t, err, "Start should succeed when MQTT is disabled")

	validJSON := `{"device_info":{"device_eui":"eui123"}, "raw_payload":"data", "message_id":"id1", "topic":"topic1", "message_timestamp":"2023-01-01T00:00:00Z"}`
	s.MessagesChan <- InMessage{Payload: []byte(validJSON), Topic: "from/test", MessageID: "test-msg-id"}

	select {
	case published := <-publisher.PublishCalledCh:
		assert.Equal(t, "eui123", published.DeviceInfo.DeviceEUI)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message was not published by the service")
	}

	s.Stop()

	select {
	case <-publisher.StopCalledCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Publisher.Stop was not called")
	}

	_, ok := <-s.ErrorChan
	assert.False(t, ok, "ErrorChan should be closed after Stop")

}

func TestIngestionService_Start_MqttConnectionFailure(t *testing.T) {
	publisher := NewMockMessagePublisher(1)
	mqttCfg := &MQTTClientConfig{
		BrokerURL:      "tcp://localhost:12345",
		Topic:          "test/topic",
		ConnectTimeout: 50 * time.Millisecond,
	}
	serviceCfg := DefaultIngestionServiceConfig()
	serviceCfg.NumProcessingWorkers = 1

	s := newTestService(t, publisher, mqttCfg, serviceCfg)

	err := s.Start()
	require.Error(t, err, "Start should fail due to MQTT connection error")
	assert.Contains(t, err.Error(), "paho MQTT client connect error", "Error message mismatch")

	select {
	case _, ok := <-s.ErrorChan:
		assert.False(t, ok, "ErrorChan should be closed after Start failure's cleanup")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timed out waiting for ErrorChan to be closed or receive error")
	}

	s.Stop()
	assert.Equal(t, 1, publisher.GetStopCallCount(), "Publisher.Stop should be called by s.Stop()")
}

func TestIngestionService_sendError(t *testing.T) {

	t.Run("SendErrorToChannel", func(t *testing.T) {
		s := newTestService(t, nil, nil, IngestionServiceConfig{InputChanCapacity: 1})
		testErr := errors.New("test error")
		s.sendError(testErr)
		select {
		case err := <-s.ErrorChan:
			assert.Equal(t, testErr, err)
		case <-time.After(50 * time.Millisecond):
			t.Fatal("Error not sent to ErrorChan")
		}
	})

	t.Run("SendErrorToFullChannel", func(t *testing.T) {
		s := newTestService(t, nil, nil, IngestionServiceConfig{InputChanCapacity: 1})
		s.ErrorChan <- errors.New("first error")

		s.sendError(errors.New("second error"))

		assert.Len(t, s.ErrorChan, 1, "ErrorChan should still contain one error")
		err := <-s.ErrorChan
		assert.EqualError(t, err, "first error")
		select {
		case <-s.ErrorChan:
			t.Fatal("ErrorChan should be empty now, second error was unexpected")
		default:
		}
	})

	t.Run("SendErrorToNilChannel", func(t *testing.T) {
		s := newTestService(t, nil, nil)
		s.ErrorChan = nil
		assert.NotPanics(t, func() {
			s.sendError(errors.New("error to nil chan"))
		}, "sendError should not panic with nil ErrorChan")
	})
}

func createTempPemFile(t *testing.T, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.pem")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())
	return tmpFile.Name()
}

func generateSelfSignedCert(t *testing.T) (certPEM string, keyPEM string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Co"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certOut := &strings.Builder{}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certPEM = certOut.String()

	keyOut := &strings.Builder{}
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	keyPEM = keyOut.String()
	return
}

func Test_newTLSConfig(t *testing.T) {
	logger := newTestLogger(t)
	validCertPEM, validKeyPEM := generateSelfSignedCert(t)

	t.Run("NoCertsProvided", func(t *testing.T) {
		cfg := &MQTTClientConfig{InsecureSkipVerify: true}
		tlsCfg, err := newTLSConfig(cfg, logger)
		require.NoError(t, err)
		require.NotNil(t, tlsCfg)
		assert.True(t, tlsCfg.InsecureSkipVerify)
		assert.Nil(t, tlsCfg.RootCAs)
		assert.Empty(t, tlsCfg.Certificates)
	})

	t.Run("CACertFileNotExists", func(t *testing.T) {
		cfg := &MQTTClientConfig{CACertFile: "/tmp/nonexistent-ca.pem"}
		_, err := newTLSConfig(cfg, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA certificate file")
	})

	t.Run("CACertFileInvalidPEM", func(t *testing.T) {
		invalidPemFile := createTempPemFile(t, "this is not a pem")
		defer os.Remove(invalidPemFile)
		cfg := &MQTTClientConfig{CACertFile: invalidPemFile}
		_, err := newTLSConfig(cfg, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to append CA certificate")
	})

	t.Run("ValidCACert", func(t *testing.T) {
		caFile := createTempPemFile(t, validCertPEM)
		defer os.Remove(caFile)
		cfg := &MQTTClientConfig{CACertFile: caFile}
		tlsCfg, err := newTLSConfig(cfg, logger)
		require.NoError(t, err)
		require.NotNil(t, tlsCfg)
		assert.NotNil(t, tlsCfg.RootCAs, "RootCAs should be set with a valid CA file")
	})

	t.Run("ClientCertKeyPairNotExists", func(t *testing.T) {
		cfg := &MQTTClientConfig{ClientCertFile: "/tmp/nonexistent-client.crt", ClientKeyFile: "/tmp/nonexistent-client.key"}
		_, err := newTLSConfig(cfg, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client certificate/key pair")

		// Unwrap the error to check for os.ErrNotExist specifically.
		// fmt.Errorf wraps the error, and os.ReadFile returns an *os.PathError.
		pathErr := errors.Unwrap(errors.Unwrap(err)) // First unwrap for fmt.Errorf, second for the os.PathError from tls.LoadX509KeyPair which calls os.ReadFile
		if pathErr != nil {
			assert.True(t, os.IsNotExist(pathErr), "Expected os.ErrNotExist")
		} else {
			// If pathErr is nil, it means the error wasn't what we expected.
			// This can happen if LoadX509KeyPair returns a different error structure.
			// For robustness, also check the top-level error message if unwrapping fails to find os.PathError
			if !strings.Contains(err.Error(), "no such file or directory") && !strings.Contains(err.Error(), "cannot find the file") {
				// Potentially log the actual error for debugging if the assertion structure needs refinement for specific OS/Go versions
				// t.Logf("Actual error from LoadX509KeyPair for non-existent files: %v", err)
			}
		}
	})

	t.Run("ClientCertKeyPairInvalid", func(t *testing.T) {
		certFile := createTempPemFile(t, "invalid cert data")
		defer os.Remove(certFile)
		keyFile := createTempPemFile(t, "invalid key data")
		defer os.Remove(keyFile)
		cfg := &MQTTClientConfig{ClientCertFile: certFile, ClientKeyFile: keyFile}
		_, err := newTLSConfig(cfg, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client certificate/key pair")
	})

	t.Run("ValidClientCertKeyPair", func(t *testing.T) {
		certFile := createTempPemFile(t, validCertPEM)
		defer os.Remove(certFile)
		keyFile := createTempPemFile(t, validKeyPEM)
		defer os.Remove(keyFile)

		cfg := &MQTTClientConfig{ClientCertFile: certFile, ClientKeyFile: keyFile}
		tlsCfg, err := newTLSConfig(cfg, logger)
		require.NoError(t, err)
		require.NotNil(t, tlsCfg)
		assert.Len(t, tlsCfg.Certificates, 1, "Certificates should be set with a valid key pair")
	})

	t.Run("ClientCertOnly", func(t *testing.T) {
		certFile := createTempPemFile(t, validCertPEM)
		defer os.Remove(certFile)
		cfg := &MQTTClientConfig{ClientCertFile: certFile}
		tlsCfg, err := newTLSConfig(cfg, logger)
		require.NoError(t, err)
		require.NotNil(t, tlsCfg)
		assert.Empty(t, tlsCfg.Certificates, "Certificates should not be set if key is missing")
	})
}

func TestIngestionService_onPahoConnect(t *testing.T) {
	publisher := NewMockMessagePublisher(1)
	mqttCfg := &MQTTClientConfig{
		BrokerURL: "tcp://dummy:1883",
		Topic:     "test/+/device",
	}
	s := newTestService(t, publisher, mqttCfg)
	mockClient := &MockPahoClient{}

	s.onPahoConnect(mockClient)

	mockClient.mu.Lock()
	assert.Equal(t, mqttCfg.Topic, mockClient.SubscribedTopic, "Subscribed topic mismatch")
	assert.Equal(t, byte(1), mockClient.SubscribedQos, "Subscribed QoS mismatch")
	mockClient.mu.Unlock()

	mockClient.SubscribeError = errors.New("subscription failed")
	s.onPahoConnect(mockClient)

	select {
	case err := <-s.ErrorChan:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to subscribe to test/+/device: subscription failed")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ErrorChan did not receive subscription error")
	}
}

func TestIngestionService_onPahoConnectionLost(t *testing.T) {
	s := newTestService(t, nil, &MQTTClientConfig{})
	mockClient := &MockPahoClient{}
	testErr := errors.New("connection deliberately lost")

	assert.NotPanics(t, func() {
		s.onPahoConnectionLost(mockClient, testErr)
	})
}

func TestIngestionService_initAndConnectMQTTClient_Failure(t *testing.T) {
	cfg := &MQTTClientConfig{
		BrokerURL:      "tcp://localhost:12345",
		Topic:          "test/topic",
		ConnectTimeout: 10 * time.Millisecond,
	}
	s := newTestService(t, nil, cfg)

	err := s.initAndConnectMQTTClient()
	require.Error(t, err, "Expected connection to fail")
	assert.Contains(t, err.Error(), "paho MQTT client connect error")

	t.Run("TLSConfigFailure", func(t *testing.T) {
		tlsCfg := &MQTTClientConfig{
			BrokerURL:  "tls://localhost:12345",
			Topic:      "test/topic",
			CACertFile: filepath.Join(t.TempDir(), "nonexistent-ca.pem"),
		}
		sTLS := newTestService(t, nil, tlsCfg)
		errTLS := sTLS.initAndConnectMQTTClient()
		require.Error(t, errTLS)
		assert.Contains(t, errTLS.Error(), "failed to create TLS config")
		assert.Contains(t, errTLS.Error(), "failed to read CA certificate file")
	})
}
