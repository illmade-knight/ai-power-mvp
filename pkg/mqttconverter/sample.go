package mqttconverter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

// CapturedMessage represents a single message saved to the output file.
type CapturedMessage struct {
	Timestamp time.Time       `json:"timestamp"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
}

// --- Sampler Core Logic ---

// Sampler encapsulates the logic for capturing MQTT messages.
type Sampler struct {
	config           MQTTClientConfig
	logger           zerolog.Logger
	numMessages      int
	client           mqtt.Client
	messages         []CapturedMessage
	mutex            sync.Mutex
	wg               sync.WaitGroup
	disconnectSignal chan struct{}
}

// NewSampler creates a new instance of the MQTT message sampler.
func NewSampler(cfg MQTTClientConfig, logger zerolog.Logger, numMessages int) *Sampler {
	return &Sampler{
		config:           cfg,
		logger:           logger,
		numMessages:      numMessages,
		messages:         make([]CapturedMessage, 0, numMessages),
		disconnectSignal: make(chan struct{}),
	}
}

// Run executes the main logic of the sampler. It connects, captures messages, and handles shutdown.
func (s *Sampler) Run(ctx context.Context) error {
	s.logger.Info().Msg("Starting Sampler run...")
	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	s.client = s.connect()
	if s.client == nil {
		return errors.New("could not create or connect MQTT client")
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.disconnectSignal:
			s.logger.Info().Msg("Disconnecting due to message count reached.")
		case <-runCtx.Done():
			s.logger.Info().Msg("Disconnecting due to context cancellation (e.g., OS signal).")
		}

		if s.client.IsConnected() {
			s.client.Disconnect(500)
			s.logger.Info().Msg("MQTT client disconnected.")
		}
	}()

	s.wg.Wait()
	s.logger.Info().Msg("Sampler run finished.")
	return nil
}

// Messages returns a copy of the captured messages.
func (s *Sampler) Messages() []CapturedMessage {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	msgsCopy := make([]CapturedMessage, len(s.messages))
	copy(msgsCopy, s.messages)
	return msgsCopy
}

// messageHandler is the callback for the Paho client. It captures messages.
func (s *Sampler) messageHandler(_ mqtt.Client, msg mqtt.Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.messages) >= s.numMessages {
		return
	}

	s.logger.Info().Str("topic", msg.Topic()).Int("size", len(msg.Payload())).Msg("Received message")

	var prettyPayload json.RawMessage
	var prettyBuf bytes.Buffer
	if err := json.Indent(&prettyBuf, msg.Payload(), "", "  "); err == nil {
		prettyPayload = prettyBuf.Bytes()
	} else {
		escapedString, _ := json.Marshal(string(msg.Payload()))
		prettyPayload = escapedString
	}

	captured := CapturedMessage{
		Timestamp: time.Now().UTC(),
		Topic:     msg.Topic(),
		Payload:   prettyPayload,
	}
	s.messages = append(s.messages, captured)
	s.logger.Info().Int("captured_count", len(s.messages)).Int("target_count", s.numMessages).Msg("Message captured")

	if len(s.messages) >= s.numMessages {
		s.logger.Info().Msg("Target message count reached. Signaling for shutdown.")
		close(s.disconnectSignal)
	}
}

// connect initializes and connects the Paho MQTT client.
func (s *Sampler) connect() mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.config.BrokerURL)
	opts.SetClientID(fmt.Sprintf("%s-sampler-%d", s.config.ClientIDPrefix, time.Now().UnixNano()%1000))
	opts.SetUsername(s.config.Username)
	opts.SetPassword(s.config.Password)
	opts.SetKeepAlive(s.config.KeepAlive)
	opts.SetConnectTimeout(s.config.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(120 * time.Second)
	opts.SetOrderMatters(false)

	if strings.HasPrefix(strings.ToLower(s.config.BrokerURL), "tls://") || strings.HasPrefix(strings.ToLower(s.config.BrokerURL), "ssl://") {
		tlsConfig, err := newTLSConfig(&s.config, s.logger)
		if err != nil {
			s.logger.Error().Err(err).Msg("Failed to create TLS config")
			return nil
		}
		opts.SetTLSConfig(tlsConfig)
		s.logger.Info().Msg("TLS configured for MQTT client.")
	}

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		s.logger.Info().Msg("Successfully connected to MQTT broker.")
		if token := c.Subscribe(s.config.Topic, 1, s.messageHandler); token.Wait() && token.Error() != nil {
			s.logger.Error().Err(token.Error()).Str("topic", s.config.Topic).Msg("Failed to subscribe to topic")
		} else {
			s.logger.Info().Str("topic", s.config.Topic).Msg("Successfully subscribed to topic.")
		}
	})
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		s.logger.Error().Err(err).Msg("MQTT connection lost. Reconnecting...")
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(s.config.ConnectTimeout) && token.Error() != nil {
		s.logger.Error().Err(token.Error()).Msg("Failed to connect to MQTT broker")
		return nil
	}
	return client
}

// --- Helper Functions ---

// loadMQTTClientConfigFromFile loads MQTT configuration from a JSON file.
func LoadMQTTClientConfigFromFile(filePath string) (*MQTTClientConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	// Unmarshal into a raw map to handle special types like duration manually
	var rawConfig map[string]interface{}
	if err := json.Unmarshal(data, &rawConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal raw config from %s: %w", filePath, err)
	}

	// Unmarshal again into the final struct to get most fields
	var cfg MQTTClientConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal typed config from %s: %w", filePath, err)
	}

	// Manually parse duration fields from the raw map (expecting seconds)
	if v, ok := rawConfig["keep_alive_seconds"].(float64); ok {
		cfg.KeepAlive = time.Duration(v) * time.Second
	}
	if v, ok := rawConfig["connect_timeout_seconds"].(float64); ok {
		cfg.ConnectTimeout = time.Duration(v) * time.Second
	}

	// Apply defaults
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = 60 * time.Second
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.ClientIDPrefix == "" {
		cfg.ClientIDPrefix = "mqtt-client-"
	}

	// Validation
	if cfg.BrokerURL == "" {
		return nil, errors.New("'broker_url' is a required field in the config file")
	}
	if cfg.Topic == "" {
		return nil, errors.New("'topic' is a required field in the config file")
	}

	return &cfg, nil
}

// LoadMQTTClientConfigFromEnv loads MQTT configuration from environment variables.
func loadMQTTClientConfigFromEnv() (*MQTTClientConfig, error) {
	cfg := &MQTTClientConfig{
		BrokerURL:      os.Getenv("MQTT_BROKER_URL"),
		Topic:          os.Getenv("MQTT_TOPIC"),
		ClientIDPrefix: os.Getenv("MQTT_CLIENT_ID_PREFIX"),
		Username:       os.Getenv("MQTT_USERNAME"),
		Password:       os.Getenv("MQTT_PASSWORD"),
		KeepAlive:      60 * time.Second,
		ConnectTimeout: 10 * time.Second,
		CACertFile:     os.Getenv("MQTT_CA_CERT_FILE"),
		ClientCertFile: os.Getenv("MQTT_CLIENT_CERT_FILE"),
		ClientKeyFile:  os.Getenv("MQTT_CLIENT_KEY_FILE"),
	}
	if skipVerify := os.Getenv("MQTT_INSECURE_SKIP_VERIFY"); skipVerify == "true" {
		cfg.InsecureSkipVerify = true
	}
	if cfg.BrokerURL == "" {
		return nil, errors.New("MQTT_BROKER_URL environment variable not set")
	}
	if cfg.Topic == "" {
		return nil, errors.New("MQTT_TOPIC environment variable not set")
	}
	if cfg.ClientIDPrefix == "" {
		cfg.ClientIDPrefix = "mqtt-client-"
	}
	return cfg, nil
}
