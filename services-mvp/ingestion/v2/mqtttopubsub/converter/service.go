package converter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// ErrMetadataNotFound is an error returned when metadata for a device cannot be found.
var ErrMetadataNotFound = errors.New("device metadata not found")

// ErrInvalidMessage is an error for when an incoming MQTT message is invalid for processing.
var ErrInvalidMessage = errors.New("invalid MQTT message for processing")

// IngestionServiceConfig holds configuration for the IngestionService.
type IngestionServiceConfig struct {
	InputChanCapacity    int // Capacity of the input channel for raw messages
	OutputChanCapacity   int // Capacity of the output channel for enriched messages
	NumProcessingWorkers int // Number of goroutines to process messages concurrently
}

// DefaultIngestionServiceConfig provides sensible defaults.
func DefaultIngestionServiceConfig() IngestionServiceConfig {
	return IngestionServiceConfig{
		InputChanCapacity:    100,
		OutputChanCapacity:   100,
		NumProcessingWorkers: 5, // Adjust based on expected load and processing time
	}
}

// IngestionService processes raw messages from an input channel, enriches them,
// and sends them to an output channel.
type IngestionService struct {
	// the change from our refactor to add Paho
	mqttClientConfig *MQTTClientConfig
	pahoClient       mqtt.Client
	// the change from our refactor to add pubsub
	publisher MessagePublisher // Using the interface

	config IngestionServiceConfig
	logger zerolog.Logger

	// Public channels for interaction
	MessagesChan chan InMessage // Input: Raw JSON messages
	ErrorChan    chan error     // Output: Errors encountered during processing (optional, for observability)

	// a private channel for internal signaling
	// for the moment we stick with using the cancelCtx
	//shutdownChan chan struct{}
	// internal signaling and cleanup
	cancelCtx  context.Context    // Used to signal shutdown to workers
	cancelFunc context.CancelFunc // Function to call to initiate shutdown

	wg sync.WaitGroup
	// sync.Once for safely closing errorChan and other one-time shutdown actions
	closeErrorChanOnce sync.Once
	// compare with cancelCtx
	//shutdownOnce       sync.Once // For overall shutdown logic if Stop can be called multiple times
}

// NewIngestionService creates and initializes a new IngestionService.
// NewIngestionService creates a new IngestionService now with a mqttCfg
func NewIngestionService(
	publisher MessagePublisher, // Inject the publisher
	logger zerolog.Logger,
	serviceCfg IngestionServiceConfig,
	mqttCfg *MQTTClientConfig,
) *IngestionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestionService{
		config:           serviceCfg,
		mqttClientConfig: mqttCfg,
		publisher:        publisher,
		logger:           logger,
		cancelCtx:        ctx,
		cancelFunc:       cancel,
		MessagesChan:     make(chan InMessage, serviceCfg.InputChanCapacity),
		ErrorChan:        make(chan error, serviceCfg.InputChanCapacity), // Ensure ErrorChan always has capacity
	}
}

// handleIncomingPahoMessage is the Paho MessageHandler.
// It pushes the payload to the MessagesChan for the workers.
func (s *IngestionService) handleIncomingPahoMessage(client mqtt.Client, msg mqtt.Message) {
	s.logger.Debug().Str("topic", msg.Topic()).Int("payload_size", len(msg.Payload())).Msg("Paho client received message")

	// Create a copy of the payload to avoid issues if Paho reuses the buffer.
	messagePayload := make([]byte, len(msg.Payload()))
	copy(messagePayload, msg.Payload())

	message := InMessage{
		Payload:   messagePayload,
		Topic:     msg.Topic(),
		MessageID: fmt.Sprintf("%d", msg.MessageID()),
	}

	select {
	case s.MessagesChan <- message:
		s.logger.Debug().Str("topic", msg.Topic()).Msg("Message pushed to MessagesChan")
	case <-s.cancelCtx.Done():
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Shutdown signaled, Paho message dropped")
	default:
		// This case means MessagesChan is full. This indicates a bottleneck.
		s.logger.Error().Str("topic", msg.Topic()).Msg("MessagesChan is full. Paho message dropped. " +
			"Consider increasing capacity or worker count.")
		// Optionally, send an error to s.ErrorChan or implement other backpressure handling.
	}
}

// processSingleMessage handles parsing and enrichment of one raw message.
func (s *IngestionService) processSingleMessage(ctx context.Context, msg InMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Int("msg_size_bytes", len(msg.Payload)).Msg("Received raw message for processing")

	mqttMsg, err := ParseMQTTMessage(msg.Payload)
	if err != nil {
		// Determine max length for snippet to avoid panic on very short payloads
		snippetLen := len(msg.Payload)
		if snippetLen > 100 {
			snippetLen = 100
		}
		s.logger.Error().
			Int("worker_id", workerID).
			Err(err).
			Str("raw_message_snippet", string(msg.Payload[:snippetLen])). // Log a snippet
			Msg("Failed to parse raw MQTT message")
		s.sendError(err) // Optionally send parse errors to ErrorChan
		return
	}
	mqttMsg.Topic = msg.Topic
	mqttMsg.MessageID = msg.MessageID

	// Publish the message
	// Use a child context with a timeout for the publish operation if desired
	publishCtx, publishCancel := context.WithTimeout(ctx, 30*time.Second) // Example timeout
	defer publishCancel()

	if err := s.publisher.Publish(publishCtx, mqttMsg); err != nil {
		s.logger.Error().
			Int("worker_id", workerID).
			Str("topic", mqttMsg.Topic).
			Str("id", mqttMsg.MessageID).
			Str("eui", mqttMsg.DeviceInfo.DeviceEUI). // Assuming DeviceInfo is always present after successful parse
			Err(err).
			Msg("Failed to publish message")
		s.sendError(err) // Send this critical error to the error channel
		// Decide on retry strategy or if message should be dead-lettered
	} else {
		s.logger.Debug().Int("worker_id", workerID).Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Msg("Successfully processed and published enriched message")
	}
}

// sendError attempts to send an error to the ErrorChan if it's non-nil and there's capacity.
func (s *IngestionService) sendError(err error) {
	if s.ErrorChan == nil {
		s.logger.Warn().Err(err).Msg("Attempted to send error to a nil ErrorChan.")
		return
	}
	// Non-blocking send to error channel
	select {
	case s.ErrorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("ErrorChan is full or nil, dropping error")
	}
}

// Start begins the message processing workers and connects the MQTT client.
func (s *IngestionService) Start() error {
	s.logger.Info().Int("workers", s.config.NumProcessingWorkers).Msg("Starting IngestionService...")

	// Start worker goroutines first
	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Info().Int("worker_id", workerID).Msg("Starting processing worker")
			for {
				select {
				case <-s.cancelCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down due to context cancellation")
					return
				case message, ok := <-s.MessagesChan:
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("MessagesChan closed, worker stopping")
						return
					}
					s.processSingleMessage(s.cancelCtx, message, workerID) // Pass context for publisher
				}
			}
		}(i)
	}

	// Then, initialize and connect MQTT client if config available other assume we're testing etc
	if s.mqttClientConfig == nil {
		s.logger.Info().Msg("IngestionService started successfully, running without MQTT client.")
		return nil
	}
	if err := s.initAndConnectMQTTClient(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to initialize or connect MQTT client during Start. Service may not receive messages.")
		s.cancelFunc() // Signal workers to stop

		go func() { // Non-blocking wait and channel close for cleanup
			s.wg.Wait()
			if s.ErrorChan != nil {
				// Use the sync.Once here as well to prevent multiple closes
				s.closeErrorChanOnce.Do(func() {
					s.logger.Info().Msg("Closing ErrorChan from Start() failure cleanup...")
					close(s.ErrorChan)
					s.logger.Info().Msg("ErrorChan closed (from Start() failure cleanup).")
				})
			}
			s.logger.Info().Msg("Worker goroutines stopped after MQTT connection failure.")
		}()
		return err // Propagate the error
	}

	s.logger.Info().Msg("IngestionService started successfully, including MQTT client connection attempt.")
	return nil
}

// Stop gracefully shuts down the IngestionService.
func (s *IngestionService) Stop() {
	s.logger.Info().Msg("Stopping IngestionService...")

	if s.pahoClient != nil && s.pahoClient.IsConnected() {
		s.logger.Info().Msg("Disconnecting Paho MQTT client...")
		s.pahoClient.Disconnect(250) // Timeout in milliseconds for disconnection
		s.logger.Info().Msg("Paho MQTT client disconnected.")
	}

	s.logger.Info().Msg("Signalling worker goroutines to stop...")
	s.cancelFunc() // This signals workers via s.cancelCtx.Done()

	// It's generally safer not to close MessagesChan if an external component (Paho) is writing to it.
	// Paho's disconnection and the context cancellation should be the primary signals for workers.
	// If Paho is guaranteed to stop writing before/during this Stop(), then closing might be okay.
	// The current logic: workers will exit when s.cancelCtx.Done() is signaled OR s.MessagesChan is closed.
	// Since s.cancelFunc() is called, they will exit due to context cancellation.
	// MessagesChan does not need to be closed here for worker shutdown if cancelCtx is used.

	s.logger.Info().Msg("Waiting for worker goroutines to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All worker goroutines completed.")

	if s.publisher != nil {
		s.publisher.Stop() // Stop the message publisher
	}

	// Safely close ErrorChan using sync.Once
	if s.ErrorChan != nil {
		s.closeErrorChanOnce.Do(func() {
			s.logger.Info().Msg("Closing ErrorChan in Stop()...")
			close(s.ErrorChan)
			s.logger.Info().Msg("ErrorChan closed in Stop().")
		})
	} else {
		s.logger.Info().Msg("ErrorChan is nil (was never initialized or already cleaned up), no need to close in Stop().")
	}
	s.logger.Info().Msg("IngestionService stopped.")
}

// onPahoConnect is called when the Paho client successfully connects to the broker.
func (s *IngestionService) onPahoConnect(client mqtt.Client) {
	s.logger.Info().Str("broker", s.mqttClientConfig.BrokerURL).Msg("Paho client connected to MQTT broker")
	topic := s.mqttClientConfig.Topic
	qos := byte(1) // At least once

	s.logger.Info().Str("topic", topic).Msg("Subscribing to MQTT topic")
	if token := client.Subscribe(topic, qos, s.handleIncomingPahoMessage); token.Wait() && token.Error() != nil {
		s.logger.Error().Err(token.Error()).Str("topic", topic).Msg("Failed to subscribe to MQTT topic")
		s.sendError(fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error()))
	} else {
		s.logger.Info().Str("topic", topic).Msg("Successfully subscribed to MQTT topic")
	}
}

// onPahoConnectionLost is called when the Paho client loses its connection.
func (s *IngestionService) onPahoConnectionLost(client mqtt.Client, err error) {
	s.logger.Error().Err(err).Msg("Paho client lost MQTT connection")
	// Paho's auto-reconnect should handle this if enabled in options.
}

// newTLSConfig creates a TLS configuration for MQTT client.
func newTLSConfig(cfg *MQTTClientConfig, logger zerolog.Logger) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CACertFile != "" {
		caCert, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			logger.Error().Err(err).Str("ca_cert_file", cfg.CACertFile).Msg("Failed to read CA certificate file")
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", cfg.CACertFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			logger.Error().Str("ca_cert_file", cfg.CACertFile).Msg("Failed to append CA certificate to pool")
			return nil, fmt.Errorf("failed to append CA certificate from %s to pool", cfg.CACertFile)
		}
		tlsConfig.RootCAs = caCertPool
		logger.Info().Str("ca_cert_file", cfg.CACertFile).Msg("CA certificate loaded")
	}

	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			logger.Error().Err(err).
				Str("client_cert_file", cfg.ClientCertFile).
				Str("client_key_file", cfg.ClientKeyFile).
				Msg("Failed to load client certificate/key pair")
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		logger.Info().
			Str("client_cert_file", cfg.ClientCertFile).
			Str("client_key_file", cfg.ClientKeyFile).
			Msg("Client certificate and key loaded for mTLS")
	} else if cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
		logger.Warn().Msg("Client certificate or key file provided without its pair; mTLS will not be configured.")
	}

	return tlsConfig, nil
}

// initAndConnectMQTTClient initializes and connects the Paho MQTT client.
func (s *IngestionService) initAndConnectMQTTClient() error {
	opts := mqtt.NewClientOptions()
	s.logger.Debug().Str("broker", s.mqttClientConfig.BrokerURL).Msg("adding broker")
	opts.AddBroker(s.mqttClientConfig.BrokerURL)

	uniqueSuffix := time.Now().UnixNano() % 1000000
	opts.SetClientID(fmt.Sprintf("%s%d", s.mqttClientConfig.ClientIDPrefix, uniqueSuffix)) // Ensure ClientIDPrefix exists or default

	opts.SetUsername(s.mqttClientConfig.Username)
	opts.SetPassword(s.mqttClientConfig.Password)

	opts.SetKeepAlive(s.mqttClientConfig.KeepAlive)
	opts.SetConnectTimeout(s.mqttClientConfig.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(s.mqttClientConfig.ReconnectWaitMax)
	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		s.logger.Info().Str("broker", broker.String()).Msg("Attempting to connect to MQTT broker")
		return tlsCfg
	})

	// Setup TLS
	if strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "tls://") ||
		strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "ssl://") ||
		(strings.Contains(s.mqttClientConfig.BrokerURL, ":") && strings.Split(s.mqttClientConfig.BrokerURL, ":")[len(strings.Split(s.mqttClientConfig.BrokerURL, ":"))-1] == "8883") {

		tlsConfig, err := newTLSConfig(s.mqttClientConfig, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
		s.logger.Info().Msg("TLS configured for MQTT client.")
	} else {
		s.logger.Info().Msg("TLS not explicitly configured for MQTT client (broker URL does not start with tls:// or ssl://, or use common TLS port 8883).")
	}

	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Received unexpected message on subscribed client (default handler)")
	})
	opts.SetOnConnectHandler(s.onPahoConnect)
	opts.SetConnectionLostHandler(s.onPahoConnectionLost)

	s.pahoClient = mqtt.NewClient(opts)
	s.logger.Info().Str("client_id", opts.ClientID).Msg("Paho MQTT client created. Attempting to connect...")

	if token := s.pahoClient.Connect(); token.WaitTimeout(s.mqttClientConfig.ConnectTimeout) && token.Error() != nil {
		s.logger.Error().Err(token.Error()).Msg("Failed to connect Paho MQTT client")
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}

	if !s.pahoClient.IsConnected() {
		// This state might be transient, as connection might be happening in background.
		// onPahoConnect is the definitive signal of successful connection and subscription.
		// The Connect().WaitTimeout() above should block until connection or timeout.
		// If it didn't error, but IsConnected is false, it's unusual but could happen if disconnected immediately.
		s.logger.Warn().Msg("Paho client Connect() call did not error, but IsConnected() is false. Waiting for OnConnect handler or ConnectionLost.")
	}

	return nil
}
