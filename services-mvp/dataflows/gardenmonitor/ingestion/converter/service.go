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
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// ErrInvalidMessage is an error for when an incoming MQTT message is invalid for processing.
var ErrInvalidMessage = errors.New("invalid MQTT message for processing")

// IngestionServiceConfig holds configuration for the IngestionService.
type IngestionServiceConfig struct {
	InputChanCapacity    int // Capacity of the input channel for raw messages
	NumProcessingWorkers int // Number of goroutines to process messages concurrently
}

// DefaultIngestionServiceConfig provides sensible defaults.
// MODIFIED: Increased input channel capacity significantly to better handle bursts.
// This value should be tuned based on expected load and memory constraints.
func DefaultIngestionServiceConfig() IngestionServiceConfig {
	return IngestionServiceConfig{
		InputChanCapacity:    5000, // Increased from 100 to provide a larger buffer
		NumProcessingWorkers: 20,   // Increased from 5. Tune based on I/O wait time.
	}
}

// IngestionService processes raw messages from an input channel, enriches them,
// and sends them to an output channel.
type IngestionService struct {
	mqttClientConfig *MQTTClientConfig
	pahoClient       mqtt.Client
	publisher        MessagePublisher // Using the interface

	config IngestionServiceConfig
	logger zerolog.Logger

	MessagesChan chan InMessage // Input: Raw JSON messages
	ErrorChan    chan error     // Output: Errors encountered during processing

	cancelCtx  context.Context
	cancelFunc context.CancelFunc

	wg                    sync.WaitGroup
	closeErrorChanOnce    sync.Once
	closeMessagesChanOnce sync.Once   // MODIFICATION: Ensures MessagesChan is closed only once.
	isShuttingDown        atomic.Bool // MODIFICATION: Atomically signals that shutdown has started.
}

// NewIngestionService creates and initializes a new IngestionService.
func NewIngestionService(
	publisher MessagePublisher,
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
		ErrorChan:        make(chan error, serviceCfg.InputChanCapacity),
	}
}

// handleIncomingPahoMessage is the Paho MessageHandler.
// It pushes the payload to the MessagesChan for the workers.
func (s *IngestionService) handleIncomingPahoMessage(_ mqtt.Client, msg mqtt.Message) {
	if s.isShuttingDown.Load() {
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Shutdown in progress, Paho message dropped.")
		return
	}

	s.logger.Info().Interface("msg", msg).Bool("ts", msg.Duplicate()).Msg("received paho message")

	messagePayload := make([]byte, len(msg.Payload()))
	copy(messagePayload, msg.Payload())

	message := InMessage{
		Payload:   messagePayload,
		Topic:     msg.Topic(),
		Duplicate: msg.Duplicate(),
		MessageID: fmt.Sprintf("%d", msg.MessageID()),
		Timestamp: time.Now().UTC(),
	}

	select {
	case s.MessagesChan <- message:
		s.logger.Debug().Str("topic", msg.Topic()).Msg("Message pushed to MessagesChan")
	case <-s.cancelCtx.Done():
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Shutdown signaled, Paho message dropped")
		// MODIFICATION: Removed the 'default' case.
		// By removing it, this operation will now block if the channel is full.
		// This creates backpressure. The Paho client will stop reading messages from the broker
		// if the service cannot keep up, preventing message loss. The actual bottleneck
		// is addressed in the publisher, so this channel should rarely fill up with the new changes.
	}
}

// processSingleMessage handles parsing and enrichment of one raw message.
func (s *IngestionService) processSingleMessage(ctx context.Context, msg InMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Msg("Received message for processing")

	decodedPayload, err := ParseMQTTMessage(msg.Payload)
	if err != nil {
		snippetLen := len(msg.Payload)
		if snippetLen > 100 {
			snippetLen = 100
		}
		s.logger.Error().
			Int("worker_id", workerID).
			Err(err).
			Str("raw_message_snippet", string(msg.Payload[:snippetLen])).
			Msg("Failed to parse raw MQTT message")
		s.sendError(err)
		return
	}

	fullMessage := &GardenMonitorMessage{
		Topic:     msg.Topic,
		MessageID: msg.MessageID,
		Timestamp: msg.Timestamp,
		Payload:   decodedPayload,
	}
	// The publisher is now non-blocking, so this call will return almost immediately.
	// Error handling for the actual publish result is now handled within the publisher itself.
	if err := s.publisher.Publish(ctx, fullMessage); err != nil {
		// This error would now likely be a setup or marshalling error, not a publish-wait error.
		s.logger.Error().
			Int("worker_id", workerID).
			Str("topic", fullMessage.Topic).
			Str("eui", fullMessage.Payload.DE).
			Err(err).
			Msg("Failed to initiate publish for message")
		s.sendError(err)
	} else {
		s.logger.Debug().Int("worker_id", workerID).Str("device_eui", fullMessage.Payload.DE).Msg("Successfully processed and handed message to publisher")
	}
}

// sendError attempts to send an error to the ErrorChan.
func (s *IngestionService) sendError(err error) {
	if s.ErrorChan == nil {
		s.logger.Warn().Err(err).Msg("Attempted to send error to a nil ErrorChan.")
		return
	}
	select {
	case s.ErrorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("ErrorChan is full, dropping error")
	}
}

// Start begins the message processing workers and connects the MQTT client.
func (s *IngestionService) Start() error {
	s.logger.Info().
		Int("workers", s.config.NumProcessingWorkers).
		Int("channel_capacity", s.config.InputChanCapacity).
		Msg("Starting IngestionService...")

	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Info().Int("worker_id", workerID).Msg("Starting processing worker")
			for {
				select {
				case <-s.cancelCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down")
					return
				case message, ok := <-s.MessagesChan:
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("MessagesChan closed, worker stopping")
						return
					}
					s.processSingleMessage(s.cancelCtx, message, workerID)
				}
			}
		}(i)
	}

	if s.mqttClientConfig == nil {
		s.logger.Info().Msg("IngestionService started without MQTT client.")
		return nil
	}
	if err := s.initAndConnectMQTTClient(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to initialize or connect MQTT client during Start.")
		s.Stop() // Trigger a full shutdown if MQTT fails to connect
		return err
	}

	s.logger.Info().Msg("IngestionService started successfully.")
	return nil
}

// Stop gracefully shuts down the IngestionService.
func (s *IngestionService) Stop() {
	s.logger.Info().Msg("--- Starting Graceful Shutdown ---")

	// 1. Atomically signal that the service is shutting down.
	// This will cause handleIncomingPahoMessage to stop accepting new messages.
	s.isShuttingDown.Store(true)
	s.logger.Info().Msg("Step 1: Shutdown signaled. No new messages will be queued.")

	// 2. Unsubscribe and disconnect from the MQTT broker.
	if s.pahoClient != nil && s.pahoClient.IsConnected() {
		if s.mqttClientConfig != nil {
			topic := s.mqttClientConfig.Topic
			s.logger.Info().Str("topic", topic).Msg("Step 2a: Unsubscribing from MQTT topic.")
			if token := s.pahoClient.Unsubscribe(topic); token.WaitTimeout(2*time.Second) && token.Error() != nil {
				s.logger.Warn().Err(token.Error()).Msg("Failed to unsubscribe during shutdown.")
			}
		}
		s.pahoClient.Disconnect(500)
		s.logger.Info().Msg("Step 2b: Paho MQTT client disconnected.")
	}

	// 3. Close the MessagesChan. This signals the worker goroutines (ranging over the channel)
	// that no more messages will be sent. They will process any remaining messages and then exit.
	s.logger.Info().Msg("Step 3: Closing message channel. Workers will now drain the buffer.")
	s.closeMessagesChanOnce.Do(func() {
		close(s.MessagesChan)
	})

	// 4. Wait for all worker goroutines to finish processing the messages currently in the channel.
	s.logger.Info().Msg("Step 4: Waiting for worker goroutines to finish draining...")
	s.wg.Wait()
	s.logger.Info().Msg("All worker goroutines have completed.")

	// 5. Cancel the context. This is a good practice to clean up any other resources
	// that might be listening to this context (e.g., inside the publisher).
	s.logger.Info().Msg("Step 5: Cancelling context.")
	s.cancelFunc()

	// 6. Stop the publisher, ensuring its internal buffers are also drained.
	if s.publisher != nil {
		s.logger.Info().Msg("Step 6: Stopping publisher.")
		s.publisher.Stop()
		s.logger.Info().Msg("Publisher stopped.")
	}

	if s.ErrorChan != nil {
		s.closeErrorChanOnce.Do(func() {
			close(s.ErrorChan)
			s.logger.Info().Msg("ErrorChan closed.")
		})
	}
	s.logger.Info().Msg("IngestionService stopped.")
}

// onPahoConnect subscribes to the topic upon successful connection.
func (s *IngestionService) onPahoConnect(client mqtt.Client) {
	s.logger.Info().Str("broker", s.mqttClientConfig.BrokerURL).Msg("Paho client connected to MQTT broker")
	topic := s.mqttClientConfig.Topic
	qos := byte(1)

	s.logger.Info().Str("topic", topic).Msg("Subscribing to MQTT topic")
	if token := client.Subscribe(topic, qos, s.handleIncomingPahoMessage); token.Wait() && token.Error() != nil {
		s.logger.Error().Err(token.Error()).Str("topic", topic).Msg("Failed to subscribe to MQTT topic")
		s.sendError(fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error()))
	} else {
		s.logger.Info().Str("topic", topic).Msg("Successfully subscribed to MQTT topic")
	}
}

// onPahoConnectionLost logs connection loss. Paho handles reconnection.
func (s *IngestionService) onPahoConnectionLost(client mqtt.Client, err error) {
	s.logger.Error().Err(err).Msg("Paho client lost MQTT connection. Auto-reconnect will be attempted.")
}

// newTLSConfig creates a TLS configuration for the MQTT client.
func newTLSConfig(cfg *MQTTClientConfig, logger zerolog.Logger) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CACertFile != "" {
		caCert, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", cfg.CACertFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate from %s to pool", cfg.CACertFile)
		}
		tlsConfig.RootCAs = caCertPool
	}

	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

// initAndConnectMQTTClient initializes and connects the Paho MQTT client.
func (s *IngestionService) initAndConnectMQTTClient() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.mqttClientConfig.BrokerURL)

	uniqueSuffix := time.Now().UnixNano() % 1000000
	opts.SetClientID(fmt.Sprintf("%s%d", s.mqttClientConfig.ClientIDPrefix, uniqueSuffix))

	opts.SetUsername(s.mqttClientConfig.Username)
	opts.SetPassword(s.mqttClientConfig.Password)

	opts.SetKeepAlive(s.mqttClientConfig.KeepAlive)
	opts.SetConnectTimeout(s.mqttClientConfig.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(s.mqttClientConfig.ReconnectWaitMax)
	opts.SetOrderMatters(false) // MODIFICATION: Allow Paho to process messages out of order for higher throughput

	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		s.logger.Info().Str("broker", broker.String()).Msg("Attempting to connect to MQTT broker")
		return tlsCfg
	})

	if strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "tls://") ||
		strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "ssl://") {
		tlsConfig, err := newTLSConfig(s.mqttClientConfig, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
		s.logger.Info().Msg("TLS configured for MQTT client.")
	}

	opts.SetOnConnectHandler(s.onPahoConnect)
	opts.SetConnectionLostHandler(s.onPahoConnectionLost)

	s.pahoClient = mqtt.NewClient(opts)
	s.logger.Info().Str("client_id", opts.ClientID).Msg("Paho MQTT client created. Attempting to connect...")

	if token := s.pahoClient.Connect(); token.WaitTimeout(s.mqttClientConfig.ConnectTimeout) && token.Error() != nil {
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}

	return nil
}
