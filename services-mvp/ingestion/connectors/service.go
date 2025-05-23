package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	publisher                   MessagePublisher // Using the interface
	unidentifiedDevicePublisher MessagePublisher // new dead letter publisher

	config     IngestionServiceConfig
	fetcher    DeviceMetadataFetcher
	logger     zerolog.Logger
	wg         sync.WaitGroup
	cancelCtx  context.Context    // Used to signal shutdown to workers
	cancelFunc context.CancelFunc // Function to call to initiate shutdown

	// Public channels for interaction
	RawMessagesChan chan []byte // Input: Raw JSON messages
	ErrorChan       chan error  // Output: Errors encountered during processing (optional, for observability)
}

// NewIngestionService creates and initializes a new IngestionService.
// NewIngestionService creates a new IngestionService now with a mqttCfg
func NewIngestionService(
	fetcher DeviceMetadataFetcher,
	publisher, unidentifiedPublisher MessagePublisher, // Inject the publisher
	logger zerolog.Logger,
	serviceCfg IngestionServiceConfig,
	mqttCfg *MQTTClientConfig,
) *IngestionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestionService{
		config:                      serviceCfg,
		mqttClientConfig:            mqttCfg,
		fetcher:                     fetcher,
		publisher:                   publisher,
		unidentifiedDevicePublisher: unidentifiedPublisher, // Store new publisher
		logger:                      logger,
		cancelCtx:                   ctx,
		cancelFunc:                  cancel,
		RawMessagesChan:             make(chan []byte, serviceCfg.InputChanCapacity),
		ErrorChan:                   make(chan error, serviceCfg.InputChanCapacity),
	}
}

// handleIncomingPahoMessage is the Paho MessageHandler.
// It pushes the payload to the RawMessagesChan for the workers.
func (s *IngestionService) handleIncomingPahoMessage(client mqtt.Client, msg mqtt.Message) {
	s.logger.Debug().Str("topic", msg.Topic()).Int("payload_size", len(msg.Payload())).Msg("Paho client received message")

	// Create a copy of the payload to avoid issues if Paho reuses the buffer.
	payloadCopy := make([]byte, len(msg.Payload()))
	copy(payloadCopy, msg.Payload())

	select {
	case s.RawMessagesChan <- payloadCopy:
		s.logger.Debug().Str("topic", msg.Topic()).Msg("Message pushed to RawMessagesChan")
	case <-s.cancelCtx.Done():
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Shutdown signaled, Paho message dropped")
	default:
		// This case means RawMessagesChan is full. This indicates a bottleneck.
		s.logger.Error().Str("topic", msg.Topic()).Msg("RawMessagesChan is full. Paho message dropped. Consider increasing capacity or worker count.")
		// Optionally, send an error to s.ErrorChan or implement other backpressure handling.
	}
}

// processSingleMessage handles parsing and enrichment of one raw message.
func (s *IngestionService) processSingleMessage(ctx context.Context, rawMsg []byte, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Int("msg_size_bytes", len(rawMsg)).Msg("Received raw message for processing")

	mqttMsg, err := ParseMQTTMessage(rawMsg)
	if err != nil {
		s.logger.Error().
			Int("worker_id", workerID).
			Err(err).
			Str("raw_message_snippet", string(rawMsg[:min(len(rawMsg), 100)])). // Log a snippet
			Msg("Failed to parse raw MQTT message")
		s.sendError(err) // Optionally send parse errors to ErrorChan
		return
	}

	enrichedMsg, err := EnrichMQTTData(mqttMsg, s.fetcher, s.logger)
	if err != nil {
		// If we don't have Metadata - send to our dead message topic
		if errors.Is(err, ErrMetadataNotFound) {
			s.handleUnidentifiedMessage(ctx, mqttMsg, workerID)
		}
		// EnrichMQTTData already logs detailed errors.
		// We might just log that enrichment failed at this level or send to ErrorChan.
		s.logger.Warn().
			Int("worker_id", workerID).
			Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).
			Err(err).
			Msg("Failed to enrich MQTT message")
		s.sendError(err) // Optionally send enrichment errors to ErrorChan
		return
	}

	log.Debug().Msg("enriched message")
	// Publish the enriched message
	// Use a child context with a timeout for the publish operation if desired
	publishCtx, publishCancel := context.WithTimeout(ctx, 30*time.Second) // Example timeout
	defer publishCancel()

	if err := s.publisher.PublishEnriched(publishCtx, enrichedMsg); err != nil {
		s.logger.Error().
			Int("worker_id", workerID).
			Str("device_eui", enrichedMsg.DeviceEUI).
			Err(err).
			Msg("Failed to publish enriched message")
		s.sendError(err) // Send this critical error to the error channel
		// Decide on retry strategy or if message should be dead-lettered
	} else {
		s.logger.Debug().Int("worker_id", workerID).Str("device_eui", enrichedMsg.DeviceEUI).Msg("Successfully processed and published enriched message")
	}
}

func (s *IngestionService) handleUnidentifiedMessage(ctx context.Context, mqttMsg *MQTTMessage, workerID int) {

	unidentifiedMsg := UnidentifiedDeviceMessage{
		DeviceEUI:          mqttMsg.DeviceInfo.DeviceEUI,
		RawPayload:         mqttMsg.RawPayload,
		OriginalMQTTTime:   mqttMsg.MessageTimestamp,
		LoRaWANReceivedAt:  mqttMsg.LoRaWAN.ReceivedAt,
		IngestionTimestamp: time.Now().UTC(),
		ProcessingError:    ErrMetadataNotFound.Error(),
	}
	publishCtx, publishCancel := context.WithTimeout(ctx, 30*time.Second)
	defer publishCancel()
	// Use the new PublishUnidentified method
	if pubErr := s.unidentifiedDevicePublisher.PublishUnidentified(publishCtx, &unidentifiedMsg); pubErr != nil {
		s.logger.Error().Int("worker_id", workerID).Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Err(pubErr).Msg("Failed to publish to unidentified device topic")
		s.sendError(fmt.Errorf("failed to publish unidentified device msg for %s: %w", mqttMsg.DeviceInfo.DeviceEUI, pubErr))
	} else {
		s.logger.Info().Int("worker_id", workerID).Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Msg("Successfully published to unidentified device topic")
	}
	// Do not send original ErrMetadataNotFound to ErrorChan if successfully routed,
	// or decide if ErrorChan should still receive it for general monitoring.
	// For now, we consider successful routing to the unidentified topic as handling this specific error.
}

// sendError attempts to send an error to the ErrorChan if it's non-nil and there's capacity.
func (s *IngestionService) sendError(err error) {
	if s.ErrorChan == nil {
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
				case rawMsg, ok := <-s.RawMessagesChan:
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("RawMessagesChan closed, worker stopping")
						return
					}
					s.processSingleMessage(s.cancelCtx, rawMsg, workerID) // Pass context for publisher
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
		// Critical error: stop the already started workers and cleanup.
		s.cancelFunc() // Signal workers to stop
		// Workers will exit as RawMessagesChan is not yet closed by Paho and they see context done.
		// Or they might process a few if any messages were already in RawMessagesChan (unlikely here).

		// Wait for workers to stop before fully exiting Start with error
		// We don't close RawMessagesChan here as Paho is not connected to write to it.
		// And it will be closed in Stop() eventually.
		go func() { // Non-blocking wait and channel close for cleanup
			s.wg.Wait()
			if s.ErrorChan != nil {
				close(s.ErrorChan)
			}
			s.logger.Info().Msg("Worker goroutines stopped after MQTT connection failure.")
		}()
		return err // Propagate the error
	}

	s.logger.Info().Msg("IngestionService started successfully, including MQTT client connection attempt.")
	return nil
}

// processSingleMessage remains the same as in 'mqtt_message_struct_go'.
// sendError remains the same as in 'mqtt_message_struct_go'.

// Stop gracefully shuts down the IngestionService.
func (s *IngestionService) Stop() {
	s.logger.Info().Msg("Stopping IngestionService...")

	if s.pahoClient != nil && s.pahoClient.IsConnected() {
		s.logger.Info().Msg("Disconnecting Paho MQTT client...")
		s.pahoClient.Disconnect(250)
		s.logger.Info().Msg("Paho MQTT client disconnected.")
	}

	s.logger.Info().Msg("Signalling worker goroutines to stop...")
	s.cancelFunc()

	// It's generally safer not to close RawMessagesChan if an external component (Paho) is writing to it.
	// Paho's disconnection and the context cancellation should be the primary signals for workers.
	// If Paho is guaranteed to stop writing before/during this Stop(), then closing might be okay.
	// For now, relying on cancelCtx.
	// s.logger.Info().Msg("Closing RawMessagesChan...")
	// close(s.RawMessagesChan) // Be cautious with this if Paho might still write.

	s.logger.Info().Msg("Waiting for worker goroutines to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All worker goroutines completed.")

	if s.publisher != nil {
		s.publisher.Stop() // Stop the message publisher
	}
	if s.unidentifiedDevicePublisher != nil { // Stop the new publisher
		s.unidentifiedDevicePublisher.Stop()
	}

	if s.ErrorChan != nil {
		s.logger.Info().Msg("Closing ErrorChan...")
		close(s.ErrorChan)
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
		// Consider more robust error handling here, e.g., trying to reconnect or shutting down.
		s.sendError(fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error()))
	} else {
		s.logger.Info().Str("topic", topic).Msg("Successfully subscribed to MQTT topic")
	}
}

// onPahoConnectionLost is called when the Paho client loses its connection.
func (s *IngestionService) onPahoConnectionLost(client mqtt.Client, err error) {
	s.logger.Error().Err(err).Msg("Paho client lost MQTT connection")
	// Paho's auto-reconnect should handle this if enabled in options.
	// We log it for observability.
}

// newTLSConfig creates a TLS configuration for MQTT client.
// Reads CA cert, client cert, and client key from files if paths are provided.
func newTLSConfig(cfg *MQTTClientConfig, logger zerolog.Logger) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, // Use with caution!
	}

	// Load CA cert
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

	// Load client certificate and key if both are provided (for mTLS)
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
		// Log a warning if only one of client cert/key is provided
		logger.Warn().Msg("Client certificate or key file provided without its pair; mTLS will not be configured.")
	}

	return tlsConfig, nil
}

// initAndConnectMQTTClient initializes and connects the Paho MQTT client.
func (s *IngestionService) initAndConnectMQTTClient() error {
	opts := mqtt.NewClientOptions()
	s.logger.Debug().Str("broker", s.mqttClientConfig.BrokerURL).Msg("adding broker") //.Interface("config", s.mqttClientConfig)
	opts.AddBroker(s.mqttClientConfig.BrokerURL)
	// Generate a unique client ID
	// ClientID must be unique for each connection to the broker.
	// Appending a timestamp or random string can help ensure uniqueness.
	uniqueSuffix := time.Now().UnixNano() % 1000000 // Example suffix
	opts.SetClientID(fmt.Sprintf("%s-%d", s.mqttClientConfig.ClientIDPrefix, uniqueSuffix))

	opts.SetUsername(s.mqttClientConfig.Username)
	opts.SetPassword(s.mqttClientConfig.Password)

	opts.SetKeepAlive(s.mqttClientConfig.KeepAlive)
	opts.SetConnectTimeout(s.mqttClientConfig.ConnectTimeout)
	opts.SetAutoReconnect(true) // Enable auto-reconnect
	opts.SetMaxReconnectInterval(s.mqttClientConfig.ReconnectWaitMax)
	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		log.Info().Str("broker", broker.String()).Msg("Attempting to connect to MQTT broker")
		return tlsCfg // Return the original or modified TLS config
	})

	// Setup TLS
	if strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "tls://") ||
		strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "ssl://") || // "ssl" is often used alias for "tls"
		strings.Contains(s.mqttClientConfig.BrokerURL, ":8883") { // Common TLS port

		tlsConfig, err := newTLSConfig(s.mqttClientConfig, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
		log.Info().Msg("TLS configured for MQTT client.")
	} else {
		s.logger.Info().Msg("TLS not explicitly configured for MQTT client (broker URL does not start with tls:// or ssl://, or use port 8883).")
	}

	// Set handlers
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		// This service primarily subscribes, but a default handler is good practice.
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Received unexpected message on subscribed client (default handler)")
	})
	opts.SetOnConnectHandler(s.onPahoConnect)
	opts.SetConnectionLostHandler(s.onPahoConnectionLost)

	s.pahoClient = mqtt.NewClient(opts)
	s.logger.Info().Str("client_id", opts.ClientID).Msg("Paho MQTT client created. Attempting to connect...")

	log.Debug().Interface("servers", opts.Servers).Str("client_id", opts.ClientID).Msg("new client")
	if token := s.pahoClient.Connect(); token.WaitTimeout(s.mqttClientConfig.ConnectTimeout) && token.Error() != nil {
		log.Error().Err(token.Error()).Msg("Failed to connect Paho MQTT client")
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}

	// Connection might not be immediate, onPahoConnect handles subscription.
	// Check IsConnected for initial status, though OnConnect handler is more definitive for operational readiness.
	if !s.pahoClient.IsConnected() {
		s.logger.Warn().Msg("Paho client initiated connection, but IsConnected() is false. Waiting for OnConnect handler.")
		// This might happen if connect takes a moment. The OnConnect handler is the true signal of readiness.
	}

	return nil
}
