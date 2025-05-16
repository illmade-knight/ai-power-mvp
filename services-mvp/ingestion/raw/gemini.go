package ingestion

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore" // Google Cloud Firestore
	"cloud.google.com/go/pubsub"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// --- Data Structs ---
type LoRaWANData struct {
	RSSI       int       `json:"rssi,omitempty"`
	SNR        float64   `json:"snr,omitempty"`
	DataRate   string    `json:"data_rate,omitempty"`
	Frequency  float64   `json:"frequency,omitempty"`
	GatewayEUI string    `json:"gateway_eui,omitempty"`
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

type DeviceInfo struct {
	DeviceEUI string `json:"device_eui"`
}

type MQTTMessage struct {
	DeviceInfo       DeviceInfo  `json:"device_info"`
	LoRaWAN          LoRaWANData `json:"lorawan_data,omitempty"`
	RawPayload       string      `json:"raw_payload"`
	MessageTimestamp time.Time   `json:"message_timestamp"`
}

type EnrichedMessage struct {
	RawPayload         string    `json:"raw_payload"`
	DeviceEUI          string    `json:"device_eui"`
	OriginalMQTTTime   time.Time `json:"original_mqtt_time,omitempty"`
	LoRaWANReceivedAt  time.Time `json:"lorawan_received_at,omitempty"`
	ClientID           string    `json:"client_id"`
	LocationID         string    `json:"location_id"`
	DeviceCategory     string    `json:"device_category"`
	IngestionTimestamp time.Time `json:"ingestion_timestamp"`
}

type UnidentifiedDeviceMessage struct {
	DeviceEUI          string    `json:"device_eui"`
	RawPayload         string    `json:"raw_payload"`
	OriginalMQTTTime   time.Time `json:"original_mqtt_time,omitempty"`
	LoRaWANReceivedAt  time.Time `json:"lorawan_received_at,omitempty"`
	IngestionTimestamp time.Time `json:"ingestion_timestamp"`
	ProcessingError    string    `json:"processing_error"`
}

func ParseMQTTMessage(jsonData []byte) (*MQTTMessage, error) {
	var msg MQTTMessage
	err := json.Unmarshal(jsonData, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

var ErrMetadataNotFound = errors.New("device metadata not found")
var ErrInvalidMessage = errors.New("invalid MQTT message for processing")

// --- DeviceMetadataFetcher Abstraction & Implementations ---
type DeviceMetadataFetcher func(deviceEUI string) (clientID, locationID, category string, err error)

type FirestoreFetcherConfig struct {
	ProjectID       string
	CollectionName  string
	CredentialsFile string
}

func LoadFirestoreFetcherConfigFromEnv() (*FirestoreFetcherConfig, error) {
	cfg := &FirestoreFetcherConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		CollectionName:  os.Getenv("FIRESTORE_COLLECTION_DEVICES"),
		CredentialsFile: os.Getenv("GCP_FIRESTORE_CREDENTIALS_FILE"),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Firestore")
	}
	if cfg.CollectionName == "" {
		return nil, errors.New("FIRESTORE_COLLECTION_DEVICES environment variable not set")
	}
	return cfg, nil
}

type GoogleDeviceMetadataFetcher struct {
	client         *firestore.Client
	collectionName string
	logger         zerolog.Logger
}

func NewGoogleDeviceMetadataFetcher(ctx context.Context, cfg *FirestoreFetcherConfig, logger zerolog.Logger) (*GoogleDeviceMetadataFetcher, error) {
	var opts []option.ClientOption
	firestoreEmulatorHost := os.Getenv("FIRESTORE_EMULATOR_HOST")
	if cfg.CredentialsFile != "" && firestoreEmulatorHost == "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for Firestore")
	} else if firestoreEmulatorHost != "" {
		logger.Info().Str("emulator_host", firestoreEmulatorHost).Msg("Using Firestore emulator")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for Firestore")
	}
	client, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("firestore.NewClient: %w", err)
	}
	return &GoogleDeviceMetadataFetcher{client: client, collectionName: cfg.CollectionName, logger: logger}, nil
}

func (f *GoogleDeviceMetadataFetcher) Fetch(deviceEUI string) (string, string, string, error) {
	f.logger.Debug().Str("device_eui", deviceEUI).Msg("Fetching metadata from Firestore")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	docRef := f.client.Collection(f.collectionName).Doc(deviceEUI)
	docSnap, err := docRef.Get(ctx)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "not found") {
			f.logger.Warn().Str("device_eui", deviceEUI).Msg("Device metadata not found in Firestore")
			return "", "", "", ErrMetadataNotFound
		}
		return "", "", "", fmt.Errorf("firestore Get for %s: %w", deviceEUI, err)
	}
	var deviceData struct {
		ClientID       string `firestore:"clientID"`
		LocationID     string `firestore:"locationID"`
		DeviceCategory string `firestore:"deviceCategory"`
	}
	if err := docSnap.DataTo(&deviceData); err != nil {
		return "", "", "", fmt.Errorf("firestore DataTo for %s: %w", deviceEUI, err)
	}
	if deviceData.ClientID == "" || deviceData.LocationID == "" || deviceData.DeviceCategory == "" {
		return "", "", "", fmt.Errorf("incomplete metadata for %s: %+v", deviceEUI, deviceData)
	}
	return deviceData.ClientID, deviceData.LocationID, deviceData.DeviceCategory, nil
}

func (f *GoogleDeviceMetadataFetcher) Close() error {
	if f.client != nil {
		f.logger.Info().Msg("Closing Firestore client for GoogleDeviceMetadataFetcher.")
		return f.client.Close()
	}
	return nil
}

// --- Publisher Abstraction (Updated Interface) ---
type MessagePublisher interface {
	PublishEnriched(ctx context.Context, message *EnrichedMessage) error
	PublishUnidentified(ctx context.Context, message *UnidentifiedDeviceMessage) error
	Stop()
}

// --- Google Cloud Pub/Sub Publisher Implementation (Updated to match new interface) ---
type GooglePubSubPublisherConfig struct {
	ProjectID       string
	TopicID         string
	CredentialsFile string
}

func LoadGooglePubSubPublisherConfigFromEnv(topicEnvVar string) (*GooglePubSubPublisherConfig, error) {
	cfg := &GooglePubSubPublisherConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		TopicID:         os.Getenv(topicEnvVar),
		CredentialsFile: os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub")
	}
	if cfg.TopicID == "" {
		return nil, fmt.Errorf("%s environment variable not set for Pub/Sub", topicEnvVar)
	}
	return cfg, nil
}

type GooglePubSubPublisher struct {
	client *pubsub.Client
	topic  *pubsub.Topic
	logger zerolog.Logger
}

func NewGooglePubSubPublisher(ctx context.Context, cfg *GooglePubSubPublisherConfig, logger zerolog.Logger) (*GooglePubSubPublisher, error) {
	var opts []option.ClientOption
	pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	if pubsubEmulatorHost != "" {
		logger.Info().Str("emulator_host", pubsubEmulatorHost).Str("topic_id", cfg.TopicID).Msg("Using Pub/Sub emulator explicitly with endpoint and no auth.")
		opts = append(opts, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	} else if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Str("topic_id", cfg.TopicID).Msg("Using specified credentials file for Pub/Sub")
	} else {
		logger.Info().Str("topic_id", cfg.TopicID).Msg("Using Application Default Credentials (ADC) for Pub/Sub")
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient for topic %s: %w", cfg.TopicID, err)
	}
	topic := client.Topic(cfg.TopicID)
	logger.Info().Str("project_id", cfg.ProjectID).Str("topic_id", cfg.TopicID).Msg("GooglePubSubPublisher initialized")
	return &GooglePubSubPublisher{client: client, topic: topic, logger: logger}, nil
}

// PublishEnriched publishes an EnrichedMessage.
func (p *GooglePubSubPublisher) PublishEnriched(ctx context.Context, message *EnrichedMessage) error {
	if message == nil {
		return errors.New("cannot publish nil EnrichedMessage")
	}
	data, err := json.Marshal(message)
	if err != nil {
		p.logger.Error().Err(err).Str("device_eui", message.DeviceEUI).Msg("Failed to marshal EnrichedMessage for Pub/Sub")
		return fmt.Errorf("json.Marshal(EnrichedMessage): %w", err)
	}
	attributes := map[string]string{
		"message_type": "enriched", // Add a type attribute
		"device_eui":   message.DeviceEUI,
		"client_id":    message.ClientID,
		"category":     message.DeviceCategory,
	}
	return p.publishData(ctx, data, attributes, message.DeviceEUI)
}

// PublishUnidentified publishes an UnidentifiedDeviceMessage.
func (p *GooglePubSubPublisher) PublishUnidentified(ctx context.Context, message *UnidentifiedDeviceMessage) error {
	if message == nil {
		return errors.New("cannot publish nil UnidentifiedDeviceMessage")
	}
	data, err := json.Marshal(message)
	if err != nil {
		p.logger.Error().Err(err).Str("device_eui", message.DeviceEUI).Msg("Failed to marshal UnidentifiedDeviceMessage for Pub/Sub")
		return fmt.Errorf("json.Marshal(UnidentifiedDeviceMessage): %w", err)
	}
	attributes := map[string]string{
		"message_type": "unidentified", // Add a type attribute
		"device_eui":   message.DeviceEUI,
		"error_reason": message.ProcessingError,
	}
	return p.publishData(ctx, data, attributes, message.DeviceEUI)
}

// publishData is a helper to reduce duplication.
func (p *GooglePubSubPublisher) publishData(ctx context.Context, data []byte, attributes map[string]string, loggingIdentifier string) error {
	result := p.topic.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})
	msgID, err := result.Get(ctx)
	if err != nil {
		p.logger.Error().Err(err).Str("identifier", loggingIdentifier).Interface("attributes", attributes).Msg("Failed to publish message to Pub/Sub")
		return fmt.Errorf("pubsub publish Get for %s: %w", loggingIdentifier, err)
	}
	p.logger.Debug().Str("message_id", msgID).Str("identifier", loggingIdentifier).Interface("attributes", attributes).Msg("Message published successfully to Pub/Sub")
	return nil
}

func (p *GooglePubSubPublisher) Stop() {
	p.logger.Info().Str("topic_id", p.topic.ID()).Msg("Stopping GooglePubSubPublisher...")
	if p.topic != nil {
		p.topic.Stop()
	}
	if p.client != nil {
		if err := p.client.Close(); err != nil {
			p.logger.Error().Err(err).Msg("Error closing Pub/Sub client")
		}
	}
}

// --- MQTT Client Configuration ---
type MQTTClientConfig struct { /* ... same as before ... */
	BrokerURL          string
	Topic              string
	ClientIDPrefix     string
	Username           string
	Password           string
	KeepAlive          time.Duration
	ConnectTimeout     time.Duration
	ReconnectWaitMin   time.Duration
	ReconnectWaitMax   time.Duration
	CACertFile         string
	ClientCertFile     string
	ClientKeyFile      string
	InsecureSkipVerify bool
}

func LoadMQTTClientConfigFromEnv() (*MQTTClientConfig, error) { /* ... same as before ... */
	cfg := &MQTTClientConfig{
		BrokerURL:        os.Getenv("MQTT_BROKER_URL"),
		Topic:            os.Getenv("MQTT_TOPIC"),
		ClientIDPrefix:   os.Getenv("MQTT_CLIENT_ID_PREFIX"),
		Username:         os.Getenv("MQTT_USERNAME"),
		Password:         os.Getenv("MQTT_PASSWORD"),
		KeepAlive:        60 * time.Second,
		ConnectTimeout:   10 * time.Second,
		ReconnectWaitMin: 1 * time.Second,
		ReconnectWaitMax: 120 * time.Second,
		CACertFile:       os.Getenv("MQTT_CA_CERT_FILE"),
		ClientCertFile:   os.Getenv("MQTT_CLIENT_CERT_FILE"),
		ClientKeyFile:    os.Getenv("MQTT_CLIENT_KEY_FILE"),
	}
	if skipVerify := os.Getenv("MQTT_INSECURE_SKIP_VERIFY"); skipVerify == "true" {
		cfg.InsecureSkipVerify = true
	}
	if cfg.BrokerURL == "" {
		return nil, errors.New("MQTT_BROKER_URL missing")
	}
	if cfg.Topic == "" {
		return nil, errors.New("MQTT_TOPIC missing")
	}
	if cfg.ClientIDPrefix == "" {
		cfg.ClientIDPrefix = "ingestion-svc-"
	}
	if ka := os.Getenv("MQTT_KEEP_ALIVE_SECONDS"); ka != "" {
		if s, err := time.ParseDuration(ka + "s"); err == nil {
			cfg.KeepAlive = s
		}
	}
	if ct := os.Getenv("MQTT_CONNECT_TIMEOUT_SECONDS"); ct != "" {
		if s, err := time.ParseDuration(ct + "s"); err == nil {
			cfg.ConnectTimeout = s
		}
	}
	return cfg, nil
}

// --- Ingestion Service ---
type IngestionServiceConfig struct { /* ... same as before ... */
	InputChanCapacity    int
	OutputChanCapacity   int
	NumProcessingWorkers int
}

func DefaultIngestionServiceConfig() IngestionServiceConfig { /* ... same as before ... */
	return IngestionServiceConfig{
		InputChanCapacity:    100,
		OutputChanCapacity:   100,
		NumProcessingWorkers: 5,
	}
}

type IngestionService struct {
	config                      IngestionServiceConfig
	mqttClientConfig            *MQTTClientConfig
	fetcher                     DeviceMetadataFetcher
	logger                      zerolog.Logger
	enrichedMessagePublisher    MessagePublisher
	unidentifiedDevicePublisher MessagePublisher
	wg                          sync.WaitGroup
	cancelCtx                   context.Context
	cancelFunc                  context.CancelFunc
	pahoClient                  mqtt.Client
	RawMessagesChan             chan []byte
	ErrorChan                   chan error
}

func NewIngestionService(
	fetcher DeviceMetadataFetcher,
	enrichedPublisher MessagePublisher,
	unidentifiedPublisher MessagePublisher,
	logger zerolog.Logger,
	serviceCfg IngestionServiceConfig,
	mqttCfg *MQTTClientConfig,
) *IngestionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestionService{
		config:                      serviceCfg,
		mqttClientConfig:            mqttCfg,
		fetcher:                     fetcher,
		enrichedMessagePublisher:    enrichedPublisher,
		unidentifiedDevicePublisher: unidentifiedPublisher,
		logger:                      logger,
		cancelCtx:                   ctx,
		cancelFunc:                  cancel,
		RawMessagesChan:             make(chan []byte, serviceCfg.InputChanCapacity),
		ErrorChan:                   make(chan error, serviceCfg.InputChanCapacity),
	}
}

func (s *IngestionService) handleIncomingPahoMessage(client mqtt.Client, msg mqtt.Message) { /* ... same as before ... */
	s.logger.Debug().Str("topic", msg.Topic()).Int("payload_size", len(msg.Payload())).Msg("Paho client received message")
	payloadCopy := make([]byte, len(msg.Payload()))
	copy(payloadCopy, msg.Payload())
	select {
	case s.RawMessagesChan <- payloadCopy:
		s.logger.Debug().Str("topic", msg.Topic()).Msg("Message pushed to RawMessagesChan")
	case <-s.cancelCtx.Done():
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Shutdown signaled, Paho message dropped")
	default:
		s.logger.Error().Str("topic", msg.Topic()).Msg("RawMessagesChan is full. Paho message dropped.")
	}
}
func (s *IngestionService) onPahoConnect(client mqtt.Client) { /* ... same as before ... */
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
func (s *IngestionService) onPahoConnectionLost(client mqtt.Client, err error) { /* ... same as before ... */
	s.logger.Error().Err(err).Msg("Paho client lost MQTT connection")
}
func newTLSConfig(cfg *MQTTClientConfig, logger zerolog.Logger) (*tls.Config, error) { /* ... same as before ... */
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.CACertFile != "" {
		caCert, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", cfg.CACertFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate from %s", cfg.CACertFile)
		}
		tlsConfig.RootCAs = caCertPool
		logger.Info().Str("ca_cert_file", cfg.CACertFile).Msg("CA certificate loaded")
	}
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		logger.Info().Str("client_cert_file", cfg.ClientCertFile).Msg("Client certificate and key loaded")
	} else if cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
		logger.Warn().Msg("Client certificate or key file provided without its pair; mTLS not configured.")
	}
	return tlsConfig, nil
}
func (s *IngestionService) initAndConnectMQTTClient() error { /* ... same as before ... */
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.mqttClientConfig.BrokerURL)
	uniqueSuffix := time.Now().UnixNano() % 1000000
	opts.SetClientID(fmt.Sprintf("%s-%d", s.mqttClientConfig.ClientIDPrefix, uniqueSuffix))
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
	if strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "tls://") ||
		strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "ssl://") ||
		strings.Contains(s.mqttClientConfig.BrokerURL, ":8883") {
		tlsConfig, err := newTLSConfig(s.mqttClientConfig, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
		s.logger.Info().Msg("TLS configured for MQTT client.")
	} else {
		s.logger.Info().Msg("TLS not explicitly configured for MQTT client.")
	}
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Received unexpected message on subscribed client")
	})
	opts.SetOnConnectHandler(s.onPahoConnect)
	opts.SetConnectionLostHandler(s.onPahoConnectionLost)
	s.pahoClient = mqtt.NewClient(opts)
	s.logger.Info().Str("client_id", opts.ClientID).Msg("Paho MQTT client created. Attempting to connect...")
	if token := s.pahoClient.Connect(); token.WaitTimeout(s.mqttClientConfig.ConnectTimeout) && token.Error() != nil {
		s.logger.Error().Err(token.Error()).Msg("Failed to connect Paho MQTT client")
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}
	return nil
}

func (s *IngestionService) Start() error {
	s.logger.Info().Int("workers", s.config.NumProcessingWorkers).Msg("Starting IngestionService...")
	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Debug().Int("worker_id", workerID).Msg("Starting processing worker")
			for {
				select {
				case <-s.cancelCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down")
					return
				case rawMsg, ok := <-s.RawMessagesChan:
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("RawMessagesChan closed, worker stopping")
						return
					}
					s.processSingleMessage(s.cancelCtx, rawMsg, workerID)
				}
			}
		}(i)
	}
	if err := s.initAndConnectMQTTClient(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to initialize or connect MQTT client during Start.")
		s.cancelFunc()
		go func() {
			s.wg.Wait()
			if s.ErrorChan != nil {
				close(s.ErrorChan)
			}
			s.logger.Info().Msg("Worker goroutines stopped after MQTT connection failure.")
		}()
		return err
	}
	s.logger.Info().Msg("IngestionService started successfully.")
	return nil
}

func (s *IngestionService) processSingleMessage(ctx context.Context, rawMsg []byte, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Int("msg_size_bytes", len(rawMsg)).Msg("Received raw message for processing")
	mqttMsg, err := ParseMQTTMessage(rawMsg)
	if err != nil {
		s.logger.Error().Int("worker_id", workerID).Err(err).Str("raw_message_snippet", string(rawMsg[:min(len(rawMsg), 100)])).Msg("Failed to parse raw MQTT message")
		s.sendError(err)
		return
	}

	enrichedMsg, err := EnrichMQTTData(mqttMsg, s.fetcher, s.logger)
	if err != nil {
		if errors.Is(err, ErrMetadataNotFound) {
			s.logger.Warn().
				Int("worker_id", workerID).
				Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).
				Err(err).
				Msg("Metadata not found, routing to unidentified device topic")

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
			return
		}

		// Handle other enrichment errors
		s.logger.Warn().Int("worker_id", workerID).Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Err(err).Msg("Failed to enrich MQTT message (other error)")
		s.sendError(err)
		return
	}

	// Publish successfully enriched message
	publishCtx, publishCancel := context.WithTimeout(ctx, 30*time.Second)
	defer publishCancel()
	// Use the new PublishEnriched method
	if pubErr := s.enrichedMessagePublisher.PublishEnriched(publishCtx, enrichedMsg); pubErr != nil {
		s.logger.Error().Int("worker_id", workerID).Str("device_eui", enrichedMsg.DeviceEUI).Err(pubErr).Msg("Failed to publish enriched message")
		s.sendError(fmt.Errorf("failed to publish enriched msg for %s: %w", enrichedMsg.DeviceEUI, pubErr))
	} else {
		s.logger.Debug().Int("worker_id", workerID).Str("device_eui", enrichedMsg.DeviceEUI).Msg("Successfully processed and published enriched message")
	}
}

func (s *IngestionService) sendError(err error) { /* ... same as before ... */
	if s.ErrorChan == nil {
		return
	}
	select {
	case s.ErrorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("ErrorChan is full or nil, dropping error")
	}
}

func (s *IngestionService) Stop() {
	s.logger.Info().Msg("Stopping IngestionService...")
	if s.pahoClient != nil && s.pahoClient.IsConnected() {
		s.pahoClient.Disconnect(250)
	}
	s.cancelFunc()
	s.wg.Wait()

	if s.enrichedMessagePublisher != nil {
		s.enrichedMessagePublisher.Stop()
	}
	if s.unidentifiedDevicePublisher != nil {
		s.unidentifiedDevicePublisher.Stop()
	}

	if s.ErrorChan != nil {
		close(s.ErrorChan)
	}
	s.logger.Info().Msg("IngestionService stopped.")
}

func min(a, b int) int { /* ... same as before ... */
	if a < b {
		return a
	}
	return b
}

// EnrichMQTTData processes an MQTTMessage, validates it, fetches metadata,
// and creates an EnrichedMessage.
func EnrichMQTTData(
	mqttMsg *MQTTMessage,
	fetcher DeviceMetadataFetcher,
	logger zerolog.Logger,
) (*EnrichedMessage, error) {
	if mqttMsg == nil {
		logger.Error().Msg("Input MQTTMessage is nil for enrichment")
		return nil, ErrInvalidMessage
	}
	if mqttMsg.DeviceInfo.DeviceEUI == "" {
		logger.Error().Msg("MQTTMessage is missing DeviceEUI for enrichment")
		return nil, ErrInvalidMessage
	}
	// Potentially add more validation for mqttMsg.RawPayload if it's critical for enrichment itself

	clientID, locationID, category, err := fetcher(mqttMsg.DeviceInfo.DeviceEUI)
	if err != nil {
		logger.Error().
			Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).
			Err(err).
			Msg("Failed to fetch device metadata during enrichment")

		return nil, err
	}

	enrichedMsg := &EnrichedMessage{
		RawPayload:         mqttMsg.RawPayload,
		DeviceEUI:          mqttMsg.DeviceInfo.DeviceEUI,
		OriginalMQTTTime:   mqttMsg.MessageTimestamp,
		LoRaWANReceivedAt:  mqttMsg.LoRaWAN.ReceivedAt,
		ClientID:           clientID,
		LocationID:         locationID,
		DeviceCategory:     category,
		IngestionTimestamp: time.Now().UTC(),
	}

	logger.Debug().
		Str("device_eui", enrichedMsg.DeviceEUI).
		Msg("Successfully enriched MQTT message")
	return enrichedMsg, nil
}
