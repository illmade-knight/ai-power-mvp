package icestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// ConsumedMessage represents a message received from a message broker.
type ConsumedMessage struct {
	ID          string    // Message ID from the broker
	Payload     []byte    // The raw message payload
	PublishTime time.Time // Timestamp when the message was published to the broker
	Ack         func()    // Function to call to acknowledge the message (Renamed from AckFunc)
	Nack        func()    // Function to call to negative-acknowledge the message (Renamed from NackFunc)
}

// MessageConsumer defines an interface for consuming messages from a broker.
type MessageConsumer interface {
	// Messages returns a channel from which ConsumedMessage can be read.
	// The channel will be closed when the consumer is stopped or encounters a fatal error.
	Messages() <-chan ConsumedMessage
	// Start begins consuming messages. Non-blocking.
	Start(ctx context.Context) error
	// Stop gracefully stops the consumer.
	Stop() error
	// Done returns a channel that's closed when the consumer has fully stopped.
	Done() <-chan struct{}
}

// --- Google Cloud Pub/Sub Consumer Implementation ---

// GooglePubSubConsumerConfig holds configuration for a Pub/Sub consumer.
type GooglePubSubConsumerConfig struct {
	ProjectID       string
	SubscriptionID  string
	CredentialsFile string // Optional
	// MaxOutstandingMessages: Controls how many messages can be processed concurrently by the Pub/Sub client library.
	MaxOutstandingMessages int
	// NumGoroutines: Number of goroutines the Pub/Sub client library uses to process messages.
	NumGoroutines int
}

// LoadGooglePubSubConsumerConfigFromEnv loads consumer configuration from environment variables.
// `subscriptionEnvVar` is the name of the environment variable holding the subscription ID.
func LoadGooglePubSubConsumerConfigFromEnv(subscriptionEnvVar string) (*GooglePubSubConsumerConfig, error) {
	cfg := &GooglePubSubConsumerConfig{
		ProjectID:              os.Getenv("GCP_PROJECT_ID"),
		SubscriptionID:         os.Getenv(subscriptionEnvVar),
		CredentialsFile:        os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"), // Optional, same as publisher
		MaxOutstandingMessages: 100,                                      // Default
		NumGoroutines:          5,                                        // Default
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub consumer")
	}
	if cfg.SubscriptionID == "" {
		return nil, fmt.Errorf("%s environment variable not set for Pub/Sub consumer", subscriptionEnvVar)
	}
	// TODO: Add parsing for MaxOutstandingMessages and NumGoroutines from env if needed
	return cfg, nil
}

// GooglePubSubConsumer implements MessageConsumer for Google Cloud Pub/Sub.
type GooglePubSubConsumer struct {
	client             *pubsub.Client
	subscription       *pubsub.Subscription
	config             *GooglePubSubConsumerConfig
	logger             zerolog.Logger
	outputChan         chan ConsumedMessage
	stopOnce           sync.Once
	cancelSubscription context.CancelFunc // To stop the Receive call
	wg                 sync.WaitGroup     // To wait for Receive goroutine to finish
	doneChan           chan struct{}      // To signal when fully stopped
}

// NewGooglePubSubConsumer creates a new consumer for Google Cloud Pub/Sub.
func NewGooglePubSubConsumer(ctx context.Context, cfg *GooglePubSubConsumerConfig, logger zerolog.Logger) (*GooglePubSubConsumer, error) {
	var opts []option.ClientOption
	pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")

	if pubsubEmulatorHost != "" {
		logger.Info().Str("emulator_host", pubsubEmulatorHost).Str("subscription_id", cfg.SubscriptionID).Msg("Using Pub/Sub emulator explicitly with endpoint and no auth for consumer.")
		opts = append(opts, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	} else if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Str("subscription_id", cfg.SubscriptionID).Msg("Using specified credentials file for Pub/Sub consumer")
	} else {
		logger.Info().Str("subscription_id", cfg.SubscriptionID).Msg("Using Application Default Credentials (ADC) for Pub/Sub consumer")
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient for subscription %s: %w", cfg.SubscriptionID, err)
	}

	sub := client.Subscription(cfg.SubscriptionID)
	// Configure ReceiveSettings
	sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = cfg.NumGoroutines

	// Check if subscription exists (optional, but good for emulators/setup)
	if pubsubEmulatorHost != "" { // Only check if using emulator and if desired
		exists, err := sub.Exists(ctx)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("subscription.Exists check for %s: %w", cfg.SubscriptionID, err)
		}
		if !exists {
			client.Close()
			return nil, fmt.Errorf("Pub/Sub subscription %s does not exist in project %s (expected to be created by test setup or deployment)", cfg.SubscriptionID, cfg.ProjectID)
		}
	}

	return &GooglePubSubConsumer{
		client:       client,
		subscription: sub,
		config:       cfg,
		logger:       logger.With().Str("component", "GooglePubSubConsumer").Str("subscription_id", cfg.SubscriptionID).Logger(),
		outputChan:   make(chan ConsumedMessage, cfg.MaxOutstandingMessages), // Buffer matches outstanding messages
		doneChan:     make(chan struct{}),
	}, nil
}

// Messages returns the output channel.
func (c *GooglePubSubConsumer) Messages() <-chan ConsumedMessage {
	return c.outputChan
}

// Start begins consuming messages from the Pub/Sub subscription.
func (c *GooglePubSubConsumer) Start(ctx context.Context) error {
	c.logger.Info().Msg("Starting Pub/Sub message consumption...")

	receiveCtx, cancel := context.WithCancel(ctx)
	c.cancelSubscription = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.outputChan)
		defer c.logger.Info().Msg("Pub/Sub Receive goroutine stopped.")

		c.logger.Info().Msg("Pub/Sub Receive goroutine started.")
		err := c.subscription.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			c.logger.Debug().Str("msg_id", msg.ID).Msg("Received Pub/Sub message")
			payloadCopy := make([]byte, len(msg.Data))
			copy(payloadCopy, msg.Data)

			consumedMsg := ConsumedMessage{
				ID:          msg.ID,
				Payload:     payloadCopy,
				PublishTime: msg.PublishTime,
				Ack:         msg.Ack,  // Renamed
				Nack:        msg.Nack, // Renamed
			}

			select {
			case c.outputChan <- consumedMsg:
				// Message successfully sent to processing channel
			case <-receiveCtx.Done():
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Consumer stopping, Nacking message.")
				msg.Nack()
			case <-ctx.Done():
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Outer context done, Nacking message.")
				msg.Nack()
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error().Err(err).Msg("Pub/Sub Receive call exited with error")
		}
		close(c.doneChan)
	}()
	return nil
}

// Stop gracefully stops the Pub/Sub consumer.
func (c *GooglePubSubConsumer) Stop() error {
	c.stopOnce.Do(func() {
		c.logger.Info().Msg("Stopping Pub/Sub consumer...")
		if c.cancelSubscription != nil {
			c.cancelSubscription()
		}

		select {
		case <-c.Done():
			c.logger.Info().Msg("Pub/Sub Receive goroutine confirmed stopped.")
		case <-time.After(30 * time.Second):
			c.logger.Error().Msg("Timeout waiting for Pub/Sub Receive goroutine to stop.")
		}

		if c.client != nil {
			if err := c.client.Close(); err != nil {
				c.logger.Error().Err(err).Msg("Error closing Pub/Sub client")
			} else {
				c.logger.Info().Msg("Pub/Sub client closed.")
			}
		}
	})
	return nil
}

// Done returns a channel that is closed when the consumer has stopped.
func (c *GooglePubSubConsumer) Done() <-chan struct{} {
	return c.doneChan
}

// --- Archival Service ---

// ArchivalServiceConfig holds configuration for the ArchivalService.
type ArchivalServiceConfig struct {
	EnrichedMessagesSubscriptionEnvVar     string
	UnidentifiedMessagesSubscriptionEnvVar string
	NumArchivalWorkers                     int
}

// LoadArchivalServiceConfigFromEnv loads service configuration.
func LoadArchivalServiceConfigFromEnv() (*ArchivalServiceConfig, error) {
	cfg := &ArchivalServiceConfig{
		EnrichedMessagesSubscriptionEnvVar:     "PUBSUB_SUBSCRIPTION_ID_ENRICHED",
		UnidentifiedMessagesSubscriptionEnvVar: "PUBSUB_SUBSCRIPTION_ID_UNIDENTIFIED",
		NumArchivalWorkers:                     5,
	}
	if subEnriched := os.Getenv("ARCHIVAL_ENRICHED_SUB_ENV_VAR"); subEnriched != "" {
		cfg.EnrichedMessagesSubscriptionEnvVar = subEnriched
	}
	if subUnidentified := os.Getenv("ARCHIVAL_UNIDENTIFIED_SUB_ENV_VAR"); subUnidentified != "" {
		cfg.UnidentifiedMessagesSubscriptionEnvVar = subUnidentified
	}
	return cfg, nil
}

// ArchivalService consumes messages and archives them.
type ArchivalService struct {
	config               *ArchivalServiceConfig
	enrichedConsumer     MessageConsumer
	unidentifiedConsumer MessageConsumer
	archiver             RawDataArchiver
	logger               zerolog.Logger
	wg                   sync.WaitGroup
	shutdownCtx          context.Context
	shutdownFunc         context.CancelFunc
}

// NewArchivalService creates a new ArchivalService.
func NewArchivalService(
	ctx context.Context,
	config *ArchivalServiceConfig,
	archiver RawDataArchiver,
	logger zerolog.Logger,
) (*ArchivalService, error) {

	enrichedSubCfg, err := LoadGooglePubSubConsumerConfigFromEnv(config.EnrichedMessagesSubscriptionEnvVar)
	if err != nil {
		return nil, fmt.Errorf("failed to load enriched consumer config: %w", err)
	}
	enrichedConsumer, err := NewGooglePubSubConsumer(ctx, enrichedSubCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create enriched message consumer: %w", err)
	}

	unidentifiedSubCfg, err := LoadGooglePubSubConsumerConfigFromEnv(config.UnidentifiedMessagesSubscriptionEnvVar)
	if err != nil {
		return nil, fmt.Errorf("failed to load unidentified consumer config: %w", err)
	}
	unidentifiedConsumer, err := NewGooglePubSubConsumer(ctx, unidentifiedSubCfg, logger)
	if err != nil {
		enrichedConsumer.Stop()
		return nil, fmt.Errorf("failed to create unidentified message consumer: %w", err)
	}

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	return &ArchivalService{
		config:               config,
		enrichedConsumer:     enrichedConsumer,
		unidentifiedConsumer: unidentifiedConsumer,
		archiver:             archiver,
		logger:               logger.With().Str("service", "ArchivalService").Logger(),
		shutdownCtx:          shutdownCtx,
		shutdownFunc:         shutdownFunc,
	}, nil
}

// Start begins consuming messages from both subscriptions and processing them.
func (s *ArchivalService) Start() error {
	s.logger.Info().Msg("Starting ArchivalService...")

	if err := s.enrichedConsumer.Start(s.shutdownCtx); err != nil {
		return fmt.Errorf("failed to start enriched message consumer: %w", err)
	}
	s.logger.Info().Msg("Enriched message consumer started.")

	if err := s.unidentifiedConsumer.Start(s.shutdownCtx); err != nil {
		s.enrichedConsumer.Stop()
		return fmt.Errorf("failed to start unidentified message consumer: %w", err)
	}
	s.logger.Info().Msg("Unidentified message consumer started.")

	for i := 0; i < s.config.NumArchivalWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Debug().Int("worker_id", workerID).Msg("Archival worker started.")
			for {
				select {
				case <-s.shutdownCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Archival worker shutting down.")
					return
				case msg, ok := <-s.enrichedConsumer.Messages():
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("Enriched consumer channel closed. Worker will rely on other channel or shutdown signal.")
						continue
					}
					s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Str("source", "enriched").Msg("Processing message")
					s.archiveConsumedMessage(msg)
				case msg, ok := <-s.unidentifiedConsumer.Messages():
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("Unidentified consumer channel closed. Worker will rely on other channel or shutdown signal.")
						continue
					}
					s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Str("source", "unidentified").Msg("Processing message")
					s.archiveConsumedMessage(msg)
				}
			}
		}(i)
	}
	s.logger.Info().Msg("ArchivalService started with workers.")
	return nil
}

func (s *ArchivalService) archiveConsumedMessage(msg ConsumedMessage) {
	err := s.archiver.Archive(context.Background(), msg.Payload, msg.PublishTime)
	if err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to archive message, Nacking.")
		msg.Nack() // Use Renamed field
	} else {
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Message archived successfully, Acking.")
		msg.Ack() // Use Renamed field
	}
}

// Stop gracefully shuts down the ArchivalService.
func (s *ArchivalService) Stop() {
	s.logger.Info().Msg("Stopping ArchivalService...")
	s.shutdownFunc()

	s.logger.Info().Msg("Stopping enriched message consumer...")
	if err := s.enrichedConsumer.Stop(); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping enriched consumer")
	}
	<-s.enrichedConsumer.Done()
	s.logger.Info().Msg("Enriched message consumer stopped.")

	s.logger.Info().Msg("Stopping unidentified message consumer...")
	if err := s.unidentifiedConsumer.Stop(); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping unidentified consumer")
	}
	<-s.unidentifiedConsumer.Done()
	s.logger.Info().Msg("Unidentified message consumer stopped.")

	s.logger.Info().Msg("Waiting for archival workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All archival workers completed.")

	if s.archiver != nil {
		s.logger.Info().Msg("Stopping GCS archiver...")
		if err := s.archiver.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Error stopping GCS archiver")
		} else {
			s.logger.Info().Msg("GCS archiver stopped.")
		}
	}
	s.logger.Info().Msg("ArchivalService stopped.")
}
