package consumers

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"os"
	"sync"
	"time"
)

// --- Google Cloud Pub/Sub Consumer Implementation (Ideally in a shared package) ---
// This is a simplified version for this service.
// It assumes the topic provides messages that can be unmarshalled into ConsumedUpstreamMessage.

type GooglePubSubConsumerConfig struct {
	ProjectID              string
	SubscriptionID         string
	CredentialsFile        string // Optional
	MaxOutstandingMessages int
	NumGoroutines          int
}

// LoadGooglePubSubConsumerConfigFromEnv loads consumer configuration from environment variables.
// Renamed from LoadConsumerConfigFromEnv
func LoadGooglePubSubConsumerConfigFromEnv() (*GooglePubSubConsumerConfig, error) {
	subID := os.Getenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT")

	cfg := &GooglePubSubConsumerConfig{
		ProjectID:              os.Getenv("GCP_PROJECT_ID"),
		SubscriptionID:         subID,
		CredentialsFile:        os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"),
		MaxOutstandingMessages: 100,
		NumGoroutines:          5,
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub consumer")
	}
	return cfg, nil
}

type GooglePubSubConsumer struct {
	client             *pubsub.Client
	subscription       *pubsub.Subscription
	logger             zerolog.Logger
	outputChan         chan types.ConsumedMessage
	stopOnce           sync.Once
	cancelSubscription context.CancelFunc
	wg                 sync.WaitGroup
	doneChan           chan struct{}
}

func NewGooglePubSubConsumer(ctx context.Context, cfg *GooglePubSubConsumerConfig, logger zerolog.Logger) (*GooglePubSubConsumer, error) {
	var opts []option.ClientOption
	pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")
	if pubsubEmulatorHost != "" {
		logger.Info().Str("emulator_host", pubsubEmulatorHost).Str("subscription_id", cfg.SubscriptionID).Msg("Using Pub/Sub emulator for consumer.")
		opts = append(opts, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	} else if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient for subscription %s: %w", cfg.SubscriptionID, err)
	}
	sub := client.Subscription(cfg.SubscriptionID)

	logger.Info().Str("subscription_id", cfg.SubscriptionID).Msg("Listening for messages")

	sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	sub.ReceiveSettings.NumGoroutines = cfg.NumGoroutines

	if pubsubEmulatorHost != "" {
		exists, err := sub.Exists(ctx)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("subscription.Exists check for %s: %w", cfg.SubscriptionID, err)
		}
		if !exists {
			client.Close()
			return nil, fmt.Errorf("Pub/Sub subscription %s does not exist in project %s", cfg.SubscriptionID, cfg.ProjectID)
		}
	}

	return &GooglePubSubConsumer{
		client:       client,
		subscription: sub,
		logger:       logger.With().Str("component", "GooglePubSubConsumer").Str("subscription_id", cfg.SubscriptionID).Logger(),
		outputChan:   make(chan types.ConsumedMessage, cfg.MaxOutstandingMessages),
		doneChan:     make(chan struct{}),
	}, nil
}
func (c *GooglePubSubConsumer) Messages() <-chan types.ConsumedMessage { return c.outputChan }
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
			payloadCopy := make([]byte, len(msg.Data))
			copy(payloadCopy, msg.Data)

			consumedMsg := types.ConsumedMessage{
				ID:          msg.ID,
				Payload:     payloadCopy,
				PublishTime: msg.PublishTime,
				Ack:         msg.Ack,
				Nack:        msg.Nack}

			// Enrich message with DeviceInfo from attributes, if present.
			if uid, ok := msg.Attributes["uid"]; ok {
				consumedMsg.DeviceInfo = &types.DeviceInfo{
					UID:      uid,
					Location: msg.Attributes["location"],
				}
			}

			select {
			case c.outputChan <- consumedMsg:
			case <-receiveCtx.Done():
				msg.Nack()
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Consumer stopping, Nacking message.")
			case <-ctx.Done():
				msg.Nack()
				c.logger.Warn().Str("msg_id", msg.ID).Msg("Outer context done, Nacking message.")
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error().Err(err).Msg("Pub/Sub Receive call exited with error")
		}
		close(c.doneChan)
	}()
	return nil
}
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
func (c *GooglePubSubConsumer) Done() <-chan struct{} { return c.doneChan }
