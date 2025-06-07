package converter

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"os"
	"time"
)

// --- Google Cloud Pub/Sub Publisher Implementation ---

// GooglePubSubPublisherConfig holds configuration for the Pub/Sub publisher.
type GooglePubSubPublisherConfig struct {
	ProjectID       string
	TopicID         string
	CredentialsFile string
}

// LoadGooglePubSubPublisherConfigFromEnv loads Pub/Sub configuration from environment variables.
func LoadGooglePubSubPublisherConfigFromEnv(topicID string) (*GooglePubSubPublisherConfig, error) {
	cfg := &GooglePubSubPublisherConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		TopicID:         os.Getenv(topicID),
		CredentialsFile: os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub")
	}
	if cfg.TopicID == "" {
		return nil, errors.New("PUBSUB_TOPIC_ID environment variable not set for Pub/Sub")
	}
	return cfg, nil
}

// GooglePubSubPublisher implements MessagePublisher for Google Cloud Pub/Sub.
type GooglePubSubPublisher struct {
	client *pubsub.Client
	topic  *pubsub.Topic
	logger zerolog.Logger
}

// NewGooglePubSubPublisher creates a new publisher for Google Cloud Pub/Sub.
func NewGooglePubSubPublisher(ctx context.Context, cfg *GooglePubSubPublisherConfig, logger zerolog.Logger) (*GooglePubSubPublisher, error) {
	var opts []option.ClientOption
	pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")

	if pubsubEmulatorHost != "" {
		logger.Info().Str("emulator_host", pubsubEmulatorHost).Msg("Using Pub/Sub emulator")
		opts = append(opts, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	} else if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using credentials file for Pub/Sub")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for Pub/Sub")
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	topic := client.Topic(cfg.TopicID)

	// MODIFICATION: Configure Pub/Sub topic settings for high throughput.
	// These settings control how the client batches messages.
	topic.PublishSettings.DelayThreshold = 100 * time.Millisecond // How long to wait before sending a batch
	topic.PublishSettings.CountThreshold = 100                    // Max number of messages in a batch
	topic.PublishSettings.ByteThreshold = 1e6                     // Max size of a batch (1MB)
	topic.PublishSettings.NumGoroutines = 10                      // Number of goroutines used to publish messages
	topic.PublishSettings.Timeout = 60 * time.Second              // Timeout for publishing a message

	logger.Info().Str("project_id", cfg.ProjectID).Str("topic_id", cfg.TopicID).Msg("GooglePubSubPublisher initialized successfully")
	return &GooglePubSubPublisher{
		client: client,
		topic:  topic,
		logger: logger,
	}, nil
}

// Publish publishes the MQTT message to pubsub asynchronously.
// MODIFICATION: This function is now non-blocking. It hands the message to the
// Pub/Sub client's background publisher and returns immediately.
// The result of the publish operation is checked in a separate goroutine.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, message *MQTTMessage) error {
	if message == nil {
		return errors.New("cannot publish a nil message")
	}
	data, err := json.Marshal(message)
	if err != nil {
		p.logger.Error().Err(err).Str("device_eui", message.DeviceInfo.DeviceEUI).Msg("Failed to marshal message for Pub/Sub")
		return fmt.Errorf("json.Marshal(message): %w", err)
	}

	attributes := map[string]string{
		"message_type": "mqtt",
		"device_eui":   message.DeviceInfo.DeviceEUI,
	}

	// topic.Publish is non-blocking. It returns a result object immediately.
	result := p.topic.Publish(ctx, &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	})

	// MODIFICATION: Check the result in a separate goroutine so we don't block
	// the main processing worker. This is the key change for decoupling.
	go func() {
		// .Get() is the blocking call that waits for the publish to complete.
		msgID, err := result.Get(context.Background()) // Use a background context here
		if err != nil {
			// This is where you would handle a failed publish, e.g., log it, send to a dead-letter queue, etc.
			p.logger.Error().Err(err).Interface("attributes", attributes).Msg("Failed to publish message to Pub/Sub")
			return
		}
		p.logger.Debug().Str("message_id", msgID).Str("topic", p.topic.ID()).Msg("Message published successfully to Pub/Sub")
	}()

	return nil
}

// Stop flushes pending messages and closes the Pub/Sub client.
func (p *GooglePubSubPublisher) Stop() {
	p.logger.Info().Msg("Stopping GooglePubSubPublisher...")
	if p.topic != nil {
		// Stop ensures all pending messages are published before returning.
		p.topic.Stop()
		p.logger.Info().Msg("Pub/Sub topic stopped and flushed.")
	}
	if p.client != nil {
		if err := p.client.Close(); err != nil {
			p.logger.Error().Err(err).Msg("Error closing Pub/Sub client")
		} else {
			p.logger.Info().Msg("Pub/Sub client closed.")
		}
	}
}
