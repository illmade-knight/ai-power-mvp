package ingestion

import (
	"cloud.google.com/go/pubsub" // Google Cloud Pub/Sub
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option" // For Pub/Sub credentials if needed
	"os"
)

// --- Google Cloud Pub/Sub Publisher Implementation ---

// GooglePubSubPublisherConfig holds configuration for the Pub/Sub publisher.
type GooglePubSubPublisherConfig struct {
	ProjectID string
	TopicID   string
	// Optional: CredentialsFile for specific service account, otherwise ADC are used.
	CredentialsFile string
}

// LoadGooglePubSubPublisherConfigFromEnv loads Pub/Sub configuration from environment variables.
func LoadGooglePubSubPublisherConfigFromEnv() (*GooglePubSubPublisherConfig, error) {
	cfg := &GooglePubSubPublisherConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		TopicID:         os.Getenv("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES"),
		CredentialsFile: os.Getenv("GCP_PUBSUB_CREDENTIALS_FILE"), // Optional
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Pub/Sub")
	}
	if cfg.TopicID == "" {
		return nil, errors.New("PUBSUB_TOPIC_ID_ENRICHED_MESSAGES environment variable not set for Pub/Sub")
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
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for Pub/Sub")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for Pub/Sub")
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		logger.Error().Err(err).Str("project_id", cfg.ProjectID).Msg("Failed to create Google Cloud Pub/Sub client")
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	topic := client.Topic(cfg.TopicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		client.Close() // Clean up client if topic check fails
		logger.Error().Err(err).Str("topic_id", cfg.TopicID).Msg("Failed to check if Pub/Sub topic exists")
		return nil, fmt.Errorf("topic.Exists: %w", err)
	}
	if !exists {
		client.Close()
		logger.Error().Str("topic_id", cfg.TopicID).Msg("Pub/Sub topic does not exist")
		return nil, fmt.Errorf("Pub/Sub topic %s does not exist in project %s", cfg.TopicID, cfg.ProjectID)
	}

	logger.Info().Str("project_id", cfg.ProjectID).Str("topic_id", cfg.TopicID).Msg("GooglePubSubPublisher initialized successfully")
	return &GooglePubSubPublisher{
		client: client,
		topic:  topic,
		logger: logger,
	}, nil
}

// Publish sends an EnrichedMessage to the configured Google Cloud Pub/Sub topic.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, message *EnrichedMessage) error {
	if message == nil {
		p.logger.Warn().Msg("Attempted to publish a nil message")
		return errors.New("cannot publish nil message")
	}

	data, err := json.Marshal(message)
	if err != nil {
		p.logger.Error().Err(err).Str("device_eui", message.DeviceEUI).Msg("Failed to marshal EnrichedMessage for Pub/Sub")
		return fmt.Errorf("json.Marshal: %w", err)
	}

	// Publish the message. The Topic.Publish method is asynchronous.
	// Get the result of the publish operation.
	// result := p.topic.Publish(ctx, &pubsub.Message{Data: data})
	// _, err = result.Get(ctx) // Blocks until the message is published or an error occurs.

	// For higher throughput, it's common not to wait for Get() on every message,
	// but to handle errors asynchronously or rely on batching.
	// For simplicity in this MVP stage, we can make it synchronous or semi-synchronous.
	// Let's use a simple synchronous publish for now.
	// For production, review Topic.EnableMessageOrdering, Topic.PublishSettings (batching, concurrency).

	result := p.topic.Publish(ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{ // Optional: add attributes for filtering subscriptions
			"device_eui": message.DeviceEUI,
			"client_id":  message.ClientID,
			"category":   message.DeviceCategory,
		},
	})

	// Get blocks until the message is published or the context is done.
	msgID, err := result.Get(ctx)
	if err != nil {
		p.logger.Error().Err(err).Str("device_eui", message.DeviceEUI).Msg("Failed to publish message to Pub/Sub")
		return fmt.Errorf("pubsub publish Get: %w", err)
	}

	p.logger.Debug().Str("message_id", msgID).Str("device_eui", message.DeviceEUI).Msg("Message published successfully to Pub/Sub")
	return nil
}

// Stop flushes pending messages and closes the Pub/Sub client.
func (p *GooglePubSubPublisher) Stop() {
	p.logger.Info().Msg("Stopping GooglePubSubPublisher...")
	if p.topic != nil {
		p.topic.Stop() // Stop publishing messages. This also flushes.
		p.logger.Info().Msg("Pub/Sub topic stopped.")
	}
	if p.client != nil {
		if err := p.client.Close(); err != nil {
			p.logger.Error().Err(err).Msg("Error closing Pub/Sub client")
		} else {
			p.logger.Info().Msg("Pub/Sub client closed.")
		}
	}
}
