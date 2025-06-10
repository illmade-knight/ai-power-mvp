package mqttconverter

import (
	"cloud.google.com/go/pubsub"
	"context"
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
		return nil, fmt.Errorf("%s environment variable not set for Pub/Sub", topicID)
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

	topic.PublishSettings.DelayThreshold = 100 * time.Millisecond
	topic.PublishSettings.CountThreshold = 100
	topic.PublishSettings.ByteThreshold = 1e6
	topic.PublishSettings.NumGoroutines = 10
	topic.PublishSettings.Timeout = 60 * time.Second

	logger.Info().Str("project_id", cfg.ProjectID).Str("topic_id", cfg.TopicID).Msg("GooglePubSubPublisher initialized successfully")
	return &GooglePubSubPublisher{
		client: client,
		topic:  topic,
		logger: logger,
	}, nil
}

// Publish publishes the raw message payload to Pub/Sub asynchronously.
// The original payload is never modified.
func (p *GooglePubSubPublisher) Publish(ctx context.Context, mqttTopic string, payload []byte, attributes map[string]string) error {
	if payload == nil {
		return errors.New("cannot publish a nil payload")
	}

	// Create a new map for attributes to avoid modifying the original,
	// and add common attributes.
	pubsubAttributes := make(map[string]string)
	for k, v := range attributes {
		pubsubAttributes[k] = v
	}
	pubsubAttributes["mqtt_topic"] = mqttTopic

	result := p.topic.Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: pubsubAttributes,
	})

	go func() {
		msgID, err := result.Get(context.Background())
		if err != nil {
			p.logger.Error().Err(err).Interface("attributes", pubsubAttributes).Msg("Failed to publish message to Pub/Sub")
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
