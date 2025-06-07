package connectors

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
func LoadGooglePubSubPublisherConfigFromEnv(topicID string) (*GooglePubSubPublisherConfig, error) {
	cfg := &GooglePubSubPublisherConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		TopicID:         os.Getenv(topicID),
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
// MODIFIED to explicitly use emulator endpoint options if PUBSUB_EMULATOR_HOST is set.
func NewGooglePubSubPublisher(ctx context.Context, cfg *GooglePubSubPublisherConfig, logger zerolog.Logger) (*GooglePubSubPublisher, error) {
	var opts []option.ClientOption
	pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST")

	if pubsubEmulatorHost != "" {
		logger.Info().Str("emulator_host", pubsubEmulatorHost).Msg("Using Pub/Sub emulator explicitly with endpoint and no auth.")
		opts = append(opts, option.WithEndpoint(pubsubEmulatorHost))
		opts = append(opts, option.WithoutAuthentication()) // Emulators typically don't use auth
	} else if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for Pub/Sub (not emulator)")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for Pub/Sub (not emulator)")
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		logger.Error().Err(err).Str("project_id", cfg.ProjectID).Msg("Failed to create Google Cloud Pub/Sub client")
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}

	topic := client.Topic(cfg.TopicID)
	// For emulators, topic creation might be handled by the test setup.
	// If not, ensure it exists or create it.
	if pubsubEmulatorHost != "" { // Only check/create if using emulator and if desired
		exists, err := topic.Exists(ctx)
		if err != nil {
			client.Close()
			logger.Error().Err(err).Str("topic_id", cfg.TopicID).Msg("Failed to check if Pub/Sub topic exists on emulator")
			return nil, fmt.Errorf("topic.Exists (emulator): %w", err)
		}
		if !exists {
			client.Close()
			logger.Error().Str("topic_id", cfg.TopicID).Msg("Pub/Sub topic does not exist on emulator (expected to be created by test setup)")
			return nil, fmt.Errorf("Pub/Sub topic %s does not exist on emulator for project %s", cfg.TopicID, cfg.ProjectID)
		}
	}

	logger.Info().Str("project_id", cfg.ProjectID).Str("topic_id", cfg.TopicID).Msg("GooglePubSubPublisher initialized successfully")
	return &GooglePubSubPublisher{
		client: client,
		topic:  topic,
		logger: logger,
	}, nil
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

	// MODIFICATION: Check the result in a separate goroutine so we don't block
	// the main processing worker. This is the key change for decoupling.
	go func() {
		msgID, err := result.Get(ctx)
		if err != nil {
			p.logger.Error().Err(err).Str("identifier", loggingIdentifier).Interface("attributes", attributes).Msg("Failed to publish message to Pub/Sub")
		}
		p.logger.Debug().Str("message_id", msgID).Str("identifier", loggingIdentifier).Str("topic", p.topic.ID()).Interface("attributes", attributes).Msg("Message published successfully to Pub/Sub")
	}()
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
