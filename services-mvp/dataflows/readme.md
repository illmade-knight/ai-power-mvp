package servicemanager

import (
"context"
"fmt"
"strings"
"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// --- PubSub Manager ---

// PubSubManager handles the creation and deletion of Pub/Sub topics and subscriptions.
type PubSubManager struct {
client PSClient // Use the interface for testability
logger zerolog.Logger
}

// NewPubSubManager creates a new PubSubManager.
func NewPubSubManager(client PSClient, logger zerolog.Logger) (*PubSubManager, error) {
if client == nil {
return nil, fmt.Errorf("PubSub client (PSClient interface) cannot be nil")
}
return &PubSubManager{
client: client,
logger: logger.With().Str("component", "PubSubManager").Logger(),
}, nil
}

// GetTargetProjectID determines the project ID to use based on the environment.
func GetTargetProjectID(cfg *TopLevelConfig, environment string) (string, error) {
if envSpec, ok := cfg.Environments[environment]; ok && envSpec.ProjectID != "" {
return envSpec.ProjectID, nil
}
if cfg.DefaultProjectID != "" {
return cfg.DefaultProjectID, nil
}
return "", fmt.Errorf("project ID not found for environment '%s' and no default_project_id set", environment)
}

// Setup creates all configured Pub/Sub topics and subscriptions for a given environment.
func (m *PubSubManager) Setup(ctx context.Context, cfg *TopLevelConfig, environment string) error {
projectID, err := GetTargetProjectID(cfg, environment)
if err != nil {
return err
}
m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub setup")

	if err := m.setupTopics(ctx, cfg.Resources.PubSubTopics); err != nil {
		return err
	}

	// Pass the resolved projectID down to the subscription setup.
	if err := m.setupSubscriptions(ctx, cfg.Resources.PubSubSubscriptions, projectID); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Pub/Sub setup completed successfully")
	return nil
}

func (m *PubSubManager) setupTopics(ctx context.Context, topicsToCreate []PubSubTopic) error {
m.logger.Info().Int("count", len(topicsToCreate)).Msg("Setting up Pub/Sub topics...")
for _, topicSpec := range topicsToCreate {
if topicSpec.Name == "" {
m.logger.Error().Msg("Skipping topic with empty name")
continue
}
topic := m.client.Topic(topicSpec.Name)
exists, err := topic.Exists(ctx)
if err != nil {
return fmt.Errorf("failed to check existence of topic '%s': %w", topicSpec.Name, err)
}
if exists {
m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic already exists, ensuring configuration (labels, etc.)")
if len(topicSpec.Labels) > 0 {
_, updateErr := topic.Update(ctx, pubsub.TopicConfigToUpdate{
Labels: topicSpec.Labels,
})
if updateErr != nil {
m.logger.Warn().Err(updateErr).Str("topic_id", topicSpec.Name).Msg("Failed to update topic labels")
} else {
m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic labels updated/ensured.")
}
}
} else {
m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Creating topic...")
var createdTopic PSTopic
var createErr error
if len(topicSpec.Labels) > 0 {
createdTopic, createErr = m.client.CreateTopicWithConfig(ctx, topicSpec.Name, &pubsub.TopicConfig{
Labels: topicSpec.Labels,
})
} else {
createdTopic, createErr = m.client.CreateTopic(ctx, topicSpec.Name)
}

			if createErr != nil {
				return fmt.Errorf("failed to create topic '%s': %w", topicSpec.Name, createErr)
			}
			m.logger.Info().Str("topic_id", createdTopic.ID()).Msg("Topic created successfully")
		}
	}
	return nil
}

// buildGcpSubscriptionConfig now accepts a projectID to create the temporary client.
func (m *PubSubManager) buildGcpSubscriptionConfig(ctx context.Context, subSpec PubSubSubscription, projectID string) (*pubsub.SubscriptionConfig, error) {
// The Google client library requires a concrete *pubsub.Topic.
// We use the projectID to create this temporary client correctly.
topicClient, err := pubsub.NewClient(ctx, projectID)
if err != nil {
return nil, fmt.Errorf("failed to create temporary client for topic wrapping: %w", err)
}
defer topicClient.Close()
topicForConfig := topicClient.Topic(subSpec.Topic)

	gcpConfig := &pubsub.SubscriptionConfig{
		Topic:  topicForConfig,
		Labels: subSpec.Labels,
	}

	if subSpec.AckDeadlineSeconds > 0 {
		gcpConfig.AckDeadline = time.Duration(subSpec.AckDeadlineSeconds) * time.Second
	}

	if subSpec.MessageRetention != "" {
		if duration, err := time.ParseDuration(subSpec.MessageRetention); err == nil {
			gcpConfig.RetentionDuration = duration
		} else {
			m.logger.Warn().Err(err).Str("sub", subSpec.Name).Msg("Invalid message retention format, using default")
		}
	}

	if subSpec.RetryPolicy != nil {
		minB, errMin := time.ParseDuration(subSpec.RetryPolicy.MinimumBackoff)
		maxB, errMax := time.ParseDuration(subSpec.RetryPolicy.MaximumBackoff)
		if errMin == nil && errMax == nil {
			gcpConfig.RetryPolicy = &pubsub.RetryPolicy{MinimumBackoff: minB, MaximumBackoff: maxB}
		} else {
			m.logger.Warn().Str("sub", subSpec.Name).Msg("Invalid retry policy duration format, using default")
		}
	}

	return gcpConfig, nil
}

// setupSubscriptions now accepts a projectID to pass to its helper.
func (m *PubSubManager) setupSubscriptions(ctx context.Context, subsToCreate []PubSubSubscription, projectID string) error {
m.logger.Info().Int("count", len(subsToCreate)).Msg("Setting up Pub/Sub subscriptions...")
for _, subSpec := range subsToCreate {
if subSpec.Name == "" || subSpec.Topic == "" {
m.logger.Error().Str("sub_name", subSpec.Name).Str("topic_name", subSpec.Topic).Msg("Skipping subscription with empty name or topic")
continue
}

		topic := m.client.Topic(subSpec.Topic)
		topicExists, err := topic.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("topic_id", subSpec.Topic).Msg("Failed to check topic existence. Skipping subscription.")
			continue
		}
		if !topicExists {
			m.logger.Error().Str("topic_id", subSpec.Topic).Str("subscription_id", subSpec.Name).Msg("Topic does not exist. Cannot create subscription.")
			continue
		}

		sub := m.client.Subscription(subSpec.Name)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of subscription '%s': %w", subSpec.Name, err)
		}

		// Use the helper function to build the config, now passing the projectID.
		gcpConfig, err := m.buildGcpSubscriptionConfig(ctx, subSpec, projectID)
		if err != nil {
			return fmt.Errorf("failed to build GCP config for subscription '%s': %w", subSpec.Name, err)
		}

		if exists {
			m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription already exists, ensuring configuration")
			configToUpdate := pubsub.SubscriptionConfigToUpdate{
				AckDeadline:       gcpConfig.AckDeadline,
				Labels:            gcpConfig.Labels,
				RetryPolicy:       gcpConfig.RetryPolicy,
				RetentionDuration: gcpConfig.RetentionDuration,
			}
			if _, updateErr := sub.Update(ctx, configToUpdate); updateErr != nil {
				m.logger.Warn().Err(updateErr).Str("subscription_id", subSpec.Name).Msg("Failed to update subscription")
			} else {
				m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription configuration updated/ensured.")
			}
		} else {
			m.logger.Info().Str("subscription_id", subSpec.Name).Str("topic_id", subSpec.Topic).Msg("Creating subscription...")
			createdSub, err := m.client.CreateSubscription(ctx, subSpec.Name, *gcpConfig)
			if err != nil {
				return fmt.Errorf("failed to create subscription '%s' for topic '%s': %w", subSpec.Name, subSpec.Topic, err)
			}
			m.logger.Info().Str("subscription_id", createdSub.ID()).Msg("Subscription created successfully")
		}
	}
	return nil
}

// Teardown deletes all configured Pub/Sub resources for a given environment.
func (m *PubSubManager) Teardown(ctx context.Context, cfg *TopLevelConfig, environment string) error {
projectID, err := GetTargetProjectID(cfg, environment)
if err != nil {
return err
}
m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub teardown")

	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		return fmt.Errorf("teardown protection enabled for environment: %s", environment)
	}

	if err := m.teardownSubscriptions(ctx, cfg.Resources.PubSubSubscriptions); err != nil {
		return err
	}
	if err := m.teardownTopics(ctx, cfg.Resources.PubSubTopics); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Pub/Sub teardown completed successfully")
	return nil
}

func (m *PubSubManager) teardownSubscriptions(ctx context.Context, subsToTeardown []PubSubSubscription) error {
m.logger.Info().Int("count", len(subsToTeardown)).Msg("Tearing down Pub/Sub subscriptions...")
for i := len(subsToTeardown) - 1; i >= 0; i-- {
subSpec := subsToTeardown[i]
if subSpec.Name == "" { continue }
sub := m.client.Subscription(subSpec.Name)
m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Attempting to delete subscription...")
if err := sub.Delete(ctx); err != nil {
if strings.Contains(err.Error(), "NotFound") {
m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription not found, skipping.")
} else {
m.logger.Error().Err(err).Str("subscription_id", subSpec.Name).Msg("Failed to delete subscription")
}
} else {
m.logger.Info().Str("subscription_id", subSpec.Name).Msg("Subscription deleted successfully")
}
}
return nil
}

func (m *PubSubManager) teardownTopics(ctx context.Context, topicsToTeardown []PubSubTopic) error {
m.logger.Info().Int("count", len(topicsToTeardown)).Msg("Tearing down Pub/Sub topics...")
for i := len(topicsToTeardown) - 1; i >= 0; i-- {
topicSpec := topicsToTeardown[i]
if topicSpec.Name == "" { continue }
topic := m.client.Topic(topicSpec.Name)
m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Attempting to delete topic...")
if err := topic.Delete(ctx); err != nil {
if strings.Contains(err.Error(), "NotFound") {
m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic not found, skipping.")
} else if strings.Contains(err.Error(), "still has subscriptions") {
m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Topic has active subscriptions and cannot be deleted.")
} else {
m.logger.Error().Err(err).Str("topic_id", topicSpec.Name).Msg("Failed to delete topic")
}
} else {
m.logger.Info().Str("topic_id", topicSpec.Name).Msg("Topic deleted successfully")
}
}
return nil
}

// CreateRealPubSubClient creates a real Pub/Sub client for use in production.
func CreateRealPubSubClient(ctx context.Context, projectID string, clientOpts ...option.ClientOption) (PSClient, error) {
realClient, err := pubsub.NewClient(ctx, projectID, clientOpts...)
if err != nil {
return nil, fmt.Errorf("pubsub.NewClient: %w", err)
}
return NewPubSubClientAdapter(realClient), nil
}
