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

// PubSubManager handles the creation and deletion of Pub/Sub topics and subscriptions.
type PubSubManager struct {
	logger zerolog.Logger
}

// NewPubSubManager creates a new PubSubManager.
func NewPubSubManager(logger zerolog.Logger) *PubSubManager {
	return &PubSubManager{
		logger: logger.With().Str("component", "PubSubManager").Logger(),
	}
}

// getTargetProjectID determines the project ID to use based on the environment.
func getTargetProjectID(cfg *TopLevelConfig, environment string) (string, error) {
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if cfg.DefaultProjectID != "" {
		return cfg.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default_project_id set", environment)
}

// Setup creates all configured Pub/Sub topics and subscriptions for a given environment.
func (m *PubSubManager) Setup(ctx context.Context, cfg *TopLevelConfig, environment string, clientOpts ...option.ClientOption) error {
	projectID, err := getTargetProjectID(cfg, environment)
	if err != nil {
		return err
	}
	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub setup")

	client, err := pubsub.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create Pub/Sub client for project %s: %w", projectID, err)
	}
	defer client.Close()

	// Setup Topics
	if err := m.setupTopics(ctx, client, cfg.Resources.PubSubTopics); err != nil {
		return err
	}

	// Setup Subscriptions
	if err := m.setupSubscriptions(ctx, client, cfg.Resources.PubSubSubscriptions); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Pub/Sub setup completed successfully")
	return nil
}

func (m *PubSubManager) setupTopics(ctx context.Context, client *pubsub.Client, topicsToCreate []PubSubTopic) error {
	m.logger.Info().Int("count", len(topicsToCreate)).Msg("Setting up Pub/Sub topics...")
	for _, topicCfg := range topicsToCreate {
		if topicCfg.Name == "" {
			m.logger.Error().Msg("Skipping topic with empty name")
			continue
		}
		topic := client.Topic(topicCfg.Name)
		exists, err := topic.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of topic '%s': %w", topicCfg.Name, err)
		}
		if exists {
			m.logger.Info().Str("topic_id", topicCfg.Name).Msg("Topic already exists, ensuring configuration (labels, etc.)")
			// Attempt to update labels if they are specified in the config
			if len(topicCfg.Labels) > 0 {
				// Get current config to compare labels (optional, but good for avoiding unnecessary updates)
				// currentConfig, err := topic.Config(ctx)
				// if err != nil {
				// 	m.logger.Warn().Err(err).Str("topic_id", topicCfg.Name).Msg("Failed to get current topic config for label comparison")
				// } else if !areMapsEqual(currentConfig.Labels, topicCfg.Labels) {
				_, updateErr := topic.Update(ctx, pubsub.TopicConfigToUpdate{
					Labels: topicCfg.Labels,
				})
				if updateErr != nil {
					m.logger.Warn().Err(updateErr).Str("topic_id", topicCfg.Name).Msg("Failed to update topic labels")
				} else {
					m.logger.Info().Str("topic_id", topicCfg.Name).Msg("Topic labels updated/ensured.")
				}
				// }
			}
		} else {
			m.logger.Info().Str("topic_id", topicCfg.Name).Msg("Creating topic...")
			var createdTopic *pubsub.Topic
			var createErr error
			if len(topicCfg.Labels) > 0 {
				// Create topic with labels if specified
				createdTopic, createErr = client.CreateTopicWithConfig(ctx, topicCfg.Name, &pubsub.TopicConfig{
					Labels: topicCfg.Labels,
				})
			} else {
				// Create topic without specific config (no labels)
				createdTopic, createErr = client.CreateTopic(ctx, topicCfg.Name)
			}

			if createErr != nil {
				return fmt.Errorf("failed to create topic '%s': %w", topicCfg.Name, createErr)
			}
			m.logger.Info().Str("topic_id", createdTopic.ID()).Msg("Topic created successfully")
			// Labels are set at creation if provided, no separate update needed here for new topics.
		}
	}
	return nil
}

func (m *PubSubManager) setupSubscriptions(ctx context.Context, client *pubsub.Client, subsToCreate []PubSubSubscription) error {
	m.logger.Info().Int("count", len(subsToCreate)).Msg("Setting up Pub/Sub subscriptions...")
	for _, subCfg := range subsToCreate {
		if subCfg.Name == "" || subCfg.Topic == "" {
			m.logger.Error().Str("sub_name", subCfg.Name).Str("topic_name", subCfg.Topic).Msg("Skipping subscription with empty name or topic")
			continue
		}

		topic := client.Topic(subCfg.Topic)
		// Ensure topic exists before creating subscription (optional, but good practice)
		topicExists, err := topic.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("topic_id", subCfg.Topic).Msg("Failed to check existence of topic for subscription. Skipping subscription.")
			continue
		}
		if !topicExists {
			m.logger.Error().Str("topic_id", subCfg.Topic).Str("subscription_id", subCfg.Name).Msg("Topic does not exist. Cannot create subscription. Please define the topic in the config.")
			continue
		}

		sub := client.Subscription(subCfg.Name)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check existence of subscription '%s': %w", subCfg.Name, err)
		}

		subscriptionConfigInput := pubsub.SubscriptionConfig{
			Topic:  topic,
			Labels: subCfg.Labels,
		}
		if subCfg.AckDeadlineSeconds > 0 {
			subscriptionConfigInput.AckDeadline = time.Duration(subCfg.AckDeadlineSeconds) * time.Second
		}

		if subCfg.MessageRetention != "" {
			duration, err := time.ParseDuration(subCfg.MessageRetention)
			if err != nil {
				m.logger.Warn().Err(err).Str("subscription_id", subCfg.Name).Str("retention_duration_str", subCfg.MessageRetention).Msg("Invalid message retention duration format, using Pub/Sub default")
			} else {
				subscriptionConfigInput.RetentionDuration = duration
			}
		}

		if subCfg.RetryPolicy != nil {
			minBackoff, errMin := time.ParseDuration(subCfg.RetryPolicy.MinimumBackoff)
			maxBackoff, errMax := time.ParseDuration(subCfg.RetryPolicy.MaximumBackoff)
			if errMin == nil && errMax == nil {
				subscriptionConfigInput.RetryPolicy = &pubsub.RetryPolicy{
					MinimumBackoff: minBackoff,
					MaximumBackoff: maxBackoff,
				}
			} else {
				m.logger.Warn().Str("subscription_id", subCfg.Name).Msg("Invalid retry policy durations, using Pub/Sub default retry policy")
			}
		}

		if exists {
			m.logger.Info().Str("subscription_id", subCfg.Name).Msg("Subscription already exists, ensuring configuration")
			configToUpdate := pubsub.SubscriptionConfigToUpdate{
				AckDeadline:       subscriptionConfigInput.AckDeadline,
				Labels:            subscriptionConfigInput.Labels,
				RetryPolicy:       subscriptionConfigInput.RetryPolicy,
				RetentionDuration: subscriptionConfigInput.RetentionDuration,
			}

			_, updateErr := sub.Update(ctx, configToUpdate)
			if updateErr != nil {
				m.logger.Warn().Err(updateErr).Str("subscription_id", subCfg.Name).Msg("Failed to update existing subscription")
			} else {
				m.logger.Info().Str("subscription_id", subCfg.Name).Msg("Subscription configuration updated/ensured.")
			}
		} else {
			m.logger.Info().Str("subscription_id", subCfg.Name).Str("topic_id", subCfg.Topic).Msg("Creating subscription...")
			createdSub, err := client.CreateSubscription(ctx, subCfg.Name, subscriptionConfigInput)
			if err != nil {
				return fmt.Errorf("failed to create subscription '%s' for topic '%s': %w", subCfg.Name, subCfg.Topic, err)
			}
			m.logger.Info().Str("subscription_id", createdSub.ID()).Msg("Subscription created successfully")
		}
	}
	return nil
}

// Teardown deletes all configured Pub/Sub subscriptions and then topics for a given environment.
func (m *PubSubManager) Teardown(ctx context.Context, cfg *TopLevelConfig, environment string, clientOpts ...option.ClientOption) error {
	projectID, err := getTargetProjectID(cfg, environment)
	if err != nil {
		return err
	}
	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting Pub/Sub teardown")

	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		m.logger.Error().Str("environment", environment).Msg("Teardown protection is enabled for this environment. Manual intervention required or override.")
		return fmt.Errorf("teardown protection enabled for environment: %s", environment)
	}

	client, err := pubsub.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create Pub/Sub client for project %s: %w", projectID, err)
	}
	defer client.Close()

	if err := m.teardownSubscriptions(ctx, client, cfg.Resources.PubSubSubscriptions); err != nil {
		return err
	}
	if err := m.teardownTopics(ctx, client, cfg.Resources.PubSubTopics); err != nil {
		return err
	}

	m.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Pub/Sub teardown completed successfully")
	return nil
}

func (m *PubSubManager) teardownSubscriptions(ctx context.Context, client *pubsub.Client, subsToTeardown []PubSubSubscription) error {
	m.logger.Info().Int("count", len(subsToTeardown)).Msg("Tearing down Pub/Sub subscriptions...")
	for i := len(subsToTeardown) - 1; i >= 0; i-- {
		subCfg := subsToTeardown[i]
		if subCfg.Name == "" {
			m.logger.Warn().Msg("Skipping subscription with empty name during teardown")
			continue
		}
		sub := client.Subscription(subCfg.Name)
		exists, err := sub.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("subscription_id", subCfg.Name).Msg("Failed to check existence of subscription during teardown")
			continue
		}
		if exists {
			m.logger.Info().Str("subscription_id", subCfg.Name).Msg("Deleting subscription...")
			if err := sub.Delete(ctx); err != nil {
				m.logger.Error().Err(err).Str("subscription_id", subCfg.Name).Msg("Failed to delete subscription")
			} else {
				m.logger.Info().Str("subscription_id", subCfg.Name).Msg("Subscription deleted successfully")
			}
		} else {
			m.logger.Info().Str("subscription_id", subCfg.Name).Msg("Subscription does not exist, skipping deletion.")
		}
	}
	return nil
}

func (m *PubSubManager) teardownTopics(ctx context.Context, client *pubsub.Client, topicsToTeardown []PubSubTopic) error {
	m.logger.Info().Int("count", len(topicsToTeardown)).Msg("Tearing down Pub/Sub topics...")
	for i := len(topicsToTeardown) - 1; i >= 0; i-- {
		topicCfg := topicsToTeardown[i]
		if topicCfg.Name == "" {
			m.logger.Warn().Msg("Skipping topic with empty name during teardown")
			continue
		}
		topic := client.Topic(topicCfg.Name)
		exists, err := topic.Exists(ctx)
		if err != nil {
			m.logger.Error().Err(err).Str("topic_id", topicCfg.Name).Msg("Failed to check existence of topic during teardown")
			continue
		}
		if exists {
			m.logger.Info().Str("topic_id", topicCfg.Name).Msg("Deleting topic...")
			if err := topic.Delete(ctx); err != nil {
				if strings.Contains(err.Error(), "still has subscriptions") {
					m.logger.Error().Err(err).Str("topic_id", topicCfg.Name).Msg("Failed to delete topic because it still has subscriptions. Ensure all subscriptions are deleted first.")
				} else {
					m.logger.Error().Err(err).Str("topic_id", topicCfg.Name).Msg("Failed to delete topic")
				}
			} else {
				m.logger.Info().Str("topic_id", topicCfg.Name).Msg("Topic deleted successfully")
			}
		} else {
			m.logger.Info().Str("topic_id", topicCfg.Name).Msg("Topic does not exist, skipping deletion.")
		}
	}
	return nil
}
