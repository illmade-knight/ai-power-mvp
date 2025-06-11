package servicemanager

import (
	"cloud.google.com/go/pubsub"
	"context"
)

// --- Pub/Sub Client Abstraction Interfaces ---
// These interfaces allow us to decouple the manager's logic from the concrete
// Google Cloud Pub/Sub client, making unit testing possible with mocks.

// PSTopic defines the interface for a single Pub/Sub topic.
type PSTopic interface {
	ID() string
	Exists(ctx context.Context) (bool, error)
	Update(ctx context.Context, cfg pubsub.TopicConfigToUpdate) (pubsub.TopicConfig, error)
	Delete(ctx context.Context) error
	// IAM() *iam.Handle // Example: Could be added for IAM management
}

// PSSubscription defines the interface for a single Pub/Sub subscription.
type PSSubscription interface {
	ID() string
	Exists(ctx context.Context) (bool, error)
	Update(ctx context.Context, cfg pubsub.SubscriptionConfigToUpdate) (pubsub.SubscriptionConfig, error)
	Delete(ctx context.Context) error
	// IAM() *iam.Handle // Example: Could be added for IAM management
}

// PSClient defines the interface for the Pub/Sub client.
type PSClient interface {
	Topic(id string) PSTopic
	Subscription(id string) PSSubscription
	CreateTopic(ctx context.Context, topicID string) (PSTopic, error)
	CreateTopicWithConfig(ctx context.Context, topicID string, config *pubsub.TopicConfig) (PSTopic, error)
	CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (PSSubscription, error)
	Close() error
}
