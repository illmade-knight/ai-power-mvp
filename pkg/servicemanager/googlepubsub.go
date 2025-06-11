package servicemanager

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"google.golang.org/api/option"
)

// --- Adapters for the real Pub/Sub client ---
// These adapter structs wrap the concrete types from the `cloud.google.com/go/pubsub`
// package and ensure they satisfy our defined interfaces.

type psTopicAdapter struct{ topic *pubsub.Topic }

func (a *psTopicAdapter) ID() string                               { return a.topic.ID() }
func (a *psTopicAdapter) Exists(ctx context.Context) (bool, error) { return a.topic.Exists(ctx) }
func (a *psTopicAdapter) Update(ctx context.Context, cfg pubsub.TopicConfigToUpdate) (pubsub.TopicConfig, error) {
	return a.topic.Update(ctx, cfg)
}
func (a *psTopicAdapter) Delete(ctx context.Context) error { return a.topic.Delete(ctx) }

type psSubscriptionAdapter struct{ sub *pubsub.Subscription }

func (a *psSubscriptionAdapter) ID() string                               { return a.sub.ID() }
func (a *psSubscriptionAdapter) Exists(ctx context.Context) (bool, error) { return a.sub.Exists(ctx) }
func (a *psSubscriptionAdapter) Update(ctx context.Context, cfg pubsub.SubscriptionConfigToUpdate) (pubsub.SubscriptionConfig, error) {
	return a.sub.Update(ctx, cfg)
}
func (a *psSubscriptionAdapter) Delete(ctx context.Context) error { return a.sub.Delete(ctx) }

type psClientAdapter struct{ client *pubsub.Client }

func (a *psClientAdapter) Topic(id string) PSTopic { return &psTopicAdapter{topic: a.client.Topic(id)} }
func (a *psClientAdapter) Subscription(id string) PSSubscription {
	return &psSubscriptionAdapter{sub: a.client.Subscription(id)}
}
func (a *psClientAdapter) CreateTopic(ctx context.Context, topicID string) (PSTopic, error) {
	t, err := a.client.CreateTopic(ctx, topicID)
	if err != nil {
		return nil, err
	}
	return &psTopicAdapter{topic: t}, nil
}
func (a *psClientAdapter) CreateTopicWithConfig(ctx context.Context, topicID string, config *pubsub.TopicConfig) (PSTopic, error) {
	t, err := a.client.CreateTopicWithConfig(ctx, topicID, config)
	if err != nil {
		return nil, err
	}
	return &psTopicAdapter{topic: t}, nil
}
func (a *psClientAdapter) CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (PSSubscription, error) {
	s, err := a.client.CreateSubscription(ctx, id, cfg)
	if err != nil {
		return nil, err
	}
	return &psSubscriptionAdapter{sub: s}, nil
}
func (a *psClientAdapter) Close() error { return a.client.Close() }

// NewPubSubClientAdapter wraps a concrete *pubsub.Client to satisfy the PSClient interface.
func NewPubSubClientAdapter(client *pubsub.Client) PSClient {
	if client == nil {
		return nil
	}
	return &psClientAdapter{client: client}
}

// CreateGooglePubSubClient creates a real Pub/Sub client for use in production.
func CreateGooglePubSubClient(ctx context.Context, projectID string, clientOpts ...option.ClientOption) (PSClient, error) {
	realClient, err := pubsub.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}
	return NewPubSubClientAdapter(realClient), nil
}
