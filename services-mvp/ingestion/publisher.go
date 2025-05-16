package ingestion

import "context"

// --- Publisher Abstraction ---

// MessagePublisher defines an interface for publishing enriched messages.
// This allows for different implementations (e.g., Google Pub/Sub, Kafka, mock).
type MessagePublisher interface {
	PublishEnriched(ctx context.Context, message *EnrichedMessage) error
	PublishUnidentified(ctx context.Context, message *UnidentifiedDeviceMessage) error
	Stop() // For releasing resources
}
