package converter

import (
	"context"
)

// --- Publisher Abstraction ---

// MessagePublisher defines an interface for publishing enriched messages.
// This allows for different implementations (e.g., Google Pub/Sub, Kafka, mock).
type MessagePublisher interface {
	Publish(ctx context.Context, message *MQTTMessage) error
	Stop() // For releasing resources
}
