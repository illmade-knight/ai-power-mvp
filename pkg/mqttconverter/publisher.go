package mqttconverter

import (
	"context"
)

// --- Publisher Abstraction ---

// MessagePublisher defines a generic interface for publishing a raw message payload.
// This allows for different implementations (e.g., Google Pub/Sub, Kafka, mock)
// and decouples the service from any specific message format.
type MessagePublisher interface {
	// Publish sends the raw payload to the messaging system.
	// It includes the original MQTT topic and a map of attributes for metadata/routing.
	Publish(ctx context.Context, mqttTopic string, payload []byte, attributes map[string]string) error
	Stop() // For releasing resources
}
