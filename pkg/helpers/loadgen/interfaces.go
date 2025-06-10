// loadgen/interfaces.go

package loadgen

import (
	"context"
)

// PayloadGenerator defines the interface for generating message payloads.
// An application can implement this interface to create its own custom payloads.
// It is passed a pointer to the device to allow device-specific information
// (like an ID) to be included in the payload.
type PayloadGenerator interface {
	GeneratePayload(device *Device) ([]byte, error)
}

// Client defines the interface for a client that can publish messages.
// This allows for different client implementations (e.g., MQTT, HTTP, etc.).
type Client interface {
	Connect() error
	Disconnect()
	// Publish is responsible for taking a device, generating its payload,
	// and sending it via the specific client implementation.
	Publish(ctx context.Context, device *Device) error
}
