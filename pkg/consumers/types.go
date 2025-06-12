package consumers

import (
	"context"

	// This assumes you have a shared package for common types like ConsumedMessage.
	// If not, this type definition would also live here.
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
)

// ====================================================================================
// This file defines the core interfaces for a generic, reusable consumer and
// processing pipeline.
// ====================================================================================

// MessageProcessor defines the contract for any component that receives and
// handles decoded messages. Both `bqstore.BatchInserter` and `icestore.Batcher`
// are perfect implementations of this interface.
type MessageProcessor[T any] interface {
	// Input returns a write-only channel for sending decoded messages to the processor.
	Input() chan<- *types.BatchedMessage[T]
	// Start begins the processor's operations (e.g., its batching worker).
	Start()
	// Stop gracefully shuts down the processor, ensuring any buffered items are handled.
	Stop()
}

// MessageConsumer defines the interface for a message source (e.g., Pub/Sub, Kafka).
// It is responsible for fetching raw messages from the broker.
type MessageConsumer interface {
	// Messages returns a read-only channel from which raw messages can be consumed.
	Messages() <-chan types.ConsumedMessage
	// Start initiates the consumption of messages.
	Start(ctx context.Context) error
	// Stop gracefully ceases message consumption.
	Stop() error
	// Done returns a channel that is closed when the consumer has fully stopped.
	Done() <-chan struct{}
}

// PayloadDecoder is a generic function type responsible for transforming the
// raw byte payload from a ConsumedMessage into a specific Go struct of type T.
type PayloadDecoder[T any] func(payload []byte) (*T, error)
