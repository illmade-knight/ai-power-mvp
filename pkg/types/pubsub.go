package types

import "time"

// broker like Google Pub/Sub. It contains the raw, unprocessed payload.
type ConsumedMessage struct {
	// ID is the unique identifier for the message from the source broker.
	ID string
	// Payload is the raw byte content of the message.
	Payload []byte
	// PublishTime is the timestamp when the message was originally published.
	PublishTime time.Time
	// Ack is a function to call to acknowledge that the message has been
	// successfully processed.
	Ack func()
	// Nack is a function to call to signal that processing has failed and the
	// message should be re-queued or sent to a dead-letter queue.
	Nack func()
}

// BatchedMessage is a generic wrapper that links a raw, original `ConsumedMessage`
// with its successfully decoded and structured payload of type T.
//
// This is a crucial intermediate type that allows the final processing stage
// (e.g., a batch inserter or uploader) to work with clean, typed data (`Payload`)
// while still retaining the ability to Ack/Nack the `OriginalMessage`.
type BatchedMessage[T any] struct {
	// OriginalMessage is the message as it was received from the consumer.
	OriginalMessage ConsumedMessage
	// Payload is the structured data of type T, created by the PayloadDecoder.
	Payload *T
}
