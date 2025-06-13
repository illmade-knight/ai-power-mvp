package types

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
