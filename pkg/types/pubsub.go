package types

import (
	"time"
)

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

	// a pipeline can enrich data with more specific device data
	DeviceInfo *DeviceInfo
}

type DeviceInfo struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	ServiceTag string `json:"serviceTag" bigquery:"service_tag"`
	Location   string `json:"location"`
}
