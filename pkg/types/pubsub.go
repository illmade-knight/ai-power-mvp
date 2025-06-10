package types

import "time"

type ConsumedMessage struct {
	ID          string
	Payload     []byte
	PublishTime time.Time
	Ack         func()
	Nack        func()
}
