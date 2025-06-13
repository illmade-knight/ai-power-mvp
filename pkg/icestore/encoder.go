package icestore

import (
	"encoding/json"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"time"
)

// NewConsumedMessageEncoder returns an encoder that transforms a raw  ConsumedMessage
// into a structured ArchivalData object.
// The return type *is* the T for the whole service.
func NewConsumedMessageEncoder() func(payload []byte) (*ArchivalData, error) {
	return func(payload []byte) (*ArchivalData, error) {
		var msg types.ConsumedMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal for key generation: %w", err)
		}

		if msg.Payload == nil {
			return nil, nil // Ack and skip message, as per original logic.
		}

		// Logic to create the key for file grouping (e.g., by day).
		ts := msg.PublishTime
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		batchKey := ts.Format("2006/01/02") // e.g., "2025/06/13"

		// Create and return the final ArchivalData payload.
		return &ArchivalData{
			BatchKey:              batchKey,
			OriginalPubSubPayload: json.RawMessage(payload), // Keep the original data for audit.
			ArchivedAt:            time.Now().UTC(),
		}, nil
	}
}
