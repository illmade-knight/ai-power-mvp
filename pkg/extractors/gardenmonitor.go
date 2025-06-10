package extractors

import (
	"encoding/json"
	"fmt"
)

// GardenMonitorExtractor is a local implementation for testing attribute extraction.
type GardenMonitorExtractor struct{}

// NewGardenMonitorExtractor creates a new extractor.
func NewGardenMonitorExtractor() *GardenMonitorExtractor {
	return &GardenMonitorExtractor{}
}

// Extract pulls the "device_eui" from the JSON payload to use as a Pub/Sub attribute.
func (e *GardenMonitorExtractor) Extract(payload []byte) (map[string]string, error) {
	var partialPayload struct {
		Payload struct {
			DE string `json:"DE"`
		} `json:"Payload"`
	}

	if err := json.Unmarshal(payload, &partialPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload for attribute extraction: %w", err)
	}

	if partialPayload.Payload.DE == "" {
		return nil, fmt.Errorf("extracted device_eui is empty")
	}

	attributes := map[string]string{
		"device_eui": partialPayload.Payload.DE,
	}

	return attributes, nil
}
