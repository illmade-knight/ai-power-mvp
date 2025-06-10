package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// GardenMonitorDecoder is the specific implementation of PayloadDecoder for our message type.
// It decodes the raw Pub/Sub message payload into our target struct.
func GardenMonitorDecoder(payload []byte) (*GardenMonitorPayload, error) {
	var msg GardenMonitorMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal outer message: %w", err)
	}
	// The message itself might be valid, but the payload within it could be null.
	if msg.Payload == nil {
		return nil, nil // Return nil to signal that this message should be acked and skipped.
	}
	// Add a timestamp if it's missing
	if msg.Payload.Timestamp.IsZero() {
		msg.Payload.Timestamp = time.Now().UTC()
	}
	return msg.Payload, nil
}

// --- Garden Monitor Specific Types ---

// GardenMonitorMessage is the expected structure of the JSON message from Pub/Sub.
type GardenMonitorMessage struct {
	Payload *GardenMonitorPayload `json:"payload"`
}

// GardenMonitorPayload is the data structure that will be inserted into BigQuery.
// The `bigquery` tags are used by the bqstore library to infer the table schema.
type GardenMonitorPayload struct {
	DE           string    `json:"DE" bigquery:"uid"`
	SIM          string    `json:"SIM" bigquery:"sim"`
	RSSI         string    `json:"RS" bigquery:"rssi"`
	Version      string    `json:"VR" bigquery:"version"`
	Sequence     int       `json:"SQ" bigquery:"sequence"`
	Battery      int       `json:"BA" bigquery:"battery"`
	Temperature  int       `json:"TM" bigquery:"temperature"`
	Humidity     int       `json:"HM" bigquery:"humidity"`
	SoilMoisture int       `json:"SM1" bigquery:"soil_moisture"`
	WaterFlow    int       `json:"FL1" bigquery:"water_flow"`
	WaterQuality int       `json:"WQ" bigquery:"water_quality"`
	TankLevel    int       `json:"DL1" bigquery:"tank_level"`
	AmbientLight int       `json:"AM" bigquery:"ambient_light"`
	Timestamp    time.Time `json:"timestamp" bigquery:"timestamp"`
}
