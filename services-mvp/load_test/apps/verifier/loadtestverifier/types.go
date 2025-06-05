package loadtestverifier

import (
	"encoding/json"
	"github.com/rs/zerolog"
	"os"
	"time"
)

// --- Data Structures for Results (from former results.go) ---

// IngestedMessagePayload is the expected structure of the message payload
// received from the ingestion service's output topics.
type IngestedMessagePayload struct {
	DeviceEUI string `json:"device_eui"`
	// Assuming these critical fields are propagated from the original MQTTMessage by the ingestion service:
	// These names should match what your `messenger` produces in its MQTTMessage
	// and what your `connectors` package propagates.
	MessageTimestamp        time.Time `json:"message_timestamp"` // This should be the OriginalClientTimestamp
	OriginalClientTimestamp time.Time `json:"original_client_timestamp"`
	ClientMessageID         string    `json:"client_message_id"`
}

// MessageVerificationRecord stores details for each verified message.
type MessageVerificationRecord struct {
	ClientMessageID         string    `json:"client_message_id"`
	DeviceEUI               string    `json:"device_eui,omitempty"`
	TopicSource             string    `json:"topic_source"` // "enriched" or "unidentified"
	OriginalClientTimestamp time.Time `json:"original_client_timestamp"`
	VerifierReceiveTime     time.Time `json:"verifier_receive_time"`
	PubSubPublishTime       time.Time `json:"pubsub_publish_time"`   // The timestamp from the Pub/Sub message itself
	LatencyMillis           int64     `json:"latency_ms"`            // VerifierReceiveTime - OriginalClientTimestamp
	ProcessingLatencyMillis int64     `json:"processing_latency_ms"` // PubSubPublishTime - OriginalClientTimestamp
}

// TestRunResults aggregates all data for a load test verification run.
type TestRunResults struct {
	TestRunID             string                      `json:"test_run_id"`
	StartTime             time.Time                   `json:"start_time"`
	EndTime               time.Time                   `json:"end_time"`
	ActualDuration        string                      `json:"actual_duration"`
	RequestedDuration     string                      `json:"requested_duration"`
	MachineInfo           string                      `json:"machine_info,omitempty"`
	EnvironmentName       string                      `json:"environment_name,omitempty"`
	ProjectID             string                      `json:"project_id"`
	EnrichedTopicID       string                      `json:"enriched_topic_id"`
	UnidentifiedTopicID   string                      `json:"unidentified_topic_id"`
	TotalMessagesVerified int                         `json:"total_messages_verified"`
	EnrichedMessages      int                         `json:"enriched_messages"`
	UnidentifiedMessages  int                         `json:"unidentified_messages"`
	Records               []MessageVerificationRecord `json:"records"`
}

// SaveResults saves the test run results to a JSON file.
// It now uses the logger passed to it, or a global one if not adapted.
// For better encapsulation, it could take a logger argument or use one from a Verifier instance.
func (trr *TestRunResults) SaveResults(filePath string, logger zerolog.Logger) error {
	resultsJSON, err := json.MarshalIndent(trr, "", "  ")
	if err != nil {
		logger.Error().Err(err).Msg("Failed to marshal results to JSON")
		return err
	}

	if err := os.WriteFile(filePath, resultsJSON, 0644); err != nil {
		logger.Error().Err(err).Str("file", filePath).Msg("Failed to write results to file")
		return err
	}
	logger.Info().Str("file", filePath).Int("num_records", len(trr.Records)).Msg("Verification results saved.")
	return nil
}
