package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
)

// --- Configuration Structs ---

// VerifierConfig holds the configuration for the verifier.
type VerifierConfig struct {
	ProjectID           string
	EnrichedTopicID     string
	UnidentifiedTopicID string
	Duration            time.Duration
	OutputFile          string
	TestRunID           string
	MachineInfo         string
	EnvironmentName     string
	ClientOpts          []option.ClientOption // For GCP client
}

// --- Data Structures for Results ---

// IngestedMessagePayload is the expected structure of the message payload
// received from the ingestionservice's output topics.
// It must contain the original timestamp and message ID from the load generator.
type IngestedMessagePayload struct {
	// Fields from your connectors.EnrichedMessage or connectors.UnidentifiedDeviceMessage
	DeviceEUI string `json:"device_eui"`
	// ... other fields from your enriched/unidentified messages ...

	// Crucial fields propagated from the original MQTTMessage by ingestionservice:
	OriginalClientTimestamp time.Time `json:"original_client_timestamp"` // Should be MQTTMessage.MessageTimestamp
	ClientMessageID         string    `json:"client_message_id"`         // Should be MQTTMessage.ClientMessageID
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
	// TODO: Add aggregate latency stats (P50, P90, P99, Avg, Min, Max)
}

// --- Global Variables / Mutexes for collecting results ---
var (
	resultsLock       sync.Mutex
	verifiedRecord    []MessageVerificationRecord
	enrichedCount     int
	unidentifiedCount int
)

// --- Helper Functions ---

func createTemporarySubscription(ctx context.Context, client *pubsub.Client, topicID, subNamePrefix string) (*pubsub.Subscription, error) {
	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic %s exists: %w", topicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist in project %s", topicID, client.Project())
	}

	subID := fmt.Sprintf("%s-%s", subNamePrefix, uuid.NewString())
	subConfig := pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second, // Adjust as needed
		// Consider setting an ExpirationPolicy if appropriate, though explicit deletion is better.
		// ExpirationPolicy: 24 * time.Hour,
		RetryPolicy: &pubsub.RetryPolicy{
			MinimumBackoff: 10 * time.Second,
			MaximumBackoff: 600 * time.Second,
		},
	}

	sub, err := client.CreateSubscription(ctx, subID, subConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription %s for topic %s: %w", subID, topicID, err)
	}
	log.Info().Str("subscription_id", sub.ID()).Str("topic_id", topicID).Msg("Temporary subscription created")
	return sub, nil
}

func deleteSubscription(ctx context.Context, sub *pubsub.Subscription) {
	if sub == nil {
		return
	}
	subID := sub.ID()
	log.Info().Str("subscription_id", subID).Msg("Attempting to delete temporary subscription...")
	if err := sub.Delete(ctx); err != nil {
		log.Error().Err(err).Str("subscription_id", subID).Msg("Failed to delete temporary subscription")
	} else {
		log.Info().Str("subscription_id", subID).Msg("Temporary subscription deleted successfully")
	}
}

func processMessage(msg *pubsub.Message, topicSource string) {
	msg.Ack() // Acknowledge immediately

	var payload IngestedMessagePayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		log.Error().Err(err).Str("topic_source", topicSource).Str("msg_id", msg.ID).Msg("Failed to unmarshal message data")
		return
	}

	// Ensure critical fields for latency calculation are present
	if payload.OriginalClientTimestamp.IsZero() {
		log.Warn().Str("topic_source", topicSource).Str("msg_id", msg.ID).Str("client_msg_id", payload.ClientMessageID).Msg("Received message with zero OriginalClientTimestamp, cannot calculate latency.")
		return
	}

	receiveTime := time.Now().UTC()
	latency := receiveTime.Sub(payload.OriginalClientTimestamp)
	processingLatency := msg.PublishTime.Sub(payload.OriginalClientTimestamp) // PubSub publish time vs original client time

	record := MessageVerificationRecord{
		ClientMessageID:         payload.ClientMessageID,
		DeviceEUI:               payload.DeviceEUI,
		TopicSource:             topicSource,
		OriginalClientTimestamp: payload.OriginalClientTimestamp,
		VerifierReceiveTime:     receiveTime,
		PubSubPublishTime:       msg.PublishTime,
		LatencyMillis:           latency.Milliseconds(),
		ProcessingLatencyMillis: processingLatency.Milliseconds(),
	}

	resultsLock.Lock()
	verifiedRecord = append(verifiedRecord, record)
	if topicSource == "enriched" {
		enrichedCount++
	} else if topicSource == "unidentified" {
		unidentifiedCount++
	}
	resultsLock.Unlock()

	log.Debug().Str("client_msg_id", record.ClientMessageID).Str("topic", topicSource).Int64("latency_ms", record.LatencyMillis).Msg("Message verified")
}

func runSubscriptionReceiver(ctx context.Context, wg *sync.WaitGroup, sub *pubsub.Subscription, topicSource string) {
	defer wg.Done()
	log.Info().Str("subscription_id", sub.ID()).Str("topic_source", topicSource).Msg("Starting message receiver...")
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		processMessage(msg, topicSource)
	})
	if err != nil && !errors.Is(err, context.Canceled) { // Don't log error if context was canceled
		log.Error().Err(err).Str("subscription_id", sub.ID()).Msg("Subscription Receive error")
	}
	log.Info().Str("subscription_id", sub.ID()).Str("topic_source", topicSource).Msg("Message receiver stopped.")
}

func main() {
	// --- Command-line flags ---
	projectID := flag.String("project", "", "GCP Project ID (required)")
	enrichedTopic := flag.String("enriched-topic", "", "Short ID of the enriched messages Pub/Sub topic (required)")
	unidentifiedTopic := flag.String("unidentified-topic", "", "Short ID of the unidentified messages Pub/Sub topic (required)")
	durationStr := flag.String("duration", "1m", "Duration for the verifier to run (e.g., 30s, 5m, 1h)")
	outputFile := flag.String("output", "verifier_results.json", "Output file for results")
	testRunID := flag.String("run-id", uuid.NewString(), "Unique ID for this test run")
	machineInfo := flag.String("machine-info", "", "Information about the machine running the test (optional)")
	envName := flag.String("env-name", "", "Name of the test environment (optional)")
	logLevelStr := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	emulatorHost := flag.String("emulator-host", "", "Pub/Sub emulator host (e.g., localhost:8085). If set, credentials are not used.")

	flag.Parse()

	// --- Logger Setup ---
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	level, err := zerolog.ParseLevel(*logLevelStr)
	if err != nil {
		log.Warn().Str("level_str", *logLevelStr).Msg("Invalid log level, defaulting to info")
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(level)
	}

	// --- Validate Flags ---
	if *projectID == "" {
		log.Fatal().Msg("-project flag is required")
	}
	if *enrichedTopic == "" {
		log.Fatal().Msg("-enriched-topic flag is required")
	}
	if *unidentifiedTopic == "" {
		log.Fatal().Msg("-unidentified-topic flag is required")
	}
	duration, err := time.ParseDuration(*durationStr)
	if err != nil {
		log.Fatal().Err(err).Msg("Invalid -duration format")
	}

	cfg := VerifierConfig{
		ProjectID:           *projectID,
		EnrichedTopicID:     *enrichedTopic,
		UnidentifiedTopicID: *unidentifiedTopic,
		Duration:            duration,
		OutputFile:          *outputFile,
		TestRunID:           *testRunID,
		MachineInfo:         *machineInfo,
		EnvironmentName:     *envName,
	}

	log.Info().Interface("config", cfg).Msg("Pub/Sub Verifier starting")

	// --- Setup Context and Signal Handling for Graceful Shutdown ---
	ctx, mainCancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer mainCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Warn().Str("signal", sig.String()).Msg("Received shutdown signal. Terminating verification...")
		mainCancel() // Cancel the main context
	}()

	// --- Initialize Pub/Sub Client ---
	if *emulatorHost != "" {
		log.Info().Str("host", *emulatorHost).Msg("Using Pub/Sub emulator")
		cfg.ClientOpts = append(cfg.ClientOpts, option.WithEndpoint(*emulatorHost), option.WithoutAuthentication())
	} else {
		log.Info().Msg("Using live Pub/Sub (ADC or specified credentials if configured)")
		// Add credential file option if needed:
		// if credsFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); credsFile != "" {
		//    cfg.ClientOpts = append(cfg.ClientOpts, option.WithCredentialsFile(credsFile))
		// }
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, cfg.ClientOpts...)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub client")
	}
	defer client.Close()

	// --- Create Temporary Subscriptions ---
	var wg sync.WaitGroup
	startTime := time.Now().UTC()

	enrichedSub, err := createTemporarySubscription(ctx, client, cfg.EnrichedTopicID, "verifier-enriched")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create subscription for enriched topic")
	}
	defer deleteSubscription(context.Background(), enrichedSub) // Use background context for cleanup

	unidentifiedSub, err := createTemporarySubscription(ctx, client, cfg.UnidentifiedTopicID, "verifier-unidentified")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create subscription for unidentified topic")
	}
	defer deleteSubscription(context.Background(), unidentifiedSub) // Use background context for cleanup

	// --- Start Receiving Messages ---
	wg.Add(2)
	go runSubscriptionReceiver(ctx, &wg, enrichedSub, "enriched")
	go runSubscriptionReceiver(ctx, &wg, unidentifiedSub, "unidentified")

	log.Info().Msg("Message receivers started. Verifying messages for duration...")
	<-ctx.Done() // Wait for duration to elapse or signal
	log.Info().Msg("Verification duration ended or shutdown signal received.")

	// Wait for receiver goroutines to finish processing any in-flight messages
	// (they will stop once their context passed to sub.Receive is canceled by mainCancel)
	log.Info().Msg("Waiting for message receivers to complete...")
	// Give a short grace period for receivers to finish after context cancellation
	shutdownGraceTimer := time.NewTimer(5 * time.Second)
	select {
	case <-shutdownGraceTimer.C:
		log.Warn().Msg("Grace period for receivers ended.")
	}
	// wg.Wait() // This might block indefinitely if Receive doesn't return promptly on ctx cancel.
	// Instead, rely on the fact that sub.Receive will return when its context is done.

	// --- Collect and Save Results ---
	endTime := time.Now().UTC()
	resultsLock.Lock() // Ensure all writes to shared slices are done
	finalResults := TestRunResults{
		TestRunID:             cfg.TestRunID,
		StartTime:             startTime,
		EndTime:               endTime,
		ActualDuration:        endTime.Sub(startTime).String(),
		RequestedDuration:     cfg.Duration.String(),
		MachineInfo:           cfg.MachineInfo,
		EnvironmentName:       cfg.EnvironmentName,
		ProjectID:             cfg.ProjectID,
		EnrichedTopicID:       cfg.EnrichedTopicID,
		UnidentifiedTopicID:   cfg.UnidentifiedTopicID,
		TotalMessagesVerified: len(verifiedRecord),
		EnrichedMessages:      enrichedCount,
		UnidentifiedMessages:  unidentifiedCount,
		Records:               verifiedRecord,
	}
	resultsLock.Unlock()

	resultsJSON, err := json.MarshalIndent(finalResults, "", "  ")
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal results to JSON")
	} else {
		// Use os.WriteFile (Go 1.16+) instead of ioutil.WriteFile
		if err := os.WriteFile(cfg.OutputFile, resultsJSON, 0644); err != nil {
			log.Error().Err(err).Str("file", cfg.OutputFile).Msg("Failed to write results to file")
		} else {
			log.Info().Str("file", cfg.OutputFile).Int("num_records", len(finalResults.Records)).Msg("Verification results saved.")
		}
	}

	log.Info().Msg("Pub/Sub Verifier finished.")
}
