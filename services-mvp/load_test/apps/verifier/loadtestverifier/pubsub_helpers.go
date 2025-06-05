package loadtestverifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// processMessage handles an incoming Pub/Sub message, acknowledges it,
// parses its payload, and creates a verification record.
// It's called by runSubscriptionReceiver.
func processMessage(
	msg *pubsub.Message,
	topicSource string,
	logger zerolog.Logger,
	records *[]MessageVerificationRecord, // Pointer to the shared slice in Verifier
	enrichedCounter *int, // Pointer to the shared counter in Verifier
	unidentifiedCounter *int, // Pointer to the shared counter in Verifier
	recordsLock *sync.Mutex, // Pointer to the shared mutex in Verifier
) {
	msg.Ack() // Acknowledge immediately

	var payload IngestedMessagePayload
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		logger.Error().Err(err).Str("topic_source", topicSource).Str("msg_id", msg.ID).Msg("Failed to unmarshal message data")
		return
	}

	// Ensure critical fields for latency calculation are present
	// The field from messenger.MQTTMessage is MessageTimestamp, which should be mapped to
	// connectors.EnrichedMessage.OriginalClientTimestamp or similar.
	// Here, IngestedMessagePayload expects `MessageTimestamp` to be that original client time.
	if payload.OriginalClientTimestamp.IsZero() {
		logger.Warn().Str("topic_source", topicSource).Str("msg_id", msg.ID).Str("client_msg_id", payload.ClientMessageID).Msg("Received message with zero OriginalClientTimestamp (payload.MessageTimestamp), cannot calculate latency accurately.")
		// Depending on requirements, you might still want to count this message,
		// but latency calculations will be invalid.
		// We don't do any processing on unidentified messages so they'll end up here
	}
	if payload.ClientMessageID == "" {
		logger.Warn().Str("topic_source", topicSource).Str("msg_id", msg.ID).Time("original_ts", payload.MessageTimestamp).Msg("Received message with empty ClientMessageID.")
		// Continue processing but this might affect result matching if ClientMessageID is key.
	}

	receiveTime := time.Now().UTC()
	latency := receiveTime.Sub(payload.MessageTimestamp)
	processingLatency := time.Duration(0) // Default to 0 if PublishTime is zero

	// msg.PublishTime is when Pub/Sub received the message.
	// This is a good indicator of when the ingestion service published it.
	if !msg.PublishTime.IsZero() {
		processingLatency = msg.PublishTime.Sub(payload.MessageTimestamp)
	} else {
		logger.Warn().Str("client_msg_id", payload.ClientMessageID).Msg("Pub/Sub message PublishTime is zero.")
	}

	record := MessageVerificationRecord{
		ClientMessageID:         payload.ClientMessageID,
		DeviceEUI:               payload.DeviceEUI,
		TopicSource:             topicSource,
		OriginalClientTimestamp: payload.MessageTimestamp, // This is key for latency
		VerifierReceiveTime:     receiveTime,
		PubSubPublishTime:       msg.PublishTime,
		LatencyMillis:           latency.Milliseconds(),
		ProcessingLatencyMillis: processingLatency.Milliseconds(),
	}

	recordsLock.Lock()
	log.Info().Str("source", topicSource).Str("client_msg_id", payload.ClientMessageID).Msg("Received message")
	*records = append(*records, record)
	if topicSource == "enriched" {
		(*enrichedCounter)++
	} else if topicSource == "unidentified" {
		(*unidentifiedCounter)++
	}
	recordsLock.Unlock()

	logger.Debug().Str("client_msg_id", record.ClientMessageID).Str("topic", topicSource).Int64("latency_ms", record.LatencyMillis).Msg("Message verified")
}

// runSubscriptionReceiver starts a goroutine to receive messages from a given Pub/Sub subscription.
// It's called by Verifier.Run for each subscription.
func runSubscriptionReceiver(
	ctx context.Context, // This context is controlled by Verifier.Run (e.g., for TestDuration)
	wg *sync.WaitGroup,
	sub *pubsub.Subscription,
	topicSource string,
	logger zerolog.Logger,
	records *[]MessageVerificationRecord, // Pointer to the shared slice in Verifier
	enrichedCounter *int, // Pointer to the shared counter in Verifier
	unidentifiedCounter *int, // Pointer to the shared counter in Verifier
	recordsLock *sync.Mutex, // Pointer to the shared mutex in Verifier
) {
	defer wg.Done()
	logger.Info().Str("subscription_id", sub.ID()).Str("topic_source", topicSource).Msg("Starting message receiver...")

	// sub.Receive will block until its context is cancelled or an unrecoverable error occurs.
	err := sub.Receive(ctx, func(ctxMsg context.Context, msg *pubsub.Message) { // ctxMsg is per-message
		// Note: The context passed to the Receive callback (ctxMsg) is different from the
		// overall receiver context (ctx). The callback should be relatively quick.
		processMessage(msg, topicSource, logger, records, enrichedCounter, unidentifiedCounter, recordsLock)
	})

	// Don't log error if context was canceled, as that's the expected way to stop.
	// Also, DeadlineExceeded is often a result of context cancellation.
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		logger.Error().Err(err).Str("subscription_id", sub.ID()).Str("topic_source", topicSource).Msg("Subscription Receive error")
	}
	logger.Info().Str("subscription_id", sub.ID()).Str("topic_source", topicSource).Msg("Message receiver stopped.")
}

// createTemporarySubscription creates a new temporary Pub/Sub subscription.
// It's called by Verifier.Run.
func createTemporarySubscription(
	ctx context.Context, // Context for the creation operation itself
	client *pubsub.Client,
	topicID,
	subNamePrefix string,
	logger zerolog.Logger,
) (*pubsub.Subscription, error) {
	topic := client.Topic(topicID)
	// Check if topic exists before trying to create a subscription.
	existsCtx, cancelExists := context.WithTimeout(ctx, 10*time.Second) // Short timeout for exists check
	defer cancelExists()
	exists, err := topic.Exists(existsCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic %s exists: %w", topicID, err)
	}
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist in project %s", topicID, client.Project())
	}

	// Generate a unique subscription MessageID to avoid conflicts.
	subID := fmt.Sprintf("%s-%s", subNamePrefix, uuid.NewString())

	subConfig := pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second, // How long to wait for an ack before redelivering.
		RetryPolicy: &pubsub.RetryPolicy{ // How to handle delivery failures.
			MinimumBackoff: 100 * time.Millisecond, // Default is 10s, too long for tests.
			MaximumBackoff: 10 * time.Second,       // Default is 600s.
		},
		// MessageRetentionDuration: 10 * time.Minute, // Default is 7 days. Min is 10 mins.
		// ExpirationPolicy: 24 * time.Hour, // Auto-delete if unused. Explicit deletion is better for tests.
	}

	// Use the creation context for CreateSubscription
	createSubCtx, cancelCreate := context.WithTimeout(ctx, 30*time.Second) // Timeout for the create operation
	defer cancelCreate()
	sub, err := client.CreateSubscription(createSubCtx, subID, subConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription %s for topic %s: %w", subID, topicID, err)
	}
	logger.Info().Str("subscription_id", sub.ID()).Str("topic_id", topicID).Msg("Temporary subscription created")
	return sub, nil
}

// deleteSubscription attempts to delete a Pub/Sub subscription.
// It's called by Verifier.Run (via defer) and Verifier.Stop.
func deleteSubscription(
	ctx context.Context, // Context for the deletion operation
	sub *pubsub.Subscription,
	logger zerolog.Logger,
) {
	if sub == nil {
		return
	}
	subID := sub.ID() // Capture before sub might become invalid after Delete
	logger.Info().Str("subscription_id", subID).Msg("Attempting to delete temporary subscription...")

	// Use the deletion context for Delete
	deleteCtx, cancelDelete := context.WithTimeout(ctx, 30*time.Second) // Timeout for the delete operation
	defer cancelDelete()
	if err := sub.Delete(deleteCtx); err != nil {
		// It's common for Delete to fail if the context used to create the client or subscription
		// has already been cancelled. Using a background context for deferred cleanup helps,
		// but if that context also times out, deletion might fail.
		// Also, check if the error is "not found" which can happen if it was already deleted or expired.
		// This requires checking the specific error type or message from the Pub/Sub client library.
		logger.Error().Err(err).Str("subscription_id", subID).Msg("Failed to delete temporary subscription")
	} else {
		logger.Info().Str("subscription_id", subID).Msg("Temporary subscription deleted successfully")
	}
}
