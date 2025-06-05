package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// ErrMetadataNotFound is an error returned when metadata for a device cannot be found.
var ErrMetadataNotFound = errors.New("device metadata not found")

// ErrInvalidMessage is an error for when an incoming MQTT message is invalid for processing.
var ErrInvalidMessage = errors.New("invalid MQTT message for processing")

// EnrichmentService consumes messages from an input Pub/Sub topic,
// enriches them using a metadata fetcher, and publishes them to
// appropriate output Pub/Sub topics.
type EnrichmentService struct {
	config EnrichmentServiceConfig
	logger zerolog.Logger

	inputSubClient        *pubsub.Client       // Client for the input subscription
	inputSubscription     *pubsub.Subscription // Input Pub/Sub subscription
	metadataFetcher       DeviceMetadataFetcher
	enrichedPublisher     MessagePublisher // For successfully enriched messages
	unidentifiedPublisher MessagePublisher // For messages from unidentified devices

	messagesChan chan *pubsub.Message // Channel to buffer messages from Pub/Sub for processing
	errorChan    chan error           // For propagating critical errors from the service

	cancelCtx  context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	closeErrorChanOnce sync.Once
}

// NewEnrichmentService creates a new EnrichmentService.
func NewEnrichmentService(
	ctx context.Context,
	cfg EnrichmentServiceConfig,
	inputSubCfg PubSubSubscriptionConfig,
	fetcher DeviceMetadataFetcher,
	enrichedPub *GooglePubSubPublisher,
	unidentifiedPub *GooglePubSubPublisher,
	logger zerolog.Logger,
) (*EnrichmentService, error) {

	serviceCtx, serviceCancel := context.WithCancel(ctx)

	// Create Pub/Sub client for the input subscription
	// PUBSUB_EMULATOR_HOST will be respected if set in environment
	var inputClientOpts []option.ClientOption
	if pubsubEmulatorHost := os.Getenv("PUBSUB_EMULATOR_HOST"); pubsubEmulatorHost != "" {
		logger.Info().Str("emulator_host", pubsubEmulatorHost).Msg("EnrichmentService: Using Pub/Sub emulator for input subscription.")
		inputClientOpts = append(inputClientOpts, option.WithEndpoint(pubsubEmulatorHost), option.WithoutAuthentication())
	}
	inputClient, err := pubsub.NewClient(serviceCtx, inputSubCfg.ProjectID, inputClientOpts...)
	if err != nil {
		serviceCancel()
		return nil, fmt.Errorf("failed to create Pub/Sub client for input subscription: %w", err)
	}

	inputSub := inputClient.Subscription(inputSubCfg.SubscriptionID)
	// Configure receive settings
	inputSub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxOutstandingMessages
	inputSub.ReceiveSettings.NumGoroutines = cfg.NumProcessingWorkers // Can align with worker pool or manage separately

	return &EnrichmentService{
		config:                cfg,
		logger:                logger,
		inputSubClient:        inputClient,
		inputSubscription:     inputSub,
		metadataFetcher:       fetcher,
		enrichedPublisher:     enrichedPub,
		unidentifiedPublisher: unidentifiedPub,
		messagesChan:          make(chan *pubsub.Message, cfg.InputChanCapacity),
		errorChan:             make(chan error, cfg.InputChanCapacity), // Consistent capacity
		cancelCtx:             serviceCtx,
		cancelFunc:            serviceCancel,
	}, nil
}

// Start begins the message processing workers and the Pub/Sub subscription.
func (s *EnrichmentService) Start() error {
	s.logger.Info().
		Int("workers", s.config.NumProcessingWorkers).
		Str("subscription", s.inputSubscription.ID()).
		Msg("Starting EnrichmentService...")

	// Start worker goroutines
	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Info().Int("worker_id", workerID).Msg("Starting enrichment worker")
			for {
				select {
				case <-s.cancelCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Enrichment worker shutting down due to context cancellation")
					return
				case pubsubMsg, ok := <-s.messagesChan:
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("MessagesChan closed, enrichment worker stopping")
						return
					}
					s.processPubSubMessage(s.cancelCtx, pubsubMsg, workerID)
				}
			}
		}(i)
	}

	// Start receiving messages from Pub/Sub
	s.wg.Add(1) // Add to WaitGroup for the Receive goroutine
	go func() {
		defer s.wg.Done() // Ensure this Done is called when Receive exits
		s.logger.Info().Str("subscription_id", s.inputSubscription.ID()).Msg("Starting Pub/Sub message receiver...")
		err := s.inputSubscription.Receive(s.cancelCtx, func(ctx context.Context, msg *pubsub.Message) {
			// This callback should be lightweight: push to channel for worker processing.
			// The context passed here (ctx) is for this specific message's lifecycle.
			s.logger.Debug().Str("pubsub_msg_id", msg.ID).Int("data_len", len(msg.Data)).Msg("Received message from Pub/Sub")
			select {
			case s.messagesChan <- msg:
				// Message successfully pushed to internal channel. Acking will be handled by worker.
			case <-s.cancelCtx.Done():
				s.logger.Warn().Str("pubsub_msg_id", msg.ID).Msg("Service shutting down, Nacking Pub/Sub message (will be redelivered)")
				msg.Nack() // Nack if service is shutting down during push
			case <-ctx.Done(): // If the message context itself is cancelled (e.g. ack deadline exceeded before push)
				s.logger.Warn().Str("pubsub_msg_id", msg.ID).Err(ctx.Err()).Msg("Pub/Sub message context done before processing, Nacking.")
				msg.Nack()
			default:
				// This case means messagesChan is full. This indicates a processing bottleneck.
				s.logger.Error().Str("pubsub_msg_id", msg.ID).Msg("messagesChan is full. Nacking Pub/Sub message. Consider increasing capacity or worker count.")
				msg.Nack() // Nack the message so it can be redelivered
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error().Err(err).Str("subscription_id", s.inputSubscription.ID()).Msg("Pub/Sub Receive() returned an error")
			s.sendError(fmt.Errorf("pubsub subscription %s Receive error: %w", s.inputSubscription.ID(), err))
			// If Receive fails critically, we might need to signal a full shutdown
			s.cancelFunc() // Trigger shutdown of workers etc.
		}
		s.logger.Info().Str("subscription_id", s.inputSubscription.ID()).Msg("Pub/Sub message receiver stopped.")
	}()

	s.logger.Info().Msg("EnrichmentService started successfully.")
	return nil
}

// processPubSubMessage handles parsing, enrichment, and publishing for one Pub/Sub message.
// It also handles Ack/Nack for the Pub/Sub message.
func (s *EnrichmentService) processPubSubMessage(ctx context.Context, pubsubMsg *pubsub.Message, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("pubsub_msg_id", pubsubMsg.ID).Msg("Processing Pub/Sub message")

	var mqttMsg MQTTMessage // Expecting the payload to be the MQTTMessage struct from the previous service
	if err := json.Unmarshal(pubsubMsg.Data, &mqttMsg); err != nil {
		s.logger.Error().Err(err).Str("pubsub_msg_id", pubsubMsg.ID).Msg("Failed to unmarshal Pub/Sub message data into MQTTMessage")
		// Consider sending to a dead-letter topic or logging extensively.
		// For now, Acking as it's unprocessable in its current form.
		pubsubMsg.Ack()
		s.sendError(fmt.Errorf("unmarshal error for msg %s: %w", pubsubMsg.ID, err))
		return
	}

	// Enrich the data
	enrichedMsg, err := EnrichMQTTData(&mqttMsg, s.metadataFetcher, s.logger) // EnrichMQTTData is from enrich.go

	publishCtx, publishCancel := context.WithTimeout(ctx, 30*time.Second) // Timeout for publishing operations
	defer publishCancel()

	if err != nil {
		if errors.Is(err, ErrMetadataNotFound) { // Assuming ErrMetadataNotFound is defined in this package
			s.logger.Warn().Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Str("pubsub_msg_id", pubsubMsg.ID).Msg("Device metadata not found, publishing to unidentified topic")
			unidentifiedPayload := &UnidentifiedDeviceMessage{
				DeviceEUI:          mqttMsg.DeviceInfo.DeviceEUI,
				RawPayload:         mqttMsg.RawPayload,
				OriginalMQTTTime:   mqttMsg.MessageTimestamp, // Assuming this is the original timestamp
				LoRaWANReceivedAt:  mqttMsg.LoRaWAN.ReceivedAt,
				IngestionTimestamp: time.Now().UTC(),
				ProcessingError:    err.Error(),
			}
			if pubErr := s.unidentifiedPublisher.PublishUnidentified(publishCtx, unidentifiedPayload); pubErr != nil {
				s.logger.Error().Err(pubErr).Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Msg("Failed to publish to unidentified topic")
				s.sendError(pubErr)
				pubsubMsg.Nack() // Nack if publish fails, so it can be retried
				return
			}
		} else {
			s.logger.Error().Err(err).Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).Str("pubsub_msg_id", pubsubMsg.ID).Msg("Error enriching MQTT data")
			s.sendError(err)
			pubsubMsg.Nack() // Nack on other enrichment errors for potential retry
			return
		}
	} else {
		s.logger.Debug().Str("device_eui", enrichedMsg.DeviceEUI).Str("pubsub_msg_id", pubsubMsg.ID).Msg("Successfully enriched, publishing to enriched topic")
		if pubErr := s.enrichedPublisher.PublishEnriched(publishCtx, enrichedMsg); pubErr != nil {
			s.logger.Error().Err(pubErr).Str("device_eui", enrichedMsg.DeviceEUI).Msg("Failed to publish to enriched topic")
			s.sendError(pubErr)
			pubsubMsg.Nack() // Nack if publish fails
			return
		}
	}

	pubsubMsg.Ack() // Ack the message once processed and published (or deliberately handled)
	s.logger.Debug().Str("pubsub_msg_id", pubsubMsg.ID).Msg("Successfully processed and Acked Pub/Sub message")
}

// Stop gracefully shuts down the EnrichmentService.
func (s *EnrichmentService) Stop() {
	s.logger.Info().Msg("Stopping EnrichmentService...")

	s.cancelFunc() // Signal Pub/Sub Receive loop and workers to stop

	// Wait for the Pub/Sub Receive goroutine and all worker goroutines to complete.
	s.wg.Wait()
	s.logger.Info().Msg("All worker and Pub/Sub receiver goroutines completed.")

	// Close messagesChan after workers and receiver are confirmed stopped.
	if s.messagesChan != nil {
		// Check if it's already closed, though s.wg.Wait() should ensure producers are stopped.
		// For safety, can wrap in a recover or ensure it's not closed elsewhere.
		// However, with proper use of cancelCtx and wg, this should be safe.
		close(s.messagesChan)
		s.logger.Info().Msg("messagesChan closed.")
	}

	// Stop the output publishers (they handle their own client closing)
	if s.enrichedPublisher != nil {
		s.enrichedPublisher.Stop()
	}
	if s.unidentifiedPublisher != nil {
		s.unidentifiedPublisher.Stop()
	}

	// Close the input Pub/Sub client
	if s.inputSubClient != nil {
		if err := s.inputSubClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing input Pub/Sub client")
		} else {
			s.logger.Info().Msg("Input Pub/Sub client closed.")
		}
	}

	// Close errorChan
	if s.errorChan != nil {
		s.closeErrorChanOnce.Do(func() {
			s.logger.Info().Msg("Closing ErrorChan in EnrichmentService Stop()...")
			close(s.errorChan)
			s.logger.Info().Msg("ErrorChan closed in EnrichmentService Stop().")
		})
	}

	s.logger.Info().Msg("EnrichmentService stopped.")
}

// sendError attempts to send an error to the errorChan.
func (s *EnrichmentService) sendError(err error) {
	if s.errorChan == nil {
		return
	}
	select {
	case s.errorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("errorChan is full, dropping error")
	}
}

// ErrorChannel returns the error channel of the service.
func (s *EnrichmentService) ErrorChannel() <-chan error {
	return s.errorChan
}
