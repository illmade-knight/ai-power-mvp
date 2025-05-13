package ingestion

import (
	"context"
	"errors"
	"github.com/rs/zerolog"
	"sync"
)

// ErrMetadataNotFound is an error returned when metadata for a device cannot be found.
var ErrMetadataNotFound = errors.New("device metadata not found")

// ErrInvalidMessage is an error for when an incoming MQTT message is invalid for processing.
var ErrInvalidMessage = errors.New("invalid MQTT message for processing")

// IngestionServiceConfig holds configuration for the IngestionService.
type IngestionServiceConfig struct {
	InputChanCapacity    int // Capacity of the input channel for raw messages
	OutputChanCapacity   int // Capacity of the output channel for enriched messages
	NumProcessingWorkers int // Number of goroutines to process messages concurrently
}

// DefaultIngestionServiceConfig provides sensible defaults.
func DefaultIngestionServiceConfig() IngestionServiceConfig {
	return IngestionServiceConfig{
		InputChanCapacity:    100,
		OutputChanCapacity:   100,
		NumProcessingWorkers: 5, // Adjust based on expected load and processing time
	}
}

// IngestionService processes raw messages from an input channel, enriches them,
// and sends them to an output channel.
type IngestionService struct {
	config     IngestionServiceConfig
	fetcher    DeviceMetadataFetcher
	logger     zerolog.Logger
	wg         sync.WaitGroup
	cancelCtx  context.Context    // Used to signal shutdown to workers
	cancelFunc context.CancelFunc // Function to call to initiate shutdown

	// Public channels for interaction
	RawMessagesChan      chan []byte           // Input: Raw JSON messages
	EnrichedMessagesChan chan *EnrichedMessage // Output: Successfully enriched messages
	ErrorChan            chan error            // Output: Errors encountered during processing (optional, for observability)
}

// NewIngestionService creates and initializes a new IngestionService.
func NewIngestionService(
	fetcher DeviceMetadataFetcher,
	logger zerolog.Logger,
	cfg IngestionServiceConfig,
) *IngestionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &IngestionService{
		config:               cfg,
		fetcher:              fetcher,
		logger:               logger,
		cancelCtx:            ctx,
		cancelFunc:           cancel,
		RawMessagesChan:      make(chan []byte, cfg.InputChanCapacity),
		EnrichedMessagesChan: make(chan *EnrichedMessage, cfg.OutputChanCapacity),
		ErrorChan:            make(chan error, cfg.InputChanCapacity), // Buffer errors too
	}
}

// Start begins the message processing loop(s) for the IngestionService.
// It's non-blocking. Call Stop() to gracefully shut down.
func (s *IngestionService) Start() {
	s.logger.Info().Int("workers", s.config.NumProcessingWorkers).Msg("Starting IngestionService...")

	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Debug().Int("worker_id", workerID).Msg("Starting processing worker")
			for {
				select {
				case <-s.cancelCtx.Done(): // Shutdown signal
					s.logger.Info().Int("worker_id", workerID).Msg("Processing worker shutting down")
					return
				case rawMsg, ok := <-s.RawMessagesChan:
					if !ok { // Channel closed
						s.logger.Info().Int("worker_id", workerID).Msg("RawMessagesChan closed, worker stopping")
						return
					}
					s.processSingleMessage(rawMsg, workerID)
				}
			}
		}(i)
	}
	s.logger.Info().Msg("IngestionService started")
}

// processSingleMessage handles parsing and enrichment of one raw message.
func (s *IngestionService) processSingleMessage(rawMsg []byte, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Int("msg_size_bytes", len(rawMsg)).Msg("Received raw message for processing")

	mqttMsg, err := ParseMQTTMessage(rawMsg)
	if err != nil {
		s.logger.Error().
			Int("worker_id", workerID).
			Err(err).
			Str("raw_message_snippet", string(rawMsg[:min(len(rawMsg), 100)])). // Log a snippet
			Msg("Failed to parse raw MQTT message")
		s.sendError(err) // Optionally send parse errors to ErrorChan
		return
	}

	enrichedMsg, err := EnrichMQTTData(mqttMsg, s.fetcher, s.logger)
	if err != nil {
		// EnrichMQTTData already logs detailed errors.
		// We might just log that enrichment failed at this level or send to ErrorChan.
		s.logger.Warn().
			Int("worker_id", workerID).
			Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).
			Err(err).
			Msg("Failed to enrich MQTT message")
		s.sendError(err) // Optionally send enrichment errors to ErrorChan
		return
	}

	// Try to send to EnrichedMessagesChan, but handle potential shutdown.
	select {
	case s.EnrichedMessagesChan <- enrichedMsg:
		s.logger.Debug().Int("worker_id", workerID).Str("device_eui", enrichedMsg.DeviceEUI).Msg("Successfully processed and sent enriched message")
	case <-s.cancelCtx.Done():
		s.logger.Warn().Int("worker_id", workerID).Str("device_eui", enrichedMsg.DeviceEUI).Msg("Shutdown signaled while trying to send enriched message")
	}
}

// sendError attempts to send an error to the ErrorChan if it's non-nil and there's capacity.
func (s *IngestionService) sendError(err error) {
	if s.ErrorChan == nil {
		return
	}
	// Non-blocking send to error channel
	select {
	case s.ErrorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("ErrorChan is full or nil, dropping error")
	}
}

// Stop gracefully shuts down the IngestionService.
// It closes the RawMessagesChan to signal workers to finish processing in-flight messages,
// then waits for all processing goroutines to complete.
func (s *IngestionService) Stop() {
	s.logger.Info().Msg("Stopping IngestionService...")

	// Signal workers to stop by cancelling the context.
	// This is preferred over just closing RawMessagesChan if workers might be blocked on other things.
	s.cancelFunc()

	// Close RawMessagesChan to stop new messages from being accepted if Start was called.
	// Workers will also exit if they read from a closed channel.
	// Ensure this is idempotent or check if already closed if Stop can be called multiple times.
	// For simplicity here, we assume Start was called and channel is open.
	close(s.RawMessagesChan)

	s.wg.Wait() // Wait for all processing goroutines to finish

	// Close output channels after all workers are done to signal consumers.
	if s.EnrichedMessagesChan != nil {
		close(s.EnrichedMessagesChan)
	}
	if s.ErrorChan != nil {
		close(s.ErrorChan)
	}
	s.logger.Info().Msg("IngestionService stopped")
}
