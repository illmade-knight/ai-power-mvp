package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// --- ConsumedMessage & MessageConsumer Interface (Ideally in a shared package) ---

type ConsumedMessage struct {
	ID          string
	Payload     []byte
	PublishTime time.Time
	Ack         func()
	Nack        func()
}

type MessageConsumer interface {
	Messages() <-chan ConsumedMessage
	Start(ctx context.Context) error
	Stop() error
	Done() <-chan struct{}
}

// --- Device Processing Service ---

type ServiceConfig struct {
	UpstreamSubscriptionEnvVar string
	NumProcessingWorkers       int
	BatchSize                  int
	FlushTimeout               time.Duration
}

func LoadServiceConfigFromEnv() (*ServiceConfig, error) {
	upstreamSub := os.Getenv("PUBSUB_SUBSCRIPTION_ID_GARDEN_MONITOR_INPUT")
	cfg := &ServiceConfig{
		UpstreamSubscriptionEnvVar: upstreamSub,
		NumProcessingWorkers:       5,
		BatchSize:                  100,
		FlushTimeout:               5 * time.Second,
	}
	return cfg, nil
}

type ProcessingService struct {
	config        *ServiceConfig
	consumer      MessageConsumer
	batchInserter *BatchInserter
	logger        zerolog.Logger
	wg            sync.WaitGroup
	shutdownCtx   context.Context
	shutdownFunc  context.CancelFunc
}

func NewProcessingService(
	ctx context.Context,
	config *ServiceConfig,
	inserter DecodedDataBatchInserter,
	logger zerolog.Logger,
) (*ProcessingService, error) {
	consumerCfg, err := LoadGooglePubSubConsumerConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to load device input consumer config: %w", err)
	}
	consumer, err := NewGooglePubSubConsumer(ctx, consumerCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create device input message consumer: %w", err)
	}

	batchInserterConfig := &BatchInserterConfig{
		BatchSize:    config.BatchSize,
		FlushTimeout: config.FlushTimeout,
	}
	batchInserter := NewBatchInserter(batchInserterConfig, inserter, logger)

	shutdownCtx, shutdownFunc := context.WithCancel(context.Background())

	return &ProcessingService{
		config:        config,
		consumer:      consumer,
		batchInserter: batchInserter,
		logger:        logger.With().Str("service", "ProcessingService").Logger(),
		shutdownCtx:   shutdownCtx,
		shutdownFunc:  shutdownFunc,
	}, nil
}

func (s *ProcessingService) Start() error {
	s.logger.Info().Msg("Starting ProcessingService...")
	s.batchInserter.Start()

	if err := s.consumer.Start(s.shutdownCtx); err != nil {
		return fmt.Errorf("failed to start message consumer: %w", err)
	}
	s.logger.Info().Msg("Message consumer started.")

	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Debug().Int("worker_id", workerID).Msg("Device processing worker started.")
			for {
				select {
				case <-s.shutdownCtx.Done():
					s.logger.Info().Int("worker_id", workerID).Msg("Device processing worker shutting down.")
					return
				case msg, ok := <-s.consumer.Messages():
					if !ok {
						s.logger.Info().Int("worker_id", workerID).Msg("Consumer channel closed, worker exiting.")
						return
					}
					s.processConsumedMessage(msg, workerID)
				}
			}
		}(i)
	}
	s.logger.Info().Msg("ProcessingService started with workers.")
	return nil
}

func (s *ProcessingService) processConsumedMessage(msg ConsumedMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Str("msg_id", msg.ID).Msg("Processing consumed message")

	var upstreamMsg GardenMonitorMessage
	if err := json.Unmarshal(msg.Payload, &upstreamMsg); err != nil {
		s.logger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to unmarshal Pub/Sub payload, Nacking.")
		msg.Nack()
		return
	}

	if upstreamMsg.Payload == nil {
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Consumed message has empty RawPayload, Acking and skipping.")
		msg.Ack()
		return
	}

	batchedMsg := &BatchedMessage{
		OriginalMessage: msg,
		Payload:         upstreamMsg.Payload,
	}

	select {
	case s.batchInserter.Input() <- batchedMsg:
		s.logger.Debug().Str("msg_id", msg.ID).Msg("Payload sent to batch inserter.")
	case <-s.shutdownCtx.Done():
		s.logger.Warn().Str("msg_id", msg.ID).Msg("Shutdown in progress, Nacking message.")
		msg.Nack()
	}
}

// Stop gracefully shuts down the ProcessingService.
func (s *ProcessingService) Stop() {
	s.logger.Info().Msg("Stopping ProcessingService...")

	// 1. Signal all workers to stop and prevent new messages from being consumed.
	s.shutdownFunc()

	// 2. Stop the consumer. This will close the Messages() channel.
	s.logger.Info().Msg("Stopping message consumer...")
	if err := s.consumer.Stop(); err != nil {
		s.logger.Error().Err(err).Msg("Error stopping message consumer")
	}
	<-s.consumer.Done()
	s.logger.Info().Msg("Message consumer stopped.")

	// 3. Wait for all processing workers to finish. They will exit after the consumer channel is closed.
	// This ensures any in-flight messages are sent to the batcher before we stop it.
	s.logger.Info().Msg("Waiting for processing workers to complete...")
	s.wg.Wait()
	s.logger.Info().Msg("All processing workers completed.")

	// 4. Now that all messages are guaranteed to be in the batcher, stop it.
	// This will trigger its final flush.
	s.batchInserter.Stop()

	s.logger.Info().Msg("ProcessingService stopped.")
}
