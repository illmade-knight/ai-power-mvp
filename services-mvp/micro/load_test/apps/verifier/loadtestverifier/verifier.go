package loadtestverifier

import (
	"context"
	"errors" // Added for NewVerifier error
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// --- Verifier Logic (from former verifier.go) ---

// Verifier orchestrates the message verification process.
type Verifier struct {
	config VerifierConfig
	logger zerolog.Logger

	// Internal state for collecting results
	recordsLock         sync.Mutex
	verifiedRecords     []MessageVerificationRecord
	enrichedCount       int
	unidentifiedCount   int
	startTime           time.Time
	pubsubClient        *pubsub.Client // Made this specific type, can be interface if more flexibility needed later
	enrichedSub         *pubsub.Subscription
	unidentifiedSub     *pubsub.Subscription
	activeSubscriptions bool
}

// NewVerifier creates a new Verifier instance.
func NewVerifier(cfg VerifierConfig, baseLogger zerolog.Logger) (*Verifier, error) {
	if cfg.ProjectID == "" {
		return nil, errors.New("projectID is required in VerifierConfig")
	}
	if cfg.EnrichedTopicID == "" {
		return nil, errors.New("enrichedTopicID is required in VerifierConfig")
	}
	if cfg.UnidentifiedTopicID == "" {
		return nil, errors.New("unidentifiedTopicID is required in VerifierConfig")
	}
	if cfg.TestDuration <= 0 {
		return nil, errors.New("testDuration must be positive")
	}

	vLogger := baseLogger.With().Str("component", "LoadTestVerifier").Logger()
	if cfg.LogLevel != "" {
		level, err := zerolog.ParseLevel(cfg.LogLevel)
		if err == nil {
			vLogger = vLogger.Level(level)
		} else {
			vLogger.Warn().Err(err).Str("log_level", cfg.LogLevel).Msg("Invalid log level for verifier, using base logger's level")
		}
	}

	return &Verifier{
		config: cfg,
		logger: vLogger,
	}, nil
}

// Run starts the verification process, listening to Pub/Sub topics for the configured duration.
// It returns the aggregated test results or an error if setup fails.
func (v *Verifier) Run(ctx context.Context) (*TestRunResults, error) {
	v.logger.Info().Interface("config", v.config).Msg("Pub/Sub Verifier starting run")
	v.startTime = time.Now().UTC()

	var runCancel context.CancelFunc
	deadline, _ := ctx.Deadline()
	if _, ok := ctx.Deadline(); !ok || time.Until(deadline) > v.config.TestDuration {
		v.logger.Info().Dur("verifier_duration", v.config.TestDuration).Msg("Using configured TestDuration to control verifier run time.")
		ctx, runCancel = context.WithTimeout(ctx, v.config.TestDuration)
	} else {
		v.logger.Info().Time("parent_ctx_deadline", deadline).Msg("Using parent context's deadline to control verifier run time.")
		ctx, runCancel = context.WithCancel(ctx)
	}
	defer runCancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		v.logger.Warn().Str("signal", sig.String()).Msg("Received OS shutdown signal. Terminating verification...")
		runCancel()
	}()

	var clientOpts []option.ClientOption
	if v.config.EmulatorHost != "" {
		v.logger.Info().Str("host", v.config.EmulatorHost).Msg("Using Pub/Sub emulator for verifier")
		clientOpts = append(clientOpts, option.WithEndpoint(v.config.EmulatorHost), option.WithoutAuthentication())
	} else {
		v.logger.Info().Msg("Using live Pub/Sub for verifier")
		clientOpts = append(clientOpts, v.config.ClientOpts...)
	}

	var err error
	v.pubsubClient, err = pubsub.NewClient(ctx, v.config.ProjectID, clientOpts...)
	if err != nil {
		v.logger.Error().Err(err).Msg("Failed to create Pub/Sub client")
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}
	defer v.pubsubClient.Close()

	v.enrichedSub, err = createTemporarySubscription(ctx, v.pubsubClient, v.config.EnrichedTopicID, "verifier-enriched", v.logger)
	if err != nil {
		v.logger.Error().Err(err).Msg("Failed to create subscription for enriched topic")
		return nil, err
	}
	defer deleteSubscription(context.Background(), v.enrichedSub, v.logger)

	v.unidentifiedSub, err = createTemporarySubscription(ctx, v.pubsubClient, v.config.UnidentifiedTopicID, "verifier-unidentified", v.logger)
	if err != nil {
		v.logger.Error().Err(err).Msg("Failed to create subscription for unidentified topic")
		return nil, err
	}
	defer deleteSubscription(context.Background(), v.unidentifiedSub, v.logger)
	v.activeSubscriptions = true

	var wg sync.WaitGroup
	wg.Add(2)
	go runSubscriptionReceiver(ctx, &wg, v.enrichedSub, "enriched", v.logger, &v.verifiedRecords, &v.enrichedCount, &v.unidentifiedCount, &v.recordsLock)
	go runSubscriptionReceiver(ctx, &wg, v.unidentifiedSub, "unidentified", v.logger, &v.verifiedRecords, &v.enrichedCount, &v.unidentifiedCount, &v.recordsLock)

	v.logger.Info().Msg("Message receivers started. Verifying messages for duration...")
	<-ctx.Done()
	v.logger.Info().Msg("Verification run context ended (duration elapsed or shutdown signal/cancel).")

	v.logger.Info().Msg("Waiting for message receivers to complete...")
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		v.logger.Info().Msg("All message receivers finished gracefully.")
	case <-time.After(10 * time.Second):
		v.logger.Warn().Msg("Timeout waiting for message receivers to finish. Some messages might be lost.")
	}
	v.activeSubscriptions = false

	endTime := time.Now().UTC()
	v.recordsLock.Lock()
	finalResults := TestRunResults{
		TestRunID:             v.config.TestRunID,
		StartTime:             v.startTime,
		EndTime:               endTime,
		ActualDuration:        endTime.Sub(v.startTime).String(),
		RequestedDuration:     v.config.TestDuration.String(),
		MachineInfo:           v.config.MachineInfo,
		EnvironmentName:       v.config.EnvironmentName,
		ProjectID:             v.config.ProjectID,
		EnrichedTopicID:       v.config.EnrichedTopicID,
		UnidentifiedTopicID:   v.config.UnidentifiedTopicID,
		TotalMessagesVerified: len(v.verifiedRecords),
		EnrichedMessages:      v.enrichedCount,
		UnidentifiedMessages:  v.unidentifiedCount,
		Records:               v.verifiedRecords,
	}
	v.recordsLock.Unlock()

	if v.config.OutputFile != "" {
		// Pass the verifier's logger to SaveResults
		if err := finalResults.SaveResults(v.config.OutputFile, v.logger); err != nil {
			// Error is already logged by SaveResults
		}
	}

	v.logger.Info().Int("total_verified", finalResults.TotalMessagesVerified).Msg("Pub/Sub Verifier run finished.")
	return &finalResults, nil
}

// Stop can be called to gracefully attempt to shutdown the verifier.
func (v *Verifier) Stop() {
	v.logger.Info().Msg("Stop called on Verifier.")
	if v.activeSubscriptions {
		v.logger.Warn().Msg("Stop called while subscriptions might still be considered active by the Verifier. Attempting cleanup.")
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if v.enrichedSub != nil {
			deleteSubscription(cleanupCtx, v.enrichedSub, v.logger)
		}
		if v.unidentifiedSub != nil {
			deleteSubscription(cleanupCtx, v.unidentifiedSub, v.logger)
		}
		v.activeSubscriptions = false
	}
	if v.pubsubClient != nil {
		v.pubsubClient.Close()
	}
}

// VerifierConfig holds the configuration for the verifier.
type VerifierConfig struct {
	ProjectID           string
	EnrichedTopicID     string
	UnidentifiedTopicID string
	TestDuration        time.Duration // Renamed from Duration for clarity within the package
	OutputFile          string
	TestRunID           string
	MachineInfo         string
	EnvironmentName     string
	ClientOpts          []option.ClientOption // For GCP client
	LogLevel            string                // Added for programmatic log level setting
	EmulatorHost        string                // Added for programmatic emulator setting
}
