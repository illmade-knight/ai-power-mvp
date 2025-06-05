package connectors

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// EnrichmentServiceConfig holds all configuration for the EnrichmentService.
type EnrichmentServiceConfig struct {
	InputSubscriptionConfig PubSubSubscriptionConfig
	NumProcessingWorkers    int // Number of goroutines to process messages concurrently
	InputChanCapacity       int // Capacity of the channel buffering messages from Pub/Sub
	MaxOutstandingMessages  int // Pub/Sub subscriber setting: max unprocessed messages
}

// PubSubSubscriptionConfig holds configuration for a single Pub/Sub subscription.
type PubSubSubscriptionConfig struct {
	ProjectID      string
	SubscriptionID string
}

// DefaultEnrichmentServiceConfig provides sensible defaults.
func DefaultEnrichmentServiceConfig() EnrichmentServiceConfig {
	return EnrichmentServiceConfig{
		NumProcessingWorkers:   5,
		InputChanCapacity:      100,
		MaxOutstandingMessages: 100, // Default for pubsub.ReceiveSettings
	}
}

// LoadInputPubSubSubscriptionConfigFromEnv loads input Pub/Sub subscription config.
func LoadInputPubSubSubscriptionConfigFromEnv(projectIDEnvKey, subscriptionIDEnvKey string) (*PubSubSubscriptionConfig, error) {
	cfg := &PubSubSubscriptionConfig{
		ProjectID:      os.Getenv(projectIDEnvKey),
		SubscriptionID: os.Getenv(subscriptionIDEnvKey),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("input Pub/Sub project ID environment variable (" + projectIDEnvKey + ") not set")
	}
	if cfg.SubscriptionID == "" {
		return nil, errors.New("input Pub/Sub subscription ID environment variable (" + subscriptionIDEnvKey + ") not set")
	}
	return cfg, nil
}

// LoadEnrichmentServiceWorkerConfigFromEnv loads worker and channel specific configurations.
func LoadEnrichmentServiceWorkerConfigFromEnv() (numWorkers, chanCapacity, maxOutstanding int, err error) {
	// Defaults
	numWorkers = 5
	chanCapacity = 100
	maxOutstanding = 100

	if nwStr := os.Getenv("ENRICHMENT_NUM_WORKERS"); nwStr != "" {
		if nw, e := strconv.Atoi(nwStr); e == nil && nw > 0 {
			numWorkers = nw
		} else {
			err = errors.Join(err, fmt.Errorf("invalid ENRICHMENT_NUM_WORKERS value: %s", nwStr))
		}
	}
	if ccStr := os.Getenv("ENRICHMENT_INPUT_CHAN_CAPACITY"); ccStr != "" {
		if cc, e := strconv.Atoi(ccStr); e == nil && cc > 0 {
			chanCapacity = cc
		} else {
			err = errors.Join(err, fmt.Errorf("invalid ENRICHMENT_INPUT_CHAN_CAPACITY value: %s", ccStr))
		}
	}
	if moStr := os.Getenv("ENRICHMENT_MAX_OUTSTANDING_MSGS"); moStr != "" {
		if mo, e := strconv.Atoi(moStr); e == nil && mo > 0 {
			maxOutstanding = mo
		} else {
			err = errors.Join(err, fmt.Errorf("invalid ENRICHMENT_MAX_OUTSTANDING_MSGS value: %s", moStr))
		}
	}
	return
}
