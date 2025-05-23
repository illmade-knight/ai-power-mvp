package connectors

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"os"
	"strings"
)

// FirestoreFetcherConfig holds configuration for the Firestore metadata fetcher.
type FirestoreFetcherConfig struct {
	ProjectID       string
	CollectionName  string // e.g., "devices"
	CredentialsFile string // Optional, for specific service account
}

// LoadFirestoreFetcherConfigFromEnv loads Firestore fetcher configuration.
func LoadFirestoreFetcherConfigFromEnv() (*FirestoreFetcherConfig, error) {
	cfg := &FirestoreFetcherConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"), // Can reuse the same project ID
		CollectionName:  os.Getenv("FIRESTORE_COLLECTION_DEVICES"),
		CredentialsFile: os.Getenv("GCP_FIRESTORE_CREDENTIALS_FILE"),
	}
	if cfg.ProjectID == "" {
		return nil, errors.New("GCP_PROJECT_ID environment variable not set for Firestore")
	}
	if cfg.CollectionName == "" {
		return nil, errors.New("FIRESTORE_COLLECTION_DEVICES environment variable not set")
	}
	return cfg, nil
}

// GoogleDeviceMetadataFetcher implements DeviceMetadataFetcher using Google Cloud Firestore.
type GoogleDeviceMetadataFetcher struct {
	client         *firestore.Client
	collectionName string
	logger         zerolog.Logger
}

// NewGoogleDeviceMetadataFetcher creates a new fetcher that uses Firestore.
// For emulator, ensure FIRESTORE_EMULATOR_HOST environment variable is set.
func NewGoogleDeviceMetadataFetcher(ctx context.Context, cfg *FirestoreFetcherConfig, logger zerolog.Logger) (*GoogleDeviceMetadataFetcher, error) {
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for Firestore")
	} else if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		// Only log about ADC if not using emulator and no specific creds file
		logger.Info().Msg("Using Application Default Credentials (ADC) for Firestore")
	}

	// The Firestore client library automatically detects FIRESTORE_EMULATOR_HOST.
	client, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		logger.Error().Err(err).Str("project_id", cfg.ProjectID).Msg("Failed to create Firestore client")
		return nil, fmt.Errorf("firestore.NewClient: %w", err)
	}

	logger.Info().Str("project_id", cfg.ProjectID).Str("collection", cfg.CollectionName).Msg("GoogleDeviceMetadataFetcher initialized successfully")
	return &GoogleDeviceMetadataFetcher{
		client:         client,
		collectionName: cfg.CollectionName,
		logger:         logger,
	}, nil
}

// Fetch retrieves device metadata from Firestore.
// Assumes document ID in Firestore is the deviceEUI.
func (f *GoogleDeviceMetadataFetcher) Fetch(deviceEUI string) (clientID, locationID, category string, err error) {
	f.logger.Debug().Str("device_eui", deviceEUI).Msg("Fetching metadata from Firestore")

	docRef := f.client.Collection(f.collectionName).Doc(deviceEUI)
	docSnap, err := docRef.Get(context.Background()) // Use a background context or pass one in

	if err != nil {
		if strings.Contains(err.Error(), "not found") { // Check for specific "not found" error if available from Firestore client
			f.logger.Warn().Str("device_eui", deviceEUI).Msg("Device metadata not found in Firestore")
			return "", "", "", ErrMetadataNotFound // Use your defined error
		}
		f.logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Failed to get device document from Firestore")
		return "", "", "", fmt.Errorf("firestore Get for %s: %w", deviceEUI, err)
	}

	var deviceData struct {
		ClientID       string `firestore:"clientID"`
		LocationID     string `firestore:"locationID"`
		DeviceCategory string `firestore:"deviceCategory"`
	}
	if err := docSnap.DataTo(&deviceData); err != nil {
		f.logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Failed to map Firestore document data")
		return "", "", "", fmt.Errorf("firestore DataTo for %s: %w", deviceEUI, err)
	}

	if deviceData.ClientID == "" || deviceData.LocationID == "" || deviceData.DeviceCategory == "" {
		f.logger.Warn().Str("device_eui", deviceEUI).Interface("data", deviceData).Msg("Fetched metadata from Firestore is incomplete")
		// Decide if incomplete data is an error or if defaults should be used.
		// For now, treat as an error similar to not found if critical fields are missing.
		return "", "", "", fmt.Errorf("incomplete metadata for %s: %+v", deviceEUI, deviceData)
	}

	f.logger.Debug().Str("device_eui", deviceEUI).
		Str("clientID", deviceData.ClientID).
		Str("locationID", deviceData.LocationID).
		Str("category", deviceData.DeviceCategory).
		Msg("Successfully fetched metadata from Firestore")
	return deviceData.ClientID, deviceData.LocationID, deviceData.DeviceCategory, nil
}

// Close closes the Firestore client.
func (f *GoogleDeviceMetadataFetcher) Close() error {
	if f.client != nil {
		return f.client.Close()
	}
	return nil
}
