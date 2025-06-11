package servicemanager

import (
	"context"
	"fmt"
	"strings"
	// "time" // No longer used directly in this file after removing unused default time

	"cloud.google.com/go/iam" // Correct import for iam.Handle
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	// "google.golang.org/api/iterator" // No longer used directly
	"google.golang.org/api/option"
)

// --- GCS Client Abstraction Interfaces ---

// GCSBucketHandle defines an interface for interacting with a GCS bucket.
// This allows for easier mocking in unit tests.
type GCSBucketHandle interface {
	Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error
	Update(ctx context.Context, attrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error)
	Delete(ctx context.Context) error
	IAM() *iam.Handle // Corrected to use iam.Handle
	// Add other methods like Lifecycle, DefaultObjectACL, etc., if needed by the manager.
}

// GCSClient defines an interface for a GCS client.
type GCSClient interface {
	Bucket(name string) GCSBucketHandle
	Buckets(ctx context.Context, projectID string) *storage.BucketIterator // For listing, if needed
	Close() error
}

// --- Adapters for real GCS client ---

type gcsBucketHandleAdapter struct {
	bucket *storage.BucketHandle
}

func (a *gcsBucketHandleAdapter) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return a.bucket.Attrs(ctx)
}
func (a *gcsBucketHandleAdapter) Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error {
	return a.bucket.Create(ctx, projectID, attrs)
}
func (a *gcsBucketHandleAdapter) Update(ctx context.Context, attrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	return a.bucket.Update(ctx, attrs)
}
func (a *gcsBucketHandleAdapter) Delete(ctx context.Context) error {
	return a.bucket.Delete(ctx)
}
func (a *gcsBucketHandleAdapter) IAM() *iam.Handle { // Corrected to use iam.Handle
	return a.bucket.IAM()
}

type gcsClientAdapter struct {
	client *storage.Client
}

func (a *gcsClientAdapter) Bucket(name string) GCSBucketHandle {
	return &gcsBucketHandleAdapter{bucket: a.client.Bucket(name)}
}
func (a *gcsClientAdapter) Buckets(ctx context.Context, projectID string) *storage.BucketIterator {
	return a.client.Buckets(ctx, projectID) // Note: This returns the concrete type directly.
}
func (a *gcsClientAdapter) Close() error {
	return a.client.Close()
}

// NewGCSClientAdapter wraps a concrete *storage.Client with the GCSClient interface.
func NewGCSClientAdapter(client *storage.Client) GCSClient {
	if client == nil {
		return nil
	}
	return &gcsClientAdapter{client: client}
}

// Ensure adapters implement the interfaces
var _ GCSBucketHandle = &gcsBucketHandleAdapter{}
var _ GCSClient = &gcsClientAdapter{}

// StorageManager handles the creation and deletion of GCS buckets.
type StorageManager struct {
	client GCSClient // Use the interface for testability
	logger zerolog.Logger
}

// NewStorageManager creates a new StorageManager.
// It takes a GCSClient interface, allowing for a real or mock client to be injected.
func NewStorageManager(client GCSClient, logger zerolog.Logger) (*StorageManager, error) {
	if client == nil {
		return nil, fmt.Errorf("GCS client (GCSClient interface) cannot be nil")
	}
	return &StorageManager{
		client: client,
		logger: logger.With().Str("component", "StorageManager").Logger(),
	}, nil
}

// Setup creates or updates GCS buckets as defined in the configuration.
func (sm *StorageManager) Setup(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("StorageManager.Setup: %w", err)
	}
	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting GCS Bucket setup")

	defaultLocation := cfg.DefaultLocation
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLocation != "" {
		defaultLocation = envSpec.DefaultLocation
	}
	var defaultLabels map[string]string
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLabels != nil {
		defaultLabels = envSpec.DefaultLabels
	}

	for _, bucketCfg := range cfg.Resources.GCSBuckets {
		if bucketCfg.Name == "" {
			sm.logger.Error().Msg("Skipping GCS bucket with empty name")
			continue
		}

		sm.logger.Debug().Str("bucket_name", bucketCfg.Name).Msg("Processing bucket configuration")

		bucketHandle := sm.client.Bucket(bucketCfg.Name)
		existingAttrs, err := bucketHandle.Attrs(ctx)

		// Combine default and bucket-specific labels
		finalLabels := make(map[string]string)
		for k, v := range defaultLabels { // Apply environment default labels first
			finalLabels[k] = v
		}
		for k, v := range bucketCfg.Labels { // Bucket-specific labels override environment defaults
			finalLabels[k] = v
		}

		attrsToApply := storage.BucketAttrs{
			Name:              bucketCfg.Name, // Name is immutable after creation
			StorageClass:      bucketCfg.StorageClass,
			VersioningEnabled: bucketCfg.VersioningEnabled,
			Labels:            finalLabels, // Used for creation
		}

		if bucketCfg.Location != "" {
			attrsToApply.Location = strings.ToUpper(bucketCfg.Location) // Location should be uppercase
		} else if defaultLocation != "" {
			attrsToApply.Location = strings.ToUpper(defaultLocation)
		} else {
			sm.logger.Warn().Str("bucket_name", bucketCfg.Name).Msg("Bucket location not specified, relying on GCS defaults.")
		}

		// Convert LifecycleRuleSpec to storage.LifecycleRule
		if len(bucketCfg.LifecycleRules) > 0 {
			attrsToApply.Lifecycle = storage.Lifecycle{Rules: []storage.LifecycleRule{}}
			for _, ruleSpec := range bucketCfg.LifecycleRules {
				lcRule := storage.LifecycleRule{
					Action:    storage.LifecycleAction{Type: ruleSpec.Action.Type},
					Condition: storage.LifecycleCondition{},
				}
				if ruleSpec.Condition.AgeDays > 0 {
					lcRule.Condition.AgeInDays = int64(ruleSpec.Condition.AgeDays)
				}
				// Add other conditions like CreatedBefore, Liveness, MatchesStorageClass, NumNewerVersions etc. if defined in your spec
				attrsToApply.Lifecycle.Rules = append(attrsToApply.Lifecycle.Rules, lcRule)
			}
		}

		if err == storage.ErrBucketNotExist {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Str("location", attrsToApply.Location).Msg("Bucket not found, creating...")
			// For creation, attrsToApply.Labels is used directly.
			if errCreate := bucketHandle.Create(ctx, projectID, &attrsToApply); errCreate != nil {
				return fmt.Errorf("failed to create bucket '%s' in project '%s': %w", bucketCfg.Name, projectID, errCreate)
			}
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket created successfully")
		} else if err != nil {
			return fmt.Errorf("failed to get attributes for bucket '%s': %w", bucketCfg.Name, err)
		} else {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket already exists. Ensuring configuration...")
			attrsToUpdate := storage.BucketAttrsToUpdate{
				StorageClass:      bucketCfg.StorageClass,
				VersioningEnabled: &bucketCfg.VersioningEnabled, // Pointer for bool
			}

			// Manage labels for update:
			// Add/update labels specified in the config
			for k, v := range finalLabels {
				attrsToUpdate.SetLabel(k, v)
			}
			// Delete labels that are on the bucket but not in the config
			if existingAttrs.Labels != nil {
				for k := range existingAttrs.Labels {
					if _, existsInFinalConfig := finalLabels[k]; !existsInFinalConfig {
						attrsToUpdate.DeleteLabel(k)
					}
				}
			}

			if len(attrsToApply.Lifecycle.Rules) > 0 {
				attrsToUpdate.Lifecycle = &attrsToApply.Lifecycle
			} else if existingAttrs.Lifecycle.Rules != nil && len(existingAttrs.Lifecycle.Rules) > 0 {
				// If config has no rules, but bucket has, clear them by setting an empty Lifecycle
				attrsToUpdate.Lifecycle = &storage.Lifecycle{Rules: []storage.LifecycleRule{}}
			}

			// A more sophisticated diff could be implemented here to avoid unnecessary updates.
			// For now, we proceed with the update call.
			if _, updateErr := bucketHandle.Update(ctx, attrsToUpdate); updateErr != nil {
				sm.logger.Warn().Err(updateErr).Str("bucket_name", bucketCfg.Name).Msg("Failed to update bucket attributes")
			} else {
				sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket attributes updated/ensured.")
			}
		}
	}
	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("GCS Bucket setup completed successfully")
	return nil
}

// Teardown deletes GCS buckets as defined in the configuration.
func (sm *StorageManager) Teardown(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	projectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("StorageManager.Teardown: %w", err)
	}
	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("Starting GCS Bucket teardown")

	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		sm.logger.Error().Str("environment", environment).Msg("Teardown protection is enabled for this environment for GCS. Manual intervention required or override.")
		return fmt.Errorf("teardown protection enabled for GCS in environment: %s", environment)
	}

	for i := len(cfg.Resources.GCSBuckets) - 1; i >= 0; i-- { // Delete in reverse order
		bucketCfg := cfg.Resources.GCSBuckets[i]
		if bucketCfg.Name == "" {
			sm.logger.Warn().Msg("Skipping GCS bucket with empty name during teardown")
			continue
		}

		bucketHandle := sm.client.Bucket(bucketCfg.Name)
		sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Attempting to delete bucket...")

		if _, errAttr := bucketHandle.Attrs(ctx); errAttr == storage.ErrBucketNotExist {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket does not exist, skipping deletion.")
			continue
		} else if errAttr != nil {
			sm.logger.Error().Err(errAttr).Str("bucket_name", bucketCfg.Name).Msg("Failed to get attributes before attempting delete, skipping.")
			continue
		}

		if errDel := bucketHandle.Delete(ctx); errDel != nil {
			if strings.Contains(errDel.Error(), "not empty") {
				sm.logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket because it is not empty. Manual cleanup of objects required.")
			} else {
				sm.logger.Error().Err(errDel).Str("bucket_name", bucketCfg.Name).Msg("Failed to delete bucket")
			}
		} else {
			sm.logger.Info().Str("bucket_name", bucketCfg.Name).Msg("Bucket deleted successfully")
		}
	}

	sm.logger.Info().Str("project_id", projectID).Str("environment", environment).Msg("GCS Bucket teardown completed")
	return nil
}

// CreateGoogleGCSClient creates a real GCS client for use when not testing with mocks.
func CreateGoogleGCSClient(ctx context.Context, clientOpts ...option.ClientOption) (GCSClient, error) {
	realClient, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	return NewGCSClientAdapter(realClient), nil
}
