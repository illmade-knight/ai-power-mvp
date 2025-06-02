package cmd

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"

	// Replace "your_module_path/servicemanager/initialization" with the actual import path
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/initialization"
)

var (
	// forceDelete will be used to bypass teardown protection.
	forceDelete bool
)

// destroyCmd represents the destroy command
var destroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "Destroy cloud resources based on the configuration for an environment",
	Long: `The destroy command reads the specified YAML configuration file and
attempts to delete the defined Google Cloud resources (Pub/Sub, BigQuery, GCS)
in the target environment.

WARNING: This is a destructive operation.
If teardown protection is enabled for the environment in the configuration,
this command will fail unless the --force flag is provided.`,
	Example: `  smctl destroy --config ./service.yaml --env test
  smctl destroy -c path/to/your/config.yaml -e staging --force
  smctl destroy -c cfg.yaml -e prod --project specific-project-id --force # Highly discouraged for prod`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// cfgFile, environment, and projectID are persistent flags from root.go
		if cfgFile == "" {
			return fmt.Errorf("--config flag (or -c) must be set to specify the configuration file")
		}
		if environment == "" {
			return fmt.Errorf("--env flag (or -e) must be set to specify the target environment")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Warn().Str("config_file", cfgFile).Str("environment", environment).Msg("Starting 'destroy' command. This is a DESTRUCTIVE operation.")

		cfg, err := initialization.LoadAndValidateConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load or validate config file '%s': %w", cfgFile, err)
		}
		log.Debug().Msg("Configuration loaded and validated successfully.")

		// Check for teardown protection
		if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
			if !forceDelete {
				errMsg := fmt.Sprintf("teardown protection is enabled for environment '%s' in the configuration. Use the --force flag to override.", environment)
				log.Error().Msg(errMsg)
				return errors.New(errMsg)
			}
			log.Warn().Str("environment", environment).Msg("Teardown protection is enabled but --force flag was provided. Proceeding with destruction.")
		} else if ok && !envSpec.TeardownProtection {
			log.Info().Str("environment", environment).Msg("Teardown protection is not enabled for this environment.")
		} else if !ok {
			log.Warn().Str("environment", environment).Msg("Environment not explicitly defined in config's 'environments' section; proceeding without teardown protection check for this specific key (will use defaults if any).")
		}

		// Determine the actual target project ID
		actualProjectID := projectID // Value from the --project flag (if provided)
		if actualProjectID == "" {   // If not overridden by flag, get from config based on environment
			actualProjectID, err = initialization.GetTargetProjectID(cfg, environment)
			if err != nil {
				return fmt.Errorf("could not determine target project ID for environment '%s': %w", environment, err)
			}
		}
		log.Info().Str("target_project_id", actualProjectID).Msg("Targeting GCP Project for destruction")

		ctx := context.Background()
		var clientOpts []option.ClientOption
		// Add SA key flag handling if implemented in root or here
		log.Info().Msg("Using Application Default Credentials (ADC) or GCE metadata service for GCP clients for teardown.")

		// It's generally safer to tear down in reverse order of potential dependencies,
		// or in an order that minimizes issues if one part fails.
		// For example, Pub/Sub topics can't be deleted if they have subscriptions.
		// Buckets can't be deleted if not empty (our current manager doesn't empty them).

		// --- Google Cloud Storage (GCS) Teardown ---
		log.Info().Msg("Starting GCS bucket teardown...")
		storageRealClient, err := storage.NewClient(ctx, clientOpts...)
		if err != nil {
			return fmt.Errorf("failed to create real GCS client: %w", err)
		}
		defer func() {
			if err := storageRealClient.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close GCS client")
			}
		}()
		gcsAdapter := initialization.NewGCSClientAdapter(storageRealClient)
		storageMgr, err := initialization.NewStorageManager(gcsAdapter, log.Logger)
		if err != nil {
			return fmt.Errorf("failed to create StorageManager for GCS: %w", err)
		}
		if err := storageMgr.Teardown(ctx, cfg, environment); err != nil {
			// Log error but continue to attempt teardown of other resource types
			log.Error().Err(err).Msg("GCS bucket teardown failed. Continuing with other resources.")
		} else {
			log.Info().Msg("GCS bucket teardown completed.")
		}

		// --- BigQuery Teardown ---
		// Tables should be deleted before datasets. TeardownTables and TeardownDatasets in BigQueryManager handle this order.
		log.Info().Msg("Starting BigQuery resource teardown...")
		bqRealClient, err := bigquery.NewClient(ctx, actualProjectID, clientOpts...)
		if err != nil {
			return fmt.Errorf("failed to create real BigQuery client for project '%s': %w", actualProjectID, err)
		}
		defer func() {
			if err := bqRealClient.Close(); err != nil {
				log.Error().Err(err).Msg("Failed to close BigQuery client")
			}
		}()
		bqAdapter := initialization.NewBigQueryClientAdapter(bqRealClient)
		bqManager, err := initialization.NewBigQueryManager(bqAdapter, log.Logger, nil) // knownSchemas not needed for teardown
		if err != nil {
			return fmt.Errorf("failed to create BigQueryManager: %w", err)
		}
		if err := bqManager.Teardown(ctx, cfg, environment); err != nil {
			log.Error().Err(err).Msg("BigQuery teardown failed. Continuing with other resources.")
		} else {
			log.Info().Msg("BigQuery resource teardown completed.")
		}

		// --- Pub/Sub Teardown ---
		// Subscriptions should be deleted before topics. PubSubManager.Teardown handles this order.
		log.Info().Msg("Starting Pub/Sub resource teardown...")
		psManager := initialization.NewPubSubManager(log.Logger)

		if err := psManager.Teardown(ctx, cfg, environment, clientOpts...); err != nil {
			log.Error().Err(err).Msg("Pub/Sub teardown failed.")
			// This might be the last step, so returning the error might be acceptable.
			// Or, if there were more steps, log and continue.
			return fmt.Errorf("Pub/Sub teardown failed: %w", err)
		}
		log.Info().Msg("Pub/Sub resource teardown completed.")

		log.Info().Str("environment", environment).Msg("'destroy' command finished.")
		fmt.Printf("Attempted to destroy resources for environment '%s' from '%s'. Check logs for details.\n", environment, cfgFile)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(destroyCmd)

	// Add local flags specific to the 'destroy' command.
	destroyCmd.Flags().BoolVarP(&forceDelete, "force", "f", false, "Force deletion even if teardown protection is enabled in the configuration for the environment.")
}
