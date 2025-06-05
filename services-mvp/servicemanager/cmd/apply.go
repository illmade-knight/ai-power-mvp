package cmd

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"

	// Replace "your_module_path/servicemanager/initialization" with the actual import path
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/initialization"
)

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply the configuration to provision or update cloud resources",
	Long: `The apply command reads the specified YAML configuration file and
provisions or updates the defined Google Cloud resources (Pub/Sub topics,
subscriptions, BigQuery datasets/tables, GCS buckets) in the
target environment.

It aims to be idempotent, meaning running it multiple times with the
same configuration should result in the same state without errors.`,
	Example: `  smctl apply --config ./service.yaml --env test
  smctl apply -c path/to/your/config.yaml -e production --project specific-project-id`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// cfgFile, environment, and projectID are persistent flags from root.go
		if cfgFile == "" {
			return fmt.Errorf("--config flag (or -c) must be set to specify the configuration file")
		}
		if environment == "" {
			return fmt.Errorf("--env flag (or -e) must be set to specify the target environment")
		}
		// projectID is optional, it overrides the config
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Info().Str("config_file", cfgFile).Str("environment", environment).Msg("Starting 'apply' command")

		cfg, err := initialization.LoadAndValidateConfig(cfgFile)
		if err != nil {
			return fmt.Errorf("failed to load or validate config file '%s': %w", cfgFile, err)
		}
		log.Debug().Msg("Configuration loaded and validated successfully.")

		// Determine the actual target project MessageID
		actualProjectID := projectID // Value from the --project flag (if provided)
		if actualProjectID == "" {   // If not overridden by flag, get from config based on environment
			actualProjectID, err = initialization.GetTargetProjectID(cfg, environment)
			if err != nil {
				return fmt.Errorf("could not determine target project MessageID for environment '%s': %w", environment, err)
			}
		}
		log.Info().Str("target_project_id", actualProjectID).Msg("Targeting GCP Project")

		ctx := context.Background()
		var clientOpts []option.ClientOption
		// TODO: Consider adding a persistent flag for a service account key file path
		// if saKeyPath != "" {
		//    clientOpts = append(clientOpts, option.WithCredentialsFile(saKeyPath))
		//    log.Info().Str("service_account_key_path", saKeyPath).Msg("Using specified service account key for GCP clients")
		// } else {
		log.Info().Msg("Using Application Default Credentials (ADC) or GCE metadata service for GCP clients")
		// }

		// --- Pub/Sub Setup ---
		log.Info().Msg("Starting Pub/Sub resource setup...")
		psManager := initialization.NewPubSubManager(log.Logger) // Assumes NewPubSubManager takes only logger
		// The Setup method in PubSubManager handles client creation internally using projectID from config & clientOpts.
		if err := psManager.Setup(ctx, cfg, environment, clientOpts...); err != nil {
			return fmt.Errorf("Pub/Sub setup failed: %w", err)
		}
		log.Info().Msg("Pub/Sub resource setup completed.")

		// --- BigQuery Setup ---
		log.Info().Msg("Starting BigQuery resource setup...")
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
		// knownSchemas can be nil if schema inference is the primary use or if schemas are fully defined in YAML
		// and handled by loadTableSchema. If you need to register specific Go structs for schema inference
		// during 'apply' (e.g., if a table is created without an explicit schema in YAML but relies on a Go struct),
		// you would populate knownSchemas here.
		bqManager, err := initialization.NewBigQueryManager(bqAdapter, log.Logger, nil)
		if err != nil {
			return fmt.Errorf("failed to create BigQueryManager: %w", err)
		}
		if err := bqManager.Setup(ctx, cfg, environment); err != nil { // environment is used to pick project from config if not overridden
			return fmt.Errorf("BigQuery setup failed: %w", err)
		}
		log.Info().Msg("BigQuery resource setup completed.")

		// --- Google Cloud Storage (GCS) Setup ---
		log.Info().Msg("Starting GCS bucket setup...")
		storageRealClient, err := storage.NewClient(ctx, clientOpts...) // GCS client does not usually take projectID at creation
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
		// StorageManager.Setup uses the projectID derived from cfg and environment internally for bucket creation.
		if err := storageMgr.Setup(ctx, cfg, environment); err != nil {
			return fmt.Errorf("GCS bucket setup failed: %w", err)
		}
		log.Info().Msg("GCS bucket setup completed.")

		log.Info().Str("environment", environment).Msg("'apply' command finished successfully.")
		fmt.Printf("Successfully applied configuration for environment '%s' from '%s'.\n", environment, cfgFile)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	// Add local flags specific to the 'apply' command, if any.
	// For example, a dry-run flag would be very useful here:
	// applyCmd.Flags().Bool("dry-run", false, "If true, only print the actions that would be taken.")
	// applyCmd.Flags().String("service-account-key", "", "Path to GCP service account key JSON file (optional, uses ADC if not set)")
}
