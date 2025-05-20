package xdevice

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"os"
	"strings"
)

// BigQueryInserterConfig holds configuration for the BigQuery inserter.
type BigQueryInserterConfig struct {
	ProjectID       string
	DatasetID       string
	TableID         string
	CredentialsFile string // Optional: For production if not using ADC
	// EmulatorHost field was removed; client injection handles emulator specifics
}

// LoadBigQueryInserterConfigFromEnv loads BigQuery configuration from environment variables.
func LoadBigQueryInserterConfigFromEnv() (*BigQueryInserterConfig, error) {
	cfg := &BigQueryInserterConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		DatasetID:       os.Getenv("BQ_DATASET_ID"),
		TableID:         os.Getenv("BQ_TABLE_ID_METER_READINGS"),
		CredentialsFile: os.Getenv("GCP_BQ_CREDENTIALS_FILE"),
	}

	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID environment variable not set for BigQuery config")
	}
	if cfg.DatasetID == "" {
		return nil, fmt.Errorf("BQ_DATASET_ID environment variable not set for BigQuery config")
	}
	if cfg.TableID == "" {
		return nil, fmt.Errorf("BQ_TABLE_ID_METER_READINGS environment variable not set for BigQuery config")
	}
	return cfg, nil
}

// NewProductionBigQueryClient creates a BigQuery client suitable for production environments.
// It now accepts a BigQueryInserterConfig struct.
func NewProductionBigQueryClient(ctx context.Context, cfg *BigQueryInserterConfig, logger zerolog.Logger) (*bigquery.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("BigQueryInserterConfig cannot be nil for NewProductionBigQueryClient")
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("cfg.ProjectID is required for NewProductionBigQueryClient")
	}

	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for production BigQuery client")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for production BigQuery client")
	}

	client, err := bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		logger.Error().Err(err).Str("project_id", cfg.ProjectID).Msg("Failed to create production BigQuery client")
		return nil, fmt.Errorf("bigquery.NewClient (production): %w", err)
	}
	logger.Info().Str("project_id", cfg.ProjectID).Msg("Production BigQuery client created successfully.")
	return client, nil
}

// BigQueryInserter implements DecodedDataInserter for Google BigQuery.
type BigQueryInserter struct {
	client    *bigquery.Client // Client is now injected
	table     *bigquery.Table
	inserter  *bigquery.Inserter
	logger    zerolog.Logger
	projectID string // Store for reference/logging
	datasetID string
	tableID   string
}

// NewBigQueryInserter creates a new inserter for MeterReading data into BigQuery.
// It now accepts a pre-configured *bigquery.Client.
func NewBigQueryInserter(
	ctx context.Context,
	client *bigquery.Client, // Injected client
	cfg *BigQueryInserterConfig, // Still need dataset and table ID
	logger zerolog.Logger,
) (*BigQueryInserter, error) {
	if client == nil {
		return nil, fmt.Errorf("bigquery client cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("BigQueryInserterConfig cannot be nil")
	}
	if cfg.DatasetID == "" || cfg.TableID == "" {
		return nil, fmt.Errorf("DatasetID and TableID must be provided in BigQueryInserterConfig")
	}

	projectID := client.Project() // Get project ID from the injected client
	if projectID == "" && cfg.ProjectID == "" {
		// This case should be rare if client is properly initialized.
		// If client.Project() is empty, and cfg.ProjectID is also empty, it's an issue.
		return nil, fmt.Errorf("project ID could not be determined from client or config")
	} else if projectID == "" {
		projectID = cfg.ProjectID // Fallback to config if client doesn't expose it easily (shouldn't happen)
		logger.Warn().Str("config_project_id", projectID).Msg("Using ProjectID from config as client.Project() was empty.")
	}

	logger.Info().Str("project_id", projectID).Str("dataset_id", cfg.DatasetID).Str("table_id", cfg.TableID).Msg("Initializing BigQueryInserter with injected client.")

	tableRef := client.Dataset(cfg.DatasetID).Table(cfg.TableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			logger.Warn().Str("dataset", cfg.DatasetID).Str("table", cfg.TableID).Msg("BigQuery table not found. Attempting to create with inferred schema from MeterReading.")
			inferredSchema, inferErr := bigquery.InferSchema(MeterReading{})
			if inferErr != nil {
				// client.Close() // Client lifecycle is managed by the caller now
				return nil, fmt.Errorf("failed to infer schema for MeterReading: %w", inferErr)
			}
			tableMetadata := &bigquery.TableMetadata{
				Schema: inferredSchema,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:  bigquery.DayPartitioningType,
					Field: "original_mqtt_time",
				},
			}
			if createErr := tableRef.Create(ctx, tableMetadata); createErr != nil {
				// client.Close()
				return nil, fmt.Errorf("failed to create BigQuery table %s.%s: %w", cfg.DatasetID, cfg.TableID, createErr)
			}
			logger.Info().Str("dataset", cfg.DatasetID).Str("table", cfg.TableID).Msg("BigQuery table created with inferred schema.")
		} else {
			// client.Close()
			return nil, fmt.Errorf("failed to get BigQuery table metadata for %s.%s: %w", cfg.DatasetID, cfg.TableID, err)
		}
	} else {
		logger.Info().Str("dataset", cfg.DatasetID).Str("table", cfg.TableID).Interface("schema_length", len(meta.Schema)).Msg("BigQuery table metadata loaded.")
	}

	return &BigQueryInserter{
		client:    client, // Store the injected client
		table:     tableRef,
		inserter:  tableRef.Inserter(),
		logger:    logger,
		projectID: projectID, // Store for reference
		datasetID: cfg.DatasetID,
		tableID:   cfg.TableID,
	}, nil
}

// Insert streams a single MeterReading to BigQuery.
func (i *BigQueryInserter) Insert(ctx context.Context, reading MeterReading) error {
	itemsToInsert := []*MeterReading{&reading}
	if err := i.inserter.Put(ctx, itemsToInsert); err != nil {
		i.logger.Error().Err(err).Str("uid", reading.UID).Str("device_eui", reading.DeviceEUI).Msg("Failed to insert row into BigQuery")
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			for _, rowErr := range multiErr {
				i.logger.Error().Str("row_index", fmt.Sprintf("%d", rowErr.RowIndex)).Msgf("BigQuery insert error for row: %v", rowErr.Errors)
			}
		}
		return fmt.Errorf("bigquery Inserter.Put: %w", err)
	}
	i.logger.Debug().Str("uid", reading.UID).Str("device_eui", reading.DeviceEUI).Msg("Successfully inserted row into BigQuery")
	return nil
}

// Close is now a no-op as the client lifecycle is managed externally.
// The caller of NewBigQueryInserter is responsible for closing the client it provided.
func (i *BigQueryInserter) Close() error {
	i.logger.Info().Msg("BigQueryInserter.Close() called. Client lifecycle is managed externally.")
	// Do not close i.client here, as it was injected.
	return nil
}
