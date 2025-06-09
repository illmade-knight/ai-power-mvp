package bigquery

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
	client    *bigquery.Client
	table     *bigquery.Table
	inserter  *bigquery.Inserter
	logger    zerolog.Logger
	projectID string
	datasetID string
	tableID   string
}

// NewBigQueryInserter creates a new inserter for MeterReading data into BigQuery.
func NewBigQueryInserter(
	ctx context.Context,
	client *bigquery.Client,
	cfg *BigQueryInserterConfig,
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

	projectID := client.Project()
	if projectID == "" && cfg.ProjectID == "" {
		return nil, fmt.Errorf("project ID could not be determined from client or config")
	} else if projectID == "" {
		projectID = cfg.ProjectID
		logger.Warn().Str("config_project_id", projectID).Msg("Using ProjectID from config as client.Project() was empty.")
	}

	logger.Info().Str("project_id", projectID).Str("dataset_id", cfg.DatasetID).Str("table_id", cfg.TableID).Msg("Initializing BigQueryInserter with injected client.")

	tableRef := client.Dataset(cfg.DatasetID).Table(cfg.TableID)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			logger.Warn().Str("dataset", cfg.DatasetID).Str("table", cfg.TableID).Msg("BigQuery table not found. Attempting to create with inferred schema from GardenMonitorPayload.")
			inferredSchema, inferErr := bigquery.InferSchema(GardenMonitorPayload{})
			if inferErr != nil {
				return nil, fmt.Errorf("failed to infer schema for GardenMonitorPayload: %w", inferErr)
			}
			tableMetadata := &bigquery.TableMetadata{
				Schema: inferredSchema,
				TimePartitioning: &bigquery.TimePartitioning{
					Type:  bigquery.DayPartitioningType,
					Field: "Timestamp",
				},
			}
			if createErr := tableRef.Create(ctx, tableMetadata); createErr != nil {
				return nil, fmt.Errorf("failed to create BigQuery table %s.%s: %w", cfg.DatasetID, cfg.TableID, createErr)
			}
			logger.Info().Str("dataset", cfg.DatasetID).Str("table", cfg.TableID).Msg("BigQuery table created with inferred schema.")
		} else {
			return nil, fmt.Errorf("failed to get BigQuery table metadata for %s.%s: %w", cfg.DatasetID, cfg.TableID, err)
		}
	} else {
		logger.Info().Str("dataset", cfg.DatasetID).Str("table", cfg.TableID).Interface("schema_length", len(meta.Schema)).Msg("BigQuery table metadata loaded.")
	}

	return &BigQueryInserter{
		client:    client,
		table:     tableRef,
		inserter:  tableRef.Inserter(),
		logger:    logger,
		projectID: projectID,
		datasetID: cfg.DatasetID,
		tableID:   cfg.TableID,
	}, nil
}

// InsertBatch streams a batch of MeterReadings to BigQuery.
// This method is modified to accept a slice of payloads for batch insertion.
func (i *BigQueryInserter) InsertBatch(ctx context.Context, readings []*GardenMonitorPayload) error {
	if len(readings) == 0 {
		i.logger.Info().Msg("InsertBatch called with an empty slice. Nothing to do.")
		return nil
	}

	err := i.inserter.Put(ctx, readings)
	if err != nil {
		i.logger.Error().Err(err).Int("batch_size", len(readings)).Msg("Failed to insert rows into BigQuery")
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			for _, rowErr := range multiErr {
				i.logger.Error().Str("row_index", fmt.Sprintf("%d", rowErr.RowIndex)).Msgf("BigQuery insert error for row: %v", rowErr.Errors)
			}
		}
		return fmt.Errorf("bigquery Inserter.Put: %w", err)
	}

	i.logger.Info().Int("batch_size", len(readings)).Msg("Successfully inserted batch into BigQuery")
	return nil
}

// Close is a no-op as the client lifecycle is managed externally.
func (i *BigQueryInserter) Close() error {
	i.logger.Info().Msg("BigQueryInserter.Close() called. Client lifecycle is managed externally.")
	return nil
}
