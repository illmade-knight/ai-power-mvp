package bqstore

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"os"
	"strings"
)

// BigQueryDatasetConfig holds configuration for the BigQuery inserter.
// This struct remains specific, but it's now used by the generic BigQueryInserter.
// For a true library, environment variable names could also be made configurable.
type BigQueryDatasetConfig struct {
	ProjectID       string
	DatasetID       string
	TableID         string
	CredentialsFile string // Optional: For production if not using ADC
}

// LoadBigQueryInserterConfigFromEnv loads BigQuery configuration from environment variables.
// This helper can be adapted or replaced depending on the application's config strategy.
func LoadBigQueryInserterConfigFromEnv() (*BigQueryDatasetConfig, error) {
	cfg := &BigQueryDatasetConfig{
		ProjectID:       os.Getenv("GCP_PROJECT_ID"),
		DatasetID:       os.Getenv("BQ_DATASET_ID"),
		TableID:         os.Getenv("BQ_TABLE_ID"), // Generic name
		CredentialsFile: os.Getenv("GCP_BQ_CREDENTIALS_FILE"),
	}

	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID environment variable not set for BigQuery config")
	}
	if cfg.DatasetID == "" {
		return nil, fmt.Errorf("BQ_DATASET_ID environment variable not set for BigQuery config")
	}
	if cfg.TableID == "" {
		return nil, fmt.Errorf("BQ_TABLE_ID environment variable not set for BigQuery config")
	}
	return cfg, nil
}

// NewProductionBigQueryClient creates a BigQuery client suitable for production.
// This function is a general utility and does not need to be generic.
func NewProductionBigQueryClient(ctx context.Context, cfg *BigQueryDatasetConfig, logger zerolog.Logger) (*bigquery.Client, error) {
	// ... (Implementation from original file is good, no changes needed)
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for BigQuery client")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for BigQuery client")
	}

	client, err := bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		logger.Error().Err(err).Str("project_id", cfg.ProjectID).Msg("Failed to create BigQuery client")
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	logger.Info().Str("project_id", cfg.ProjectID).Msg("BigQuery client created successfully.")
	return client, nil
}

// BigQueryInserter is a generic struct that implements DataBatchInserter[T] for Google BigQuery.
// It can insert batches of any struct type T into a BigQuery table.
type BigQueryInserter[T any] struct {
	client    *bigquery.Client
	table     *bigquery.Table
	inserter  *bigquery.Inserter
	logger    zerolog.Logger
	projectID string
	datasetID string
	tableID   string
}

// NewBigQueryInserter creates a new generic inserter for a specified type T.
// If the target table does not exist, it attempts to create it by inferring the
// schema from the zero value of type T.
func NewBigQueryInserter[T any](
	ctx context.Context,
	client *bigquery.Client,
	cfg *BigQueryDatasetConfig,
	logger zerolog.Logger,
) (*BigQueryInserter[T], error) {
	if client == nil {
		return nil, fmt.Errorf("bigquery client cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("BigQueryDatasetConfig cannot be nil")
	}

	projectID := client.Project()
	logger = logger.With().Str("project_id", projectID).Str("dataset_id", cfg.DatasetID).Str("table_id", cfg.TableID).Logger()

	tableRef := client.Dataset(cfg.DatasetID).Table(cfg.TableID)
	_, err := tableRef.Metadata(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "notFound") {
			logger.Warn().Msg("BigQuery table not found. Attempting to create with inferred schema.")

			// KEY CHANGE: Infer schema from a zero value of the generic type T.
			var zero T
			inferredSchema, inferErr := bigquery.InferSchema(zero)
			if inferErr != nil {
				return nil, fmt.Errorf("failed to infer schema for type %T: %w", zero, inferErr)
			}
			logger.Info().Int("inferred_field_count", len(inferredSchema)).Msg("Successfully inferred schema from type.")

			tableMetadata := &bigquery.TableMetadata{Schema: inferredSchema}

			// Attempt to add time partitioning if a 'Timestamp' field exists.
			for _, field := range inferredSchema {
				if field.Name == "Timestamp" && field.Type == bigquery.TimestampFieldType {
					tableMetadata.TimePartitioning = &bigquery.TimePartitioning{
						Type:  bigquery.DayPartitioningType,
						Field: "Timestamp",
					}
					logger.Info().Msg("Detected 'Timestamp' field. Applying daily time partitioning.")
					break
				}
			}

			if createErr := tableRef.Create(ctx, tableMetadata); createErr != nil {
				return nil, fmt.Errorf("failed to create BigQuery table %s.%s: %w", cfg.DatasetID, cfg.TableID, createErr)
			}
			logger.Info().Msg("BigQuery table created successfully.")
		} else {
			return nil, fmt.Errorf("failed to get BigQuery table metadata: %w", err)
		}
	} else {
		logger.Info().Msg("Successfully connected to existing BigQuery table.")
	}

	return &BigQueryInserter[T]{
		client:    client,
		table:     tableRef,
		inserter:  tableRef.Inserter(),
		logger:    logger,
		projectID: projectID,
		datasetID: cfg.DatasetID,
		tableID:   cfg.TableID,
	}, nil
}

// InsertBatch streams a batch of items of type T to BigQuery.
// It fulfills the DataBatchInserter[T] interface.
func (i *BigQueryInserter[T]) InsertBatch(ctx context.Context, items []*T) error {
	if len(items) == 0 {
		i.logger.Info().Msg("InsertBatch called with an empty slice. Nothing to do.")
		return nil
	}

	// The BigQuery client's Put method can already handle a slice of any struct pointers.
	err := i.inserter.Put(ctx, items)
	if err != nil {
		i.logger.Error().Err(err).Int("batch_size", len(items)).Msg("Failed to insert rows into BigQuery")
		// Log detailed row-level errors if available
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			for _, rowErr := range multiErr {
				i.logger.Error().
					Int("row_index", rowErr.RowIndex).
					Msgf("BigQuery insert error for row: %v", rowErr.Errors)
			}
		}
		return fmt.Errorf("bigquery Inserter.Put failed: %w", err)
	}

	i.logger.Info().Int("batch_size", len(items)).Msg("Successfully inserted batch into BigQuery")
	return nil
}

// Close is a no-op as the BigQuery client's lifecycle is managed externally
// to allow client sharing across different inserters.
func (i *BigQueryInserter[T]) Close() error {
	i.logger.Info().Msg("BigQueryInserter.Close() called. Client lifecycle is managed externally.")
	return nil
}
