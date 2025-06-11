package servicemanager

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	telemetry "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"

	// "regexp" // No longer needed for toSnakeCase
	"strings"
)

// --- BigQuery Client Abstraction Interfaces ---
type BQTable interface {
	Metadata(ctx context.Context) (*bigquery.TableMetadata, error)
	Create(ctx context.Context, meta *bigquery.TableMetadata) error
	Delete(ctx context.Context) error
}
type BQDataset interface {
	Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error)
	Create(ctx context.Context, meta *bigquery.DatasetMetadata) error
	Update(ctx context.Context, metaToUpdate bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error)
	Delete(ctx context.Context) error
	Table(tableID string) BQTable
}
type BQClient interface {
	Dataset(datasetID string) BQDataset
	Project() string
	Close() error
}

// --- Adapters for real BigQuery client ---
type bqTableClientAdapter struct{ table *bigquery.Table }

func (a *bqTableClientAdapter) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	return a.table.Metadata(ctx)
}
func (a *bqTableClientAdapter) Create(ctx context.Context, meta *bigquery.TableMetadata) error {
	return a.table.Create(ctx, meta)
}
func (a *bqTableClientAdapter) Delete(ctx context.Context) error { return a.table.Delete(ctx) }

type bqDatasetClientAdapter struct{ dataset *bigquery.Dataset }

func (a *bqDatasetClientAdapter) Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error) {
	return a.dataset.Metadata(ctx)
}
func (a *bqDatasetClientAdapter) Create(ctx context.Context, meta *bigquery.DatasetMetadata) error {
	return a.dataset.Create(ctx, meta)
}
func (a *bqDatasetClientAdapter) Update(ctx context.Context, metaToUpdate bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error) {
	return a.dataset.Update(ctx, metaToUpdate, etag)
}
func (a *bqDatasetClientAdapter) Delete(ctx context.Context) error { return a.dataset.Delete(ctx) }
func (a *bqDatasetClientAdapter) Table(tableID string) BQTable {
	return &bqTableClientAdapter{table: a.dataset.Table(tableID)}
}

type bqClientAdapter struct{ client *bigquery.Client }

func (a *bqClientAdapter) Dataset(datasetID string) BQDataset {
	return &bqDatasetClientAdapter{dataset: a.client.Dataset(datasetID)}
}
func (a *bqClientAdapter) Project() string { return a.client.Project() }
func (a *bqClientAdapter) Close() error    { return a.client.Close() }

func NewBigQueryClientAdapter(client *bigquery.Client) BQClient {
	if client == nil {
		return nil
	}
	return &bqClientAdapter{client: client}
}

var _ BQTable = &bqTableClientAdapter{}
var _ BQDataset = &bqDatasetClientAdapter{}
var _ BQClient = &bqClientAdapter{}

// BigQueryManager handles the creation and deletion of BigQuery datasets and tables.
type BigQueryManager struct {
	client         BQClient
	logger         zerolog.Logger
	schemaRegistry map[string]interface{}
}

// NewBigQueryManager creates a new BigQueryManager with an injected BQClient interface.
func NewBigQueryManager(client BQClient, logger zerolog.Logger, knownSchemas map[string]interface{}) (*BigQueryManager, error) {
	if client == nil {
		return nil, fmt.Errorf("BigQuery client (BQClient interface) cannot be nil")
	}
	if knownSchemas == nil {
		knownSchemas = make(map[string]interface{})
	}
	// Register the MeterReadingBQWrapper for schema inference.
	// The key MUST match exactly what's in the YAML's schema_source_identifier.
	knownSchemas["github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading"] = &telemetry.MeterReadingBQWrapper{}

	return &BigQueryManager{
		client:         client,
		logger:         logger.With().Str("component", "BigQueryManager").Logger(),
		schemaRegistry: knownSchemas,
	}, nil
}

// CreateGoogleBigQueryClient creates a real BigQuery client for use when not testing with mocks.
func CreateGoogleBigQueryClient(ctx context.Context, projectID string, opts ...option.ClientOption) (BQClient, error) {
	realClient, err := bigquery.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	return NewBigQueryClientAdapter(realClient), nil
}

// GetTargetProjectID, TopLevelConfig, etc. are assumed to be defined elsewhere in the package.

// Setup creates all configured BigQuery datasets and tables for a given environment.
func (m *BigQueryManager) Setup(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	targetProjectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("BigQueryManager.Setup: %w", err)
	}
	if m.client.Project() != targetProjectID {
		return fmt.Errorf("injected BigQuery client is for project '%s', but setup is targeted for project '%s'", m.client.Project(), targetProjectID)
	}
	m.logger.Info().Str("project_id", targetProjectID).Str("environment", environment).Msg("Starting BigQuery setup using injected client")
	defaultLocation := cfg.DefaultLocation
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.DefaultLocation != "" {
		defaultLocation = envSpec.DefaultLocation
	}
	if err := m.setupDatasets(ctx, m.client, cfg.Resources.BigQueryDatasets, defaultLocation, targetProjectID); err != nil {
		return err
	}
	if err := m.setupTables(ctx, m.client, cfg.Resources.BigQueryTables, targetProjectID); err != nil {
		return err
	}
	m.logger.Info().Str("project_id", targetProjectID).Str("environment", environment).Msg("BigQuery setup completed successfully")
	return nil
}

func (m *BigQueryManager) setupDatasets(ctx context.Context, client BQClient, datasetsToCreate []BigQueryDataset, defaultLocation, projectID string) error {
	m.logger.Info().Int("count", len(datasetsToCreate)).Msg("Setting up BigQuery datasets...")
	for _, dsCfg := range datasetsToCreate {
		if dsCfg.Name == "" {
			m.logger.Error().Msg("Skipping dataset with empty name")
			continue
		}
		dataset := client.Dataset(dsCfg.Name)
		_, err := dataset.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset not found, creating dataset...")
				meta := &bigquery.DatasetMetadata{Name: dsCfg.Name, Description: dsCfg.Description, Labels: dsCfg.Labels}
				if dsCfg.Location != "" {
					meta.Location = dsCfg.Location
				} else if defaultLocation != "" {
					meta.Location = defaultLocation
				} else {
					m.logger.Warn().Str("dataset_id", dsCfg.Name).Msg("Dataset location not specified, relying on BigQuery defaults.")
				}
				if err := dataset.Create(ctx, meta); err != nil {
					return fmt.Errorf("failed to create dataset '%s': %w", dsCfg.Name, err)
				}
				m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset created successfully")
			} else {
				return fmt.Errorf("failed to get metadata for dataset '%s': %w", dsCfg.Name, err)
			}
		} else {
			m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset already exists. Ensuring configuration...")
			update := bigquery.DatasetMetadataToUpdate{Description: dsCfg.Description}
			if dsCfg.Labels != nil {
				update.SetLabel("dummyKey", "")
				for k, v := range dsCfg.Labels {
					update.SetLabel(k, v)
				}
				update.DeleteLabel("dummyKey")
			}
			if _, err := dataset.Update(ctx, update, ""); err != nil {
				m.logger.Warn().Err(err).Str("dataset_id", dsCfg.Name).Msg("Failed to update dataset metadata")
			} else {
				m.logger.Info().Str("dataset_id", dsCfg.Name).Msg("Dataset metadata updated/ensured.")
			}
		}
	}
	return nil
}

func (m *BigQueryManager) setupTables(ctx context.Context, client BQClient, tablesToCreate []BigQueryTable, projectID string) error {
	m.logger.Info().Int("count", len(tablesToCreate)).Msg("Setting up BigQuery tables...")
	for _, tableCfg := range tablesToCreate {
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			m.logger.Error().Str("table_name", tableCfg.Name).Str("dataset_name", tableCfg.Dataset).Msg("Skipping table with empty name or dataset")
			continue
		}
		dataset := client.Dataset(tableCfg.Dataset)
		table := dataset.Table(tableCfg.Name)
		_, err := table.Metadata(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("table_id", tableCfg.Name).Str("dataset_id", tableCfg.Dataset).Msg("Table not found, creating table...")
				schema, errSchema := m.loadTableSchema(tableCfg)
				if errSchema != nil {
					return fmt.Errorf("load schema for '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, errSchema)
				}
				if schema == nil {
					return fmt.Errorf("schema nil for '%s.%s'", tableCfg.Dataset, tableCfg.Name)
				}
				meta := &bigquery.TableMetadata{Name: tableCfg.Name, Description: tableCfg.Description, Schema: schema}
				if tableCfg.TimePartitioningField != "" {
					meta.TimePartitioning = &bigquery.TimePartitioning{Field: tableCfg.TimePartitioningField}
					if tableCfg.TimePartitioningType != "" {
						switch strings.ToUpper(tableCfg.TimePartitioningType) {
						case "HOUR":
							meta.TimePartitioning.Type = bigquery.HourPartitioningType
						case "DAY":
							meta.TimePartitioning.Type = bigquery.DayPartitioningType
						case "MONTH":
							meta.TimePartitioning.Type = bigquery.MonthPartitioningType
						case "YEAR":
							meta.TimePartitioning.Type = bigquery.YearPartitioningType
						default:
							m.logger.Warn().Str("table", tableCfg.Name).Msg("Unsupported time partitioning type")
						}
					}
				}
				if len(tableCfg.ClusteringFields) > 0 {
					meta.Clustering = &bigquery.Clustering{Fields: tableCfg.ClusteringFields}
				}
				if err := table.Create(ctx, meta); err != nil {
					return fmt.Errorf("create table '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, err)
				}
				m.logger.Info().Str("table_id", tableCfg.Name).Msg("Table created successfully")
			} else {
				return fmt.Errorf("get metadata for '%s.%s': %w", tableCfg.Dataset, tableCfg.Name, err)
			}
		} else {
			m.logger.Info().Str("table_id", tableCfg.Name).Msg("Table already exists.")
		}
	}
	return nil
}

// loadTableSchema now relies on the registered type implementing bigquery.ValueSaver
// (like MeterReadingBQWrapper) for InferSchema to get snake_case column names.
func (m *BigQueryManager) loadTableSchema(tableCfg BigQueryTable) (bigquery.Schema, error) {
	m.logger.Info().Str("table", tableCfg.Name).Str("source_type", tableCfg.SchemaSourceType).Str("identifier", tableCfg.SchemaSourceIdentifier).Msg("Loading table schema")

	switch tableCfg.SchemaSourceType {
	case "go_struct":
		instance, ok := m.schemaRegistry[tableCfg.SchemaSourceIdentifier]
		if !ok {
			return nil, fmt.Errorf("unknown schema_source_identifier '%s' for go_struct type in table '%s'. Ensure it's registered in NewBigQueryManager.", tableCfg.SchemaSourceIdentifier, tableCfg.Name)
		}
		// bigquery.InferSchema will call the Save() method on the instance (if it's a ValueSaver)
		// to determine the schema, including column names.
		schema, err := bigquery.InferSchema(instance)
		if err != nil {
			return nil, fmt.Errorf("failed to infer schema from Go struct/saver '%s' for table '%s': %w", tableCfg.SchemaSourceIdentifier, tableCfg.Name, err)
		}
		m.logger.Info().Str("table", tableCfg.Name).Msg("Inferred schema using registered Go struct/saver. Column names will reflect Saver output (expected to be snake_case).")
		// The toSnakeCase transformation loop is no longer needed here.
		return schema, nil
	case "json_file":
		return nil, fmt.Errorf("json_file schema loading not implemented for '%s'", tableCfg.Name)
	default:
		return nil, fmt.Errorf("unsupported schema_source_type '%s' for '%s'", tableCfg.SchemaSourceType, tableCfg.Name)
	}
}

// Teardown (remains the same)
func (m *BigQueryManager) Teardown(ctx context.Context, cfg *TopLevelConfig, environment string) error {
	targetProjectID, err := GetTargetProjectID(cfg, environment)
	if err != nil {
		return fmt.Errorf("BigQueryManager.Teardown: %w", err)
	}
	if m.client.Project() != targetProjectID {
		return fmt.Errorf("injected client project '%s' != target project '%s'", m.client.Project(), targetProjectID)
	}
	m.logger.Info().Str("project_id", targetProjectID).Str("environment", environment).Msg("Starting BigQuery teardown")
	if envSpec, ok := cfg.Environments[environment]; ok && envSpec.TeardownProtection {
		return fmt.Errorf("teardown protection enabled for: %s", environment)
	}
	if err := m.teardownTables(ctx, m.client, cfg.Resources.BigQueryTables); err != nil {
		return err
	}
	if err := m.teardownDatasets(ctx, m.client, cfg.Resources.BigQueryDatasets); err != nil {
		return err
	}
	m.logger.Info().Str("project_id", targetProjectID).Msg("BigQuery teardown completed")
	return nil
}

// teardownTables (remains the same)
func (m *BigQueryManager) teardownTables(ctx context.Context, client BQClient, tablesToTeardown []BigQueryTable) error {
	m.logger.Info().Int("count", len(tablesToTeardown)).Msg("Tearing down BigQuery tables...")
	for i := len(tablesToTeardown) - 1; i >= 0; i-- {
		tableCfg := tablesToTeardown[i]
		if tableCfg.Name == "" || tableCfg.Dataset == "" {
			continue
		}
		table := client.Dataset(tableCfg.Dataset).Table(tableCfg.Name)
		m.logger.Info().Str("table", tableCfg.Name).Msg("Attempting to delete table...")
		if err := table.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("table", tableCfg.Name).Msg("Table not found, skipping.")
			} else {
				m.logger.Error().Err(err).Str("table", tableCfg.Name).Msg("Failed to delete table")
			}
		} else {
			m.logger.Info().Str("table", tableCfg.Name).Msg("Table deleted.")
		}
	}
	return nil
}

// teardownDatasets (remains the same)
func (m *BigQueryManager) teardownDatasets(ctx context.Context, client BQClient, datasetsToTeardown []BigQueryDataset) error {
	m.logger.Info().Int("count", len(datasetsToTeardown)).Msg("Tearing down BigQuery datasets...")
	for i := len(datasetsToTeardown) - 1; i >= 0; i-- {
		dsCfg := datasetsToTeardown[i]
		if dsCfg.Name == "" {
			continue
		}
		dataset := client.Dataset(dsCfg.Name)
		m.logger.Info().Str("dataset", dsCfg.Name).Msg("Attempting to delete dataset...")
		if err := dataset.Delete(ctx); err != nil {
			if strings.Contains(err.Error(), "notFound") {
				m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset not found, skipping.")
			} else if strings.Contains(err.Error(), "still contains resources") {
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Dataset not empty.")
			} else {
				m.logger.Error().Err(err).Str("dataset", dsCfg.Name).Msg("Failed to delete dataset")
			}
		} else {
			m.logger.Info().Str("dataset", dsCfg.Name).Msg("Dataset deleted.")
		}
	}
	return nil
}
