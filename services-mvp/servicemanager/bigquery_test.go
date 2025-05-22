package servicemanager

import (
	"cloud.google.com/go/bigquery" // Still needed for concrete types in mock returns/args
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"os" // Required for os.Stdout for logger if not Nop
	"testing"

	// Import your generated protobuf package
	telemetry "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry"
)

// --- Mock Implementations for BQClient, BQDataset, BQTable interfaces ---

type MockBQTable struct {
	mock.Mock
}

func (m *MockBQTable) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { // Handle nil return for metadata if error is expected
		return nil, args.Error(1)
	}
	return args.Get(0).(*bigquery.TableMetadata), args.Error(1)
}
func (m *MockBQTable) Create(ctx context.Context, meta *bigquery.TableMetadata) error {
	args := m.Called(ctx, meta)
	return args.Error(0)
}
func (m *MockBQTable) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockBQDataset struct {
	mock.Mock
}

func (m *MockBQDataset) Metadata(ctx context.Context) (*bigquery.DatasetMetadata, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil { // Handle nil return for metadata if error is expected
		return nil, args.Error(1)
	}
	return args.Get(0).(*bigquery.DatasetMetadata), args.Error(1)
}
func (m *MockBQDataset) Create(ctx context.Context, meta *bigquery.DatasetMetadata) error {
	args := m.Called(ctx, meta)
	return args.Error(0)
}
func (m *MockBQDataset) Update(ctx context.Context, metaToUpdate bigquery.DatasetMetadataToUpdate, etag string) (*bigquery.DatasetMetadata, error) {
	args := m.Called(ctx, metaToUpdate, etag)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bigquery.DatasetMetadata), args.Error(1)
}
func (m *MockBQDataset) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
func (m *MockBQDataset) Table(tableID string) BQTable { // Returns our BQTable interface
	args := m.Called(tableID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(BQTable) // Expecting our interface type
}

type MockBQClient struct {
	mock.Mock
	projectID string
}

func (m *MockBQClient) Dataset(datasetID string) BQDataset { // Returns our BQDataset interface
	args := m.Called(datasetID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(BQDataset) // Expecting our interface type
}
func (m *MockBQClient) Project() string {
	args := m.Called()    // Track the call to Project()
	return args.String(0) // Return the projectID configured for this mock
}
func (m *MockBQClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Ensure mocks implement the interfaces
var _ BQTable = &MockBQTable{}
var _ BQDataset = &MockBQDataset{}
var _ BQClient = &MockBQClient{}

// --- Helper to simulate "notFound" error ---
type notFoundError struct{ msg string }

func (e *notFoundError) Error() string { return e.msg }
func newNotFoundError(resourceType, resourceID string) error {
	return &notFoundError{msg: fmt.Sprintf("%s %s notFound", resourceType, resourceID)}
}

func TestBigQueryManager_Setup(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()
	testProjectID := "test-project-bq-manager"

	// Schema for MeterReading (inferred from the proto-generated struct)
	// This is used by the manager, so we need it for our Create mock's matcher if being specific.
	meterReadingSchemaForMatcher, errInfer := bigquery.InferSchema((*telemetry.MeterReading)(nil))
	require.NoError(t, errInfer)

	testConfig := &TopLevelConfig{
		DefaultProjectID: testProjectID,
		Environments: map[string]EnvironmentSpec{
			"test": {ProjectID: testProjectID, TeardownProtection: false},
		},
		Resources: ResourcesSpec{
			BigQueryDatasets: []BigQueryDataset{
				{Name: "dataset1", Description: "Test Dataset 1", Labels: map[string]string{"env": "test"}},
			},
			BigQueryTables: []BigQueryTable{
				{
					Name:                   "table1",
					Dataset:                "dataset1",
					Description:            "Test Table 1",
					SchemaSourceType:       "go_struct",
					SchemaSourceIdentifier: "github.com/illmade-knight/ai-power-mvp/gen/go/protos/telemetry.MeterReading",
					TimePartitioningField:  "original_mqtt_time",
					TimePartitioningType:   "DAY",
				},
			},
		},
	}

	t.Run("Create new dataset and table", func(t *testing.T) {
		mockClient := new(MockBQClient)
		// mockClient.projectID = testProjectID // No longer needed if Project() mock returns it
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)

		// Setup expectations
		mockClient.On("Project").Return(testProjectID).Once()           // Expect Project() to be called once
		mockClient.On("Dataset", "dataset1").Return(mockDataset).Once() // Expect Dataset("dataset1") once for dataset setup

		mockDataset.On("Metadata", ctx).Return(nil, newNotFoundError("Dataset", "dataset1")).Once()
		mockDataset.On("Create", ctx, mock.MatchedBy(func(meta *bigquery.DatasetMetadata) bool {
			return meta.Name == "dataset1" && meta.Description == "Test Dataset 1" && meta.Labels["env"] == "test"
		})).Return(nil).Once()
		// Expect Dataset("dataset1") again for table setup, then Table("table1")
		mockClient.On("Dataset", "dataset1").Return(mockDataset).Once()
		mockDataset.On("Table", "table1").Return(mockTable).Once()

		mockTable.On("Metadata", ctx).Return(nil, newNotFoundError("Table", "table1")).Once()
		mockTable.On("Create", ctx, mock.MatchedBy(func(meta *bigquery.TableMetadata) bool {
			return meta.Name == "table1" &&
				len(meta.Schema) == len(meterReadingSchemaForMatcher) &&
				meta.TimePartitioning != nil && meta.TimePartitioning.Field == "original_mqtt_time" &&
				meta.TimePartitioning.Type == bigquery.DayPartitioningType
		})).Return(nil).Once()

		manager, err := NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Setup(ctx, testConfig, "test")
		require.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})

	t.Run("Dataset and table already exist, update dataset", func(t *testing.T) {
		mockClient := new(MockBQClient)
		// mockClient.projectID = testProjectID
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)

		mockClient.On("Project").Return(testProjectID).Once()
		mockClient.On("Dataset", "dataset1").Return(mockDataset).Once() // For dataset check

		existingDatasetMeta := &bigquery.DatasetMetadata{
			Name: "dataset1", Description: "Old Desc", Labels: map[string]string{"env": "old"},
		}
		mockDataset.On("Metadata", ctx).Return(existingDatasetMeta, nil).Once()
		mockDataset.On("Update", ctx, mock.AnythingOfType("bigquery.DatasetMetadataToUpdate"), "").Return(nil, nil).Once()

		// Expect Dataset("dataset1") again for table setup, then Table("table1")
		mockClient.On("Dataset", "dataset1").Return(mockDataset).Once()
		mockDataset.On("Table", "table1").Return(mockTable).Once()

		existingTableMeta := &bigquery.TableMetadata{Name: "table1" /* Schema: someSchema */}
		mockTable.On("Metadata", ctx).Return(existingTableMeta, nil).Once()
		// No Create call expected for table

		manager, err := NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Setup(ctx, testConfig, "test")
		require.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})

}

func TestBigQueryManager_Teardown(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.Disabled)
	ctx := context.Background()
	testProjectID := "test-project-bq-teardown"

	testConfig := &TopLevelConfig{
		DefaultProjectID: testProjectID,
		Environments: map[string]EnvironmentSpec{
			"test_teardown": {ProjectID: testProjectID, TeardownProtection: false},
			"prod_teardown": {ProjectID: testProjectID, TeardownProtection: true},
		},
		Resources: ResourcesSpec{
			BigQueryDatasets: []BigQueryDataset{{Name: "dataset_to_delete"}},
			BigQueryTables:   []BigQueryTable{{Name: "table_to_delete", Dataset: "dataset_to_delete"}},
		},
	}

	t.Run("Successful teardown", func(t *testing.T) {
		mockClient := new(MockBQClient)
		// mockClient.projectID = testProjectID
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)

		mockClient.On("Project").Return(testProjectID).Once()                     // Project check
		mockClient.On("Dataset", "dataset_to_delete").Return(mockDataset).Twice() // Once for table teardown, once for dataset teardown

		mockDataset.On("Table", "table_to_delete").Return(mockTable).Once()
		mockTable.On("Delete", ctx).Return(nil).Once()
		mockDataset.On("Delete", ctx).Return(nil).Once()

		manager, err := NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Teardown(ctx, testConfig, "test_teardown")
		require.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockDataset.AssertExpectations(t)
		mockTable.AssertExpectations(t)
	})

	t.Run("Teardown protection enabled", func(t *testing.T) {
		mockClient := new(MockBQClient)
		// mockClient.projectID = testProjectID
		mockClient.On("Project").Return(testProjectID).Once()

		manager, err := NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Teardown(ctx, testConfig, "prod_teardown")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "teardown protection enabled")

		mockClient.AssertCalled(t, "Project")
		mockClient.AssertNotCalled(t, "Dataset", mock.Anything)
	})

	t.Run("Table notFound during teardown is ignored", func(t *testing.T) {
		mockClient := new(MockBQClient)
		// mockClient.projectID = testProjectID
		mockDataset := new(MockBQDataset)
		mockTable := new(MockBQTable)

		mockClient.On("Project").Return(testProjectID).Once()
		mockClient.On("Dataset", "dataset_to_delete").Return(mockDataset).Twice()

		mockDataset.On("Table", "table_to_delete").Return(mockTable).Once()
		mockTable.On("Delete", ctx).Return(newNotFoundError("Table", "table_to_delete")).Once()
		mockDataset.On("Delete", ctx).Return(nil).Once()

		manager, err := NewBigQueryManager(mockClient, logger, nil)
		require.NoError(t, err)

		err = manager.Teardown(ctx, testConfig, "test_teardown")
		require.NoError(t, err)

		mockTable.AssertCalled(t, "Delete", ctx)
		mockDataset.AssertCalled(t, "Delete", ctx)
	})

}
