package xdevice

import (
	"context"
	"errors"
	"testing"
	"time"

	// "cloud.google.com/go/bigquery" // No longer needed for these specific unit tests of the interface user
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- MockDecodedDataInserter ---
// This is a mock implementation of the DecodedDataInserter interface using testify/mock.
type MockDecodedDataInserter struct {
	mock.Mock
}

// Insert provides a mock function with given fields: ctx, reading
func (m *MockDecodedDataInserter) Insert(ctx context.Context, reading MeterReading) error {
	args := m.Called(ctx, reading)
	return args.Error(0)
}

// Close provides a mock function with given fields:
func (m *MockDecodedDataInserter) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Ensure MockDecodedDataInserter satisfies the DecodedDataInserter interface
var _ DecodedDataInserter = &MockDecodedDataInserter{}

func TestLoadBigQueryInserterConfigFromEnv(t *testing.T) {
	t.Run("Valid config", func(t *testing.T) {
		// t.Setenv automatically restores the original environment variable value after the test.
		t.Setenv("GCP_PROJECT_ID", "test-project-id")
		t.Setenv("BQ_DATASET_ID", "test-dataset-id")
		t.Setenv("BQ_TABLE_ID_METER_READINGS", "test-meter-readings-table")
		// No defer os.Unsetenv needed

		cfg, err := LoadBigQueryInserterConfigFromEnv()
		require.NoError(t, err)
		assert.Equal(t, "test-project-id", cfg.ProjectID)
		assert.Equal(t, "test-dataset-id", cfg.DatasetID)
		assert.Equal(t, "test-meter-readings-table", cfg.TableID)
	})

	t.Run("Missing project ID", func(t *testing.T) {
		// Store original value if needed for other tests, though t.Setenv handles cleanup for *this* test.
		// originalProjectID := os.Getenv("GCP_PROJECT_ID")
		t.Setenv("GCP_PROJECT_ID", "") // Set to empty or use t.Unsetenv if available and desired
		t.Setenv("BQ_DATASET_ID", "test-d")
		t.Setenv("BQ_TABLE_ID_METER_READINGS", "test-t")
		// defer os.Setenv("GCP_PROJECT_ID", originalProjectID) // Not needed with t.Setenv

		_, err := LoadBigQueryInserterConfigFromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "GCP_PROJECT_ID")
	})

	t.Run("Missing dataset ID", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "test-p")
		t.Setenv("BQ_DATASET_ID", "")
		t.Setenv("BQ_TABLE_ID_METER_READINGS", "test-t")

		_, err := LoadBigQueryInserterConfigFromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "BQ_DATASET_ID")
	})

	t.Run("Missing table ID", func(t *testing.T) {
		t.Setenv("GCP_PROJECT_ID", "test-p")
		t.Setenv("BQ_DATASET_ID", "test-d")
		t.Setenv("BQ_TABLE_ID_METER_READINGS", "")

		_, err := LoadBigQueryInserterConfigFromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "BQ_TABLE_ID_METER_READINGS")
	})
}

// --- Test for a component USING the DecodedDataInserter ---

// ExampleProcessor is a hypothetical component that uses a DecodedDataInserter.
// In a real scenario, this would be your service that consumes from Pub/Sub, decodes,
// and then uses the inserter.
type ExampleProcessor struct {
	inserter DecodedDataInserter
	logger   zerolog.Logger
}

func NewExampleProcessor(inserter DecodedDataInserter, logger zerolog.Logger) *ExampleProcessor {
	return &ExampleProcessor{inserter: inserter, logger: logger}
}

// ProcessAndStore simulates processing a decoded payload and storing it.
func (p *ExampleProcessor) ProcessAndStore(ctx context.Context, reading MeterReading) error {
	p.logger.Info().Str("uid", reading.UID).Msg("Processing meter reading for storage")
	// Potentially more logic here before inserting...
	err := p.inserter.Insert(ctx, reading)
	if err != nil {
		p.logger.Error().Err(err).Str("uid", reading.UID).Msg("Failed to store meter reading")
		return err
	}
	p.logger.Info().Str("uid", reading.UID).Msg("Meter reading stored successfully")
	return nil
}

func (p *ExampleProcessor) Shutdown() error {
	p.logger.Info().Msg("ExampleProcessor shutting down, closing inserter.")
	return p.inserter.Close()
}

// TestExampleProcessor_ProcessAndStore tests the ExampleProcessor's interaction
// with the DecodedDataInserter interface using a mock.
func TestExampleProcessor_ProcessAndStore(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()

	mockInserter := new(MockDecodedDataInserter) // Create an instance of our mock

	// Create the component we want to test, injecting the mock
	processor := NewExampleProcessor(mockInserter, logger)

	testReading := NewMeterReading( // Using the existing helper
		"UID123",
		10.5,
		1.1,
		2.2,
		230.0,
		229.5,
		ConsumedUpstreamMessage{
			DeviceEUI:          "EUI_XYZ",
			ClientID:           "ClientTest",
			LocationID:         "LocationTest",
			DeviceCategory:     "TestCategory",
			OriginalMQTTTime:   time.Now().Add(-1 * time.Hour),
			IngestionTimestamp: time.Now().Add(-30 * time.Minute),
		},
		"XDeviceTest",
	)

	t.Run("Successful insert", func(t *testing.T) {
		// Use mock.Anything for the context argument for more robust matching.
		mockInserter.On("Insert", mock.Anything, testReading).Return(nil).Once()

		err := processor.ProcessAndStore(ctx, testReading)
		require.NoError(t, err)

		mockInserter.AssertExpectations(t)
	})

	t.Run("Inserter returns error", func(t *testing.T) {
		expectedError := errors.New("simulated inserter error")
		// Use mock.Anything for the context argument.
		mockInserter.On("Insert", mock.Anything, testReading).Return(expectedError).Once()

		err := processor.ProcessAndStore(ctx, testReading)
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedError), "Expected error to be the simulated inserter error")

		mockInserter.AssertExpectations(t)
	})

	t.Run("Shutdown calls inserter Close", func(t *testing.T) {
		mockInserter.On("Close").Return(nil).Once()

		err := processor.Shutdown()
		require.NoError(t, err)
		mockInserter.AssertExpectations(t)
	})

	t.Run("Shutdown handles inserter Close error", func(t *testing.T) {
		expectedCloseError := errors.New("simulated close error")
		mockInserter.On("Close").Return(expectedCloseError).Once()

		err := processor.Shutdown()
		require.Error(t, err)
		assert.True(t, errors.Is(err, expectedCloseError))
		mockInserter.AssertExpectations(t)
	})
}

// Note: The actual BigQueryInserter implementation (NewBigQueryInserter, its Insert, and Close methods)
// would be tested more thoroughly with integration tests using the BigQuery emulator or a dedicated test project,
// as mocking the concrete Google Cloud client libraries at a low level is complex and often less valuable
// than testing the real interaction. This file now focuses on unit testing components that *use* the
// DecodedDataInserter interface.
