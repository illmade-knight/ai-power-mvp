package icestore_test

import (
	"context"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/illmade-knight/ai-power-mpv/pkg/icestore"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks for icestore components ---

// MockDataUploader is a no-op implementation of the icestore.DataUploader interface.
type MockDataUploader[T any] struct{}

func (m *MockDataUploader[T]) UploadBatch(ctx context.Context, items []*T) error { return nil }
func (m *MockDataUploader[T]) Close() error                                      { return nil }

func TestNewIceStorageService(t *testing.T) {
	logger := zerolog.Nop()
	mockConsumer := &consumers.MockMessageConsumer{} // Assumes mock is in shared test helpers
	mockUploader := &MockDataUploader[icestore.ArchivalData]{}
	mockDecoder := func(payload []byte) (*icestore.ArchivalData, error) { return nil, nil }

	// A valid batcher is needed for the success case.
	validBatcher := icestore.NewBatcher[icestore.ArchivalData](
		&icestore.BatcherConfig{BatchSize: 1, FlushTimeout: 1 * time.Second},
		mockUploader,
		logger,
	)

	testCases := []struct {
		name          string
		numWorkers    int
		consumer      consumers.MessageConsumer
		batcher       *icestore.Batcher[icestore.ArchivalData]
		decoder       consumers.PayloadDecoder[icestore.ArchivalData]
		expectError   bool
		expectedError string
	}{
		{
			name:        "successful creation",
			numWorkers:  1,
			consumer:    mockConsumer,
			batcher:     validBatcher,
			decoder:     mockDecoder,
			expectError: false,
		},
		{
			name:          "error on nil consumer",
			numWorkers:    1,
			consumer:      nil,
			batcher:       validBatcher,
			decoder:       mockDecoder,
			expectError:   true,
			expectedError: "MessageConsumer cannot be nil",
		},
		{
			name:          "error on nil batcher",
			numWorkers:    1,
			consumer:      mockConsumer,
			batcher:       nil,
			decoder:       mockDecoder,
			expectError:   true,
			expectedError: "Batcher cannot be nil",
		},
		{
			name:          "error on nil decoder",
			numWorkers:    1,
			consumer:      mockConsumer,
			batcher:       validBatcher,
			decoder:       nil,
			expectError:   true,
			expectedError: "PayloadDecoder cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			service, err := icestore.NewIceStorageService[icestore.ArchivalData](
				tc.numWorkers,
				tc.consumer,
				tc.batcher,
				tc.decoder,
				logger,
			)

			// Assert
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
				assert.Nil(t, service)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, service)
			}
		})
	}
}
