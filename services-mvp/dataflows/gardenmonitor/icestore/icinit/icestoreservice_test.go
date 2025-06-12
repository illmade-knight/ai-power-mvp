package icinit_test

import (
	"bigquery/bqinit"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/consumers"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// NOTE: This test assumes the test mocks (MockMessageConsumer, etc.)
// have been moved to a shared test helper file.

// MockInserter is a no-op inserter that implements bqstore.DataBatchInserter
type MockInserter[T any] struct{}

func (m *MockInserter[T]) InsertBatch(ctx context.Context, items []*T) error { return nil }
func (m *MockInserter[T]) Close() error                                      { return nil }

// TestServerStartup verifies that the HTTP server starts and the healthz endpoint is available.
func TestServerStartup(t *testing.T) {
	// --- Setup ---
	logger := zerolog.Nop()

	// Config can be empty for this test as we aren't using the port from it.
	cfg := &bqinit.Config{}

	// Create mocks for the service dependencies.
	// Correctly instantiate the mock consumer using its constructor.
	mockConsumer := consumers.NewMockMessageConsumer(1) // Assumes this is defined in a shared test helper file
	mockInserter := &MockInserter[types.GardenMonitorPayload]{}
	mockDecoder := func(payload []byte) (*types.GardenMonitorPayload, error) { return nil, nil }

	batcher := bqstore.NewBatchInserter[types.GardenMonitorPayload](
		&bqstore.BatchInserterConfig{BatchSize: 1, FlushTimeout: 1 * time.Second},
		mockInserter,
		logger,
	)

	// Use the new, clean constructor to create the processing service.
	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorPayload](
		1, // numWorkers
		mockConsumer,
		batcher,
		mockDecoder,
		logger,
	)
	require.NoError(t, err)

	server := bqinit.NewServer(cfg, processingService, logger)

	// Start the non-HTTP parts of the service in a goroutine.
	go func() {
		// We only need to start the batch processing part.
		// We don't call server.Start() because httptest is handling the web server.
		if err := processingService.Start(); err != nil {
			t.Logf("processingService.Start() returned an error on shutdown: %v", err)
		}
	}()
	t.Cleanup(processingService.Stop) // Ensure the processing service is stopped.

	// --- Test Execution: Start the service and test the HTTP handler ---
	// We don't call server.Start() directly because it blocks on ListenAndServe.
	// Instead, we use the standard library's httptest package to test the handler.
	// This gives us a test server with a known URL and lifecycle.
	testHttpServer := httptest.NewServer(server.GetHandler())
	defer testHttpServer.Close()

	// --- Verification ---
	// Make a request to the healthz endpoint of the test server.
	healthzURL := testHttpServer.URL + "/healthz"
	resp, err := http.Get(healthzURL)
	require.NoError(t, err, "Request to healthz endpoint should not fail")
	defer resp.Body.Close()

	// Assert that we get a 200 OK response.
	require.Equal(t, http.StatusOK, resp.StatusCode, "expected healthz to return 200 OK")
}

// GetHandler is a helper method added to the Server for testability.
// It returns the server's HTTP handler so it can be tested with httptest.
// NOTE: You would need to add this method to your `bqinit.Server` struct:
/*
func (s *Server) GetHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthzHandler)
	return mux
}
*/
