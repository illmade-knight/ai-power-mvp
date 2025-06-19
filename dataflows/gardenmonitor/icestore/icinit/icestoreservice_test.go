package icinit_test

import (
	"context"
	"github.com/illmade-knight/ai-power-mvp/dataflows/gardenmonitor/icestore/icinit"
	"github.com/illmade-knight/go-iot/pkg/consumers"
	"github.com/illmade-knight/go-iot/pkg/helpers/emulators"
	"github.com/illmade-knight/go-iot/pkg/icestore"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const (
	testProjectID  = "icestore-test-project"
	testTopicID    = "icestore-test-topic"
	testBucketName = "icestore-test-bucket"

	testBatchSize    = 5
	testFlushTimeout = time.Second * 10
)

// MockInserter is a no-op inserter that implements bqstore.DataBatchInserter
type MockInserter[T any] struct{}

func (m *MockInserter[T]) InsertBatch(ctx context.Context, items []*T) error { return nil }
func (m *MockInserter[T]) Close() error                                      { return nil }

// TestServerStartup verifies that the HTTP server starts and the healthz endpoint is available.
func TestServerStartup(t *testing.T) {
	// --- Setup ---
	logger := zerolog.Nop()

	ctx := context.Background()

	// IceServiceConfig can be empty for this test as we aren't using the port from it.
	gcsConfig := emulators.GetDefaultGCSConfig(testProjectID, testBucketName)
	gcsClient, gcsCleanup := emulators.SetupGCSEmulator(t, ctx, gcsConfig)
	defer gcsCleanup()

	// Create mocks for the service dependencies.
	// Correctly instantiate the mock consumer using its constructor.
	mockConsumer := consumers.NewMockMessageConsumer(1) // Assumes this is defined in a shared test helper file

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{BatchSize: testBatchSize, FlushTimeout: testFlushTimeout},
		icestore.GCSBatchUploaderConfig{BucketName: testBucketName, ObjectPrefix: "archived-data"},
		logger,
	)
	require.NoError(t, err)

	service, err := icestore.NewIceStorageService(2, mockConsumer, batcher, icestore.ArchivalTransformer, logger)
	require.NoError(t, err)

	cfg := &icinit.IceServiceConfig{}
	processingService := icinit.NewServer(cfg, service, logger)

	// Start the non-HTTP parts of the service in a goroutine.
	go func() {
		// We only need to start the batch processing part.
		// We don't call server.Start() because httptest is handling the web server.
		if err := processingService.Start(); err != nil {
			t.Logf("processingService.Start() returned an error on shutdown: %v", err)
		}
	}()
	t.Cleanup(processingService.Shutdown) // Ensure the processing service is stopped.

	// --- Test Execution: Start the service and test the HTTP handler ---
	// We don't call server.Start() directly because it blocks on ListenAndServe.
	// Instead, we use the standard library's httptest package to test the handler.
	// This gives us a test server with a known URL and lifecycle.
	testHttpServer := httptest.NewServer(processingService.GetHandler())
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
