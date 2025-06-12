package bqinit_test

import (
	"bigquery/bqinit"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/bqstore"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// MockMessageConsumer is a no-op consumer for testing purposes.
type MockMessageConsumer struct{}

func (m *MockMessageConsumer) Messages() <-chan types.ConsumedMessage { return nil }
func (m *MockMessageConsumer) Start(ctx context.Context) error        { return nil }
func (m *MockMessageConsumer) Stop() error                            { return nil }
func (m *MockMessageConsumer) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

// MockInserter is a no-op inserter for testing purposes.
type MockInserter[T any] struct{}

func (m *MockInserter[T]) Insert(ctx context.Context, items []*T) error { return nil }

// TestServerStartup verifies that the HTTP server starts and the healthz endpoint is available.
func TestServerStartup(t *testing.T) {
	// --- Setup ---
	logger := zerolog.Nop()

	cfg := &bqinit.Config{
		HTTPPort: ":0", // Use port 0 to let the OS pick an available port.
	}

	// Create a mock processing service that doesn't connect to real GCP services.
	batcher := bqstore.NewBatchInserter[types.GardenMonitorPayload](
		&bqstore.BatchInserterConfig{},
		MockInserter[types.GardenMonitorPayload]{},
		logger,
	)

	processingService, err := bqstore.NewBatchingService[types.GardenMonitorPayload](
		&bqstore.ServiceConfig{},
		&MockMessageConsumer{},
		batcher,
		types.GardenMonitorDecoder,
		logger,
	)
	require.NoError(t, err)

	server := bqinit.NewServer(cfg, processingService, logger)

	// --- Run the server in a goroutine ---
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("server.Start() returned an unexpected error: %v", err)
		}
	}()
	t.Cleanup(server.Shutdown)

	// --- Verification ---
	healthzURL := "http://localhost:" + server.GetHTTPPort() + "/healthz"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for healthz endpoint to be available at %s", healthzURL)
		case <-ticker.C:
			resp, err := http.Get(healthzURL)
			if err != nil {
				continue
			}
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode, "expected healthz to return 200 OK")

			// Success!
			return
		}
	}
}
