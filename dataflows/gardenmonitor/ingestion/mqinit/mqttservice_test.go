package mqinit_test

import (
	"context"
	"ingestion/mqinit"
	"net/http"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// MockPublisher is a no-op publisher for testing purposes.
type MockPublisher struct{}

func (m *MockPublisher) Publish(ctx context.Context, topic string, payload []byte, attributes map[string]string) error {
	return nil
}
func (m *MockPublisher) Stop() {}

// TestServerStartup verifies that the HTTP server starts and the healthz endpoint is available.
func TestServerStartup(t *testing.T) {
	// --- Setup ---
	// Use a test logger that doesn't write to stdout unless the test fails.
	logger := zerolog.Nop()

	// Create a mock configuration.
	cfg := &mqinit.Config{
		HTTPPort: ":0", // Use port 0 to let the OS pick an available port.
	}

	// Create a mock ingestion service that doesn't connect to a real MQTT broker.
	// We pass an empty MQTTClientConfig to prevent the connection attempt.
	ingestionService := mqttconverter.NewIngestionService(
		&MockPublisher{},
		nil,
		logger,
		mqttconverter.DefaultIngestionServiceConfig(),
		mqttconverter.MQTTClientConfig{},
	)

	server := mqinit.NewServer(cfg, ingestionService, logger)

	// --- Run the server in a goroutine ---
	go func() {
		// We expect this to block until Shutdown is called, so any error is a test failure.
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			t.Errorf("server.Start() returned an unexpected error: %v", err)
		} else {
			t.Logf("server.Start()")
		}
	}()
	// Ensure server is shut down at the end of the test.
	t.Cleanup(server.Shutdown)

	// --- Verification ---
	// We need to wait a moment for the server to start listening.
	// We will poll the healthz endpoint until it responds or we time out.
	healthzURL := "http://" + server.GetListenerAddr() + "/healthz"

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
				// Server not ready yet, continue polling.
				continue
			}
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode, "expected healthz to return 200 OK")

			// Success! The server is up and responding.
			return
		}
	}
}
