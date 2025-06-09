package bigquery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// TestBigQueryInserter_WithMockServer tests the BigQueryInserter by providing a mock
// HTTP server that emulates the BigQuery API. This avoids changing the source
// code to introduce interfaces.
func TestBigQueryInserter_WithMockServer(t *testing.T) {
	// --- Test Setup ---
	ctx := context.Background()
	logger := zerolog.Nop()
	var lastInsertRequest string

	// This mock server will act as a fake BigQuery API backend.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set content type for all JSON responses.
		w.Header().Set("Content-Type", "application/json")

		// Route requests based on their path to simulate different API endpoints.
		if strings.Contains(r.URL.Path, "insertAll") {
			// Read the body to inspect its content and store it for assertions.
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error": {"message": "failed to read request body"}}`))
				return
			}
			lastInsertRequest = string(bodyBytes)

			// The BigQuery client wraps the payload in a structure. We need to unmarshal it
			// to check for our trigger values.
			var insertReq struct {
				Rows []struct {
					JSON GardenMonitorPayload `json:"json"`
				} `json:"rows"`
			}
			// We only unmarshal to check for error triggers, so we ignore errors here.
			_ = json.Unmarshal(bodyBytes, &insertReq)

			if len(insertReq.Rows) > 0 {
				uid := insertReq.Rows[0].JSON.UID
				// Trigger a multi-error response based on a specific UID.
				if uid == "trigger-multi-error" {
					w.WriteHeader(http.StatusOK) // The overall request is OK, but contains errors.
					_, _ = w.Write([]byte(`{
						"insertErrors": [
							{
								"index": 0,
								"errors": [{"reason": "invalid", "message": "Invalid field value for testing"}]
							}
						]
					}`))
					return
				}
				// Trigger a generic API error based on another specific UID.
				if uid == "trigger-api-error" {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error": {"message": "Internal server error for testing"}}`))
					return
				}
			}

			// Default success case.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`)) // Empty JSON object for success.

		} else if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "tables") {
			// This is the endpoint for getting table metadata, called by NewBigQueryInserter.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"schema": {
					"fields": [{"name": "uid", "type": "STRING"}]
				}
			}`))
		} else {
			// Handle any unexpected requests.
			w.WriteHeader(http.StatusNotFound)
			t.Errorf("Unexpected request to mock server: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer mockServer.Close()

	// --- Test Execution ---

	// Create a real BigQuery client, but configure it to talk to our mock server.
	opts := []option.ClientOption{
		option.WithEndpoint(mockServer.URL),
		option.WithoutAuthentication(),
		option.WithHTTPClient(mockServer.Client()), // Ensures the server's test transport is used.
	}
	client, err := bigquery.NewClient(ctx, "test-project", opts...)
	require.NoError(t, err, "Failed to create client pointed at mock server")

	// Configuration for the inserter.
	config := &BigQueryInserterConfig{
		ProjectID: "test-project",
		DatasetID: "test_dataset",
		TableID:   "test_table",
	}

	t.Run("successful insert", func(t *testing.T) {
		// Create a new inserter for this sub-test to ensure isolation.
		inserter, err := NewBigQueryInserter(ctx, client, config, logger)
		require.NoError(t, err)

		reading := &GardenMonitorPayload{
			UID: "device-007",
		}
		err = inserter.InsertBatch(ctx, []*GardenMonitorPayload{reading})
		assert.NoError(t, err)
		assert.Contains(t, lastInsertRequest, `"uid":"device-007"`, "The request body should contain the correct data.")
	})

}
