package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Replace "your_module_path/servicemanager/initialization" with your actual module path
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/initialization"
)

// Helper function to create a sample TopLevelConfig for testing
func createTestConfig() *initialization.TopLevelConfig {
	return &initialization.TopLevelConfig{
		DefaultProjectID: "default-proj",
		DefaultLocation:  "us-central1",
		Environments: map[string]initialization.EnvironmentSpec{
			"test": {
				ProjectID:       "test-proj",
				DefaultLocation: "europe-west1",
				DefaultLabels:   map[string]string{"env-label": "test"},
			},
			"prod": {
				ProjectID:          "prod-proj",
				TeardownProtection: true,
			},
		},
		Resources: initialization.ResourcesSpec{
			PubSubTopics: []initialization.PubSubTopic{
				{Name: "topic-a", Labels: map[string]string{"topic": "A"}, ProducerService: "service-alpha"},
				{Name: "topic-b", Labels: map[string]string{"topic": "B"}, ProducerService: "service-beta"},
				{Name: "shared-topic", ProducerService: "service-alpha"}, // Also produced by alpha
			},
			PubSubSubscriptions: []initialization.PubSubSubscription{
				{Name: "sub-alpha-for-topic-a", Topic: "topic-a", AckDeadlineSeconds: 30, ConsumerService: "service-alpha"},
				{Name: "sub-beta-for-topic-b", Topic: "topic-b", ConsumerService: "service-beta"},
				{Name: "sub-gamma-for-shared", Topic: "shared-topic", ConsumerService: "service-gamma"},
			},
			GCSBuckets: []initialization.GCSBucket{
				{Name: "alpha-bucket", Location: "europe-west1", AccessingServices: []string{"service-alpha", "service-gamma"}},
				{Name: "beta-bucket", AccessingServices: []string{"service-beta"}},
			},
			BigQueryTables: []initialization.BigQueryTable{
				{Name: "alpha_table", Dataset: "alpha_dataset", AccessingServices: []string{"service-alpha"}},
				{Name: "shared_table", Dataset: "common_dataset", AccessingServices: []string{"service-alpha", "service-beta"}},
			},
		},
	}
}

func TestConfigServer_GetServiceConfigHandler(t *testing.T) {
	logger := zerolog.Nop()
	testConfig := createTestConfig()
	configServer := NewConfigServer(testConfig, logger)

	// Setup test server
	handler := http.HandlerFunc(configServer.GetServiceConfigHandler)

	testCases := []struct {
		name               string
		serviceNameQuery   string
		envQuery           string
		expectedStatusCode int
		expectedResponse   *ServiceConfigurationResponse // Pointer to allow nil for error cases
		errorContains      string                        // For checking error messages in HTTP response body for 4xx/5xx
	}{
		{
			name:               "Successful config retrieval for service-alpha in test env",
			serviceNameQuery:   "service-alpha",
			envQuery:           "test",
			expectedStatusCode: http.StatusOK,
			expectedResponse: &ServiceConfigurationResponse{
				ServiceName:  "service-alpha",
				Environment:  "test",
				GCPProjectID: "test-proj",
				PublishesToTopics: []ServiceResourceInfo{
					{Name: "projects/test-proj/topics/topic-a", Labels: map[string]string{"topic": "A"}},
					{Name: "projects/test-proj/topics/shared-topic", Labels: nil},
				},
				ConsumesFromSubscriptions: []ServiceSubscriptionInfo{
					{Name: "projects/test-proj/subscriptions/sub-alpha-for-topic-a", Topic: "projects/test-proj/topics/topic-a", AckDeadlineSeconds: 30, Labels: nil},
				},
				AccessesGCSBuckets: []ServiceGCSBucketInfo{
					{Name: "alpha-bucket", Location: "europe-west1", DeclaredAccess: []string{"service-alpha", "service-gamma"}},
				},
				AccessesBigQueryTables: []ServiceBigQueryTableInfo{
					{ProjectID: "test-proj", DatasetID: "alpha_dataset", TableID: "alpha_table", DeclaredAccess: []string{"service-alpha"}},
					{ProjectID: "test-proj", DatasetID: "common_dataset", TableID: "shared_table", DeclaredAccess: []string{"service-alpha", "service-beta"}},
				},
			},
		},
		{
			name:               "Successful config retrieval for service-beta in prod env",
			serviceNameQuery:   "service-beta",
			envQuery:           "prod",
			expectedStatusCode: http.StatusOK,
			expectedResponse: &ServiceConfigurationResponse{
				ServiceName:  "service-beta",
				Environment:  "prod",
				GCPProjectID: "prod-proj",
				PublishesToTopics: []ServiceResourceInfo{
					{Name: "projects/prod-proj/topics/topic-b", Labels: map[string]string{"topic": "B"}},
				},
				ConsumesFromSubscriptions: []ServiceSubscriptionInfo{
					{Name: "projects/prod-proj/subscriptions/sub-beta-for-topic-b", Topic: "projects/prod-proj/topics/topic-b", Labels: nil},
				},
				AccessesGCSBuckets: []ServiceGCSBucketInfo{
					// beta-bucket has no explicit location, should use prod env default (none specified) then global default
					{Name: "beta-bucket", Location: "us-central1", DeclaredAccess: []string{"service-beta"}},
				},
				AccessesBigQueryTables: []ServiceBigQueryTableInfo{
					{ProjectID: "prod-proj", DatasetID: "common_dataset", TableID: "shared_table", DeclaredAccess: []string{"service-alpha", "service-beta"}},
				},
			},
		},
		{
			name:               "Service with no specific resources (service-delta)",
			serviceNameQuery:   "service-delta", // Not explicitly in config resources
			envQuery:           "test",
			expectedStatusCode: http.StatusOK,
			expectedResponse: &ServiceConfigurationResponse{ // Expect empty lists for resources
				ServiceName:               "service-delta",
				Environment:               "test",
				GCPProjectID:              "test-proj",
				PublishesToTopics:         []ServiceResourceInfo{},
				ConsumesFromSubscriptions: []ServiceSubscriptionInfo{},
				AccessesGCSBuckets:        []ServiceGCSBucketInfo{},
				AccessesBigQueryTables:    []ServiceBigQueryTableInfo{},
			},
		},
		{
			name:               "Missing serviceName query parameter",
			serviceNameQuery:   "",
			envQuery:           "test",
			expectedStatusCode: http.StatusBadRequest,
			errorContains:      "query parameter 'serviceName' is required",
		},
		{
			name:               "Missing env query parameter",
			serviceNameQuery:   "service-alpha",
			envQuery:           "",
			expectedStatusCode: http.StatusBadRequest,
			errorContains:      "query parameter 'env' is required",
		},
		{
			name:               "Environment not found in config",
			serviceNameQuery:   "service-alpha",
			envQuery:           "staging", // Not in testConfig.Environments
			expectedStatusCode: http.StatusInternalServerError,
			errorContains:      "Failed to determine project ID for environment 'staging'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", fmt.Sprintf("/config?serviceName=%s&env=%s", tc.serviceNameQuery, tc.envQuery), nil)
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code, "HTTP status code mismatch")

			if tc.expectedStatusCode == http.StatusOK {
				require.NotNil(t, tc.expectedResponse, "expectedResponse should not be nil for OK status")
				var actualResponse ServiceConfigurationResponse
				err = json.Unmarshal(rr.Body.Bytes(), &actualResponse)
				require.NoError(t, err, "Failed to unmarshal response body")

				// Sort slices for consistent comparison, as order doesn't matter for these lists
				// (Actual implementation might not sort, but for testing equality, it's helpful)
				// For simplicity, we'll rely on testify's ElementsMatch for slices if direct assert.Equal fails
				// or manually sort if needed. Here, we'll try direct comparison first.

				assert.Equal(t, tc.expectedResponse.ServiceName, actualResponse.ServiceName)
				assert.Equal(t, tc.expectedResponse.Environment, actualResponse.Environment)
				assert.Equal(t, tc.expectedResponse.GCPProjectID, actualResponse.GCPProjectID)

				assert.ElementsMatch(t, tc.expectedResponse.PublishesToTopics, actualResponse.PublishesToTopics, "PublishesToTopics mismatch")
				assert.ElementsMatch(t, tc.expectedResponse.ConsumesFromSubscriptions, actualResponse.ConsumesFromSubscriptions, "ConsumesFromSubscriptions mismatch")
				assert.ElementsMatch(t, tc.expectedResponse.AccessesGCSBuckets, actualResponse.AccessesGCSBuckets, "AccessesGCSBuckets mismatch")
				assert.ElementsMatch(t, tc.expectedResponse.AccessesBigQueryTables, actualResponse.AccessesBigQueryTables, "AccessesBigQueryTables mismatch")

			} else {
				if tc.errorContains != "" {
					assert.Contains(t, rr.Body.String(), tc.errorContains, "HTTP response body error message mismatch")
				}
			}
		})
	}
}
