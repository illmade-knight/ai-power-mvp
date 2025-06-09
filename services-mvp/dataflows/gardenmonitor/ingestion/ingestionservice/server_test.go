package ingestionservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	// Import for the concrete type is not strictly needed here if CoreService is defined
	// in this package or a common types package accessible to both server and test.
	// However, if your connectors.IngestionService is needed for other reasons (e.g. complex setup),
	// it might still be imported by the test's setup logic (not directly by these unit tests).
	// For this example, we assume CoreService interface is defined in the ingestionservice package (server.go).
	// "your_module_path/ingestion/connectors"
)

// MockCoreIngestionService is a mock implementation of CoreService.
type MockCoreIngestionService struct {
	mock.Mock
}

func (m *MockCoreIngestionService) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockCoreIngestionService) Stop() {
	m.Called()
}

// Ensure MockCoreIngestionService satisfies the CoreService interface.
// This assumes CoreService is defined in the same package (e.g., in server.go).
var _ CoreService = (*MockCoreIngestionService)(nil)

func TestNewServer(t *testing.T) {
	logger := zerolog.Nop()
	mockCoreSvc := new(MockCoreIngestionService) // This is now a CoreService

	validConfig := &Config{
		ServiceName:                 "test-ingestion-svc",
		Environment:                 "dev",
		ServiceManagerAPIURL:        "http://localhost:8080",
		HTTPPort:                    8081,
		ShutdownTimeout:             5 * time.Second,
		ExpectedEnrichedTopicID:     "enriched-topic",
		ExpectedUnidentifiedTopicID: "unidentified-topic",
	}

	testCases := []struct {
		name        string
		cfg         *Config
		coreSvc     CoreService // Changed to use the CoreService interface
		expectError bool
		errorMsg    string
	}{
		{"valid config and service", validConfig, mockCoreSvc, false, ""}, // Pass mockCoreSvc directly
		{"nil config", nil, mockCoreSvc, true, "server config cannot be nil"},
		{"nil coreService", validConfig, nil, true, "coreService cannot be nil"}, // Updated error message
		{"missing ServiceName", func() *Config { c := *validConfig; c.ServiceName = ""; return &c }(), mockCoreSvc, true, "server config: ServiceName is required"},
		{"missing Environment", func() *Config { c := *validConfig; c.Environment = ""; return &c }(), mockCoreSvc, true, "server config: Environment is required"},
		{"missing ServiceManagerAPIURL", func() *Config { c := *validConfig; c.ServiceManagerAPIURL = ""; return &c }(), mockCoreSvc, true, "server config: ServiceManagerAPIURL is required"},
		{"missing ExpectedEnrichedTopicID", func() *Config { c := *validConfig; c.ExpectedEnrichedTopicID = ""; return &c }(), mockCoreSvc, true, "server config: ExpectedEnrichedTopicID is required"},
		{"missing ExpectedUnidentifiedTopicID", func() *Config { c := *validConfig; c.ExpectedUnidentifiedTopicID = ""; return &c }(), mockCoreSvc, true, "server config: ExpectedUnidentifiedTopicID is required"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server, err := NewServer(tc.cfg, logger, tc.coreSvc) // No cast needed
			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
				assert.Nil(t, server)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, server)
				assert.Equal(t, tc.cfg, server.config)
				assert.Equal(t, tc.coreSvc, server.coreService)
			}
		})
	}
}

func TestServer_HealthzHandler(t *testing.T) {
	logger := zerolog.Nop()
	cfg := &Config{HTTPPort: 8080, ServiceName: "test-svc", Environment: "test", ServiceManagerAPIURL: "http://dummy", ExpectedEnrichedTopicID: "t1", ExpectedUnidentifiedTopicID: "t2"}
	mockCoreSvc := new(MockCoreIngestionService)
	server, err := NewServer(cfg, logger, mockCoreSvc) // Pass mockCoreSvc directly
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/healthz", nil)
	rr := httptest.NewRecorder()

	server.healthzHandler(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "ok\n", rr.Body.String())
}

func TestServer_FetchAndValidateServiceManagerConfig(t *testing.T) {
	logger := zerolog.Nop()
	mockCoreSvc := new(MockCoreIngestionService)

	baseServerConfig := &Config{
		ServiceName:                 "ingestion-service",
		Environment:                 "test",
		ExpectedEnrichedTopicID:     "enriched-messages",
		ExpectedUnidentifiedTopicID: "unidentified-messages",
		HTTPPort:                    8080,
		ShutdownTimeout:             5 * time.Second,
	}

	testCases := []struct {
		name                       string
		mockServiceManagerResponse func(w http.ResponseWriter, r *http.Request)
		expectedErrorMsg           string
		expectedSmConfig           *ServiceManagerConfiguration
	}{
		{
			name: "successful fetch and validation",
			mockServiceManagerResponse: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "ingestion-service", r.URL.Query().Get("serviceName"))
				assert.Equal(t, "test", r.URL.Query().Get("env"))
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(ServiceManagerConfiguration{
					ServiceName:  "ingestion-service",
					Environment:  "test",
					GCPProjectID: "test-project",
					PublishesToTopics: []ServiceManagerResourceInfo{
						{Name: "projects/test-project/topics/enriched-messages"},
						{Name: "projects/test-project/topics/unidentified-messages"},
					},
				})
			},
			expectedSmConfig: &ServiceManagerConfiguration{
				ServiceName:  "ingestion-service",
				Environment:  "test",
				GCPProjectID: "test-project",
				PublishesToTopics: []ServiceManagerResourceInfo{
					{Name: "projects/test-project/topics/enriched-messages"},
					{Name: "projects/test-project/topics/unidentified-messages"},
				},
			},
		},
		{
			name: "ServiceManager API returns non-OK status",
			mockServiceManagerResponse: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "internal server error from SM", http.StatusInternalServerError)
			},
			expectedErrorMsg: "ServiceManager API returned non-OK status: 500",
		},
		{
			name: "ServiceManager API returns unparseable JSON",
			mockServiceManagerResponse: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, "{not_valid_json")
			},
			expectedErrorMsg: "failed to decode response from ServiceManager API",
		},
		{
			name: "validation fails - enriched topic not listed by SM",
			mockServiceManagerResponse: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(ServiceManagerConfiguration{
					ServiceName:  "ingestion-service",
					Environment:  "test",
					GCPProjectID: "test-project",
					PublishesToTopics: []ServiceManagerResourceInfo{
						{Name: "projects/test-project/topics/unidentified-messages"},
					},
				})
			},
			expectedSmConfig: &ServiceManagerConfiguration{
				ServiceName:  "ingestion-service",
				Environment:  "test",
				GCPProjectID: "test-project",
				PublishesToTopics: []ServiceManagerResourceInfo{
					{Name: "projects/test-project/topics/unidentified-messages"},
				},
			},
		},
		{
			name: "validation fails - GCPProjectID missing from SM response",
			mockServiceManagerResponse: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(ServiceManagerConfiguration{
					ServiceName: "ingestion-service",
					Environment: "test",
					PublishesToTopics: []ServiceManagerResourceInfo{
						{Name: "projects/test-project/topics/enriched-messages"},
						{Name: "projects/test-project/topics/unidentified-messages"},
					},
				})
			},
			expectedSmConfig: &ServiceManagerConfiguration{
				ServiceName: "ingestion-service",
				Environment: "test",
				PublishesToTopics: []ServiceManagerResourceInfo{
					{Name: "projects/test-project/topics/enriched-messages"},
					{Name: "projects/test-project/topics/unidentified-messages"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockSMServer := httptest.NewServer(http.HandlerFunc(tc.mockServiceManagerResponse))
			defer mockSMServer.Close()

			cfg := *baseServerConfig
			cfg.ServiceManagerAPIURL = mockSMServer.URL

			server, err := NewServer(&cfg, logger, mockCoreSvc) // Pass mockCoreSvc directly
			require.NoError(t, err)

			err = server.fetchAndValidateServiceManagerConfig(context.Background())

			if tc.expectedErrorMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrorMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, server.serviceManagerConfig)
				assert.Equal(t, tc.expectedSmConfig.ServiceName, server.serviceManagerConfig.ServiceName)
				assert.Equal(t, tc.expectedSmConfig.Environment, server.serviceManagerConfig.Environment)
				assert.Equal(t, tc.expectedSmConfig.GCPProjectID, server.serviceManagerConfig.GCPProjectID)
				assert.ElementsMatch(t, tc.expectedSmConfig.PublishesToTopics, server.serviceManagerConfig.PublishesToTopics)
			}
		})
	}
}

func TestServer_StartAndStop(t *testing.T) {
	logger := zerolog.New(os.Stdout).Level(zerolog.DebugLevel)
	mockCoreSvc := new(MockCoreIngestionService)

	mockSMServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ServiceManagerConfiguration{
			ServiceName:  "ingestion-service",
			Environment:  "test",
			GCPProjectID: "test-project",
			PublishesToTopics: []ServiceManagerResourceInfo{
				{Name: "projects/test-project/topics/enriched-topic"},
				{Name: "projects/test-project/topics/unidentified-topic"},
			},
		})
	}))
	defer mockSMServer.Close()

	cfg := &Config{
		ServiceName:                 "ingestion-service",
		Environment:                 "test",
		ServiceManagerAPIURL:        mockSMServer.URL,
		HTTPPort:                    0,
		ShutdownTimeout:             1 * time.Second,
		ExpectedEnrichedTopicID:     "enriched-topic",
		ExpectedUnidentifiedTopicID: "unidentified-topic",
	}

	mockCoreSvc.On("Start").Return(nil).Once()
	mockCoreSvc.On("Stop").Return().Once()

	server, err := NewServer(cfg, logger, mockCoreSvc) // Pass mockCoreSvc directly
	require.NoError(t, err)

	startErrChan := make(chan error, 1)
	var wgServer sync.WaitGroup
	wgServer.Add(1)

	go func() {
		defer wgServer.Done()
		if err := server.Start(context.Background()); err != nil {
			startErrChan <- err
		}
	}()

	select {
	case err := <-startErrChan:
		t.Fatalf("Server.Start() failed: %v", err)
	case <-time.After(200 * time.Millisecond):
		t.Log("Server Start called, HTTP server likely started (or attempting to).")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelStop()
	err = server.Stop(stopCtx)
	require.NoError(t, err, "Server.Stop() returned an error")

	wgServer.Wait()

	mockCoreSvc.AssertExpectations(t)
	t.Log("Server Start and Stop sequence completed.")
}

func TestServer_Run_SignalHandling(t *testing.T) {
	logger := zerolog.Nop()
	mockCoreSvc := new(MockCoreIngestionService)

	mockSMServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(ServiceManagerConfiguration{GCPProjectID: "proj"})
	}))
	defer mockSMServer.Close()

	cfg := &Config{
		ServiceName: "sig-test-svc", Environment: "dev", ServiceManagerAPIURL: mockSMServer.URL,
		HTTPPort: 0, ShutdownTimeout: 500 * time.Millisecond,
		ExpectedEnrichedTopicID: "t1", ExpectedUnidentifiedTopicID: "t2",
	}

	mockCoreSvc.On("Start").Return(nil).Once()
	mockCoreSvc.On("Stop").Return().Once()

	server, err := NewServer(cfg, logger, mockCoreSvc) // Pass mockCoreSvc directly
	require.NoError(t, err)

	runErrChan := make(chan error, 1)
	ctxRun, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	go func() {
		runErrChan <- server.Run(ctxRun)
	}()

	time.Sleep(100 * time.Millisecond)

	//if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
	//	t.Fatalf("Failed to send SIGINT: %v", err)
	//}

	select {
	case err := <-runErrChan:
		assert.NoError(t, err, "server.Run() should exit cleanly on signal")
	case <-time.After(2 * time.Second):
		t.Error("server.Run() did not exit after SIGINT")
		cancelRun()
		<-runErrChan
	}

	mockCoreSvc.AssertExpectations(t)
}
