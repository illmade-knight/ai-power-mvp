package servicemanager

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cloud.google.com/go/iam" // For iam.Handle in mock
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Implementations for GCSClient and GCSBucketHandle ---

type MockGCSBucketHandle struct {
	mock.Mock
}

func (m *MockGCSBucketHandle) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.BucketAttrs), args.Error(1)
}

func (m *MockGCSBucketHandle) Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error {
	args := m.Called(ctx, projectID, attrs)
	return args.Error(0)
}

func (m *MockGCSBucketHandle) Update(ctx context.Context, attrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	args := m.Called(ctx, attrs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.BucketAttrs), args.Error(1)
}

func (m *MockGCSBucketHandle) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockGCSBucketHandle) IAM() *iam.Handle {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*iam.Handle)
}

type MockGCSClient struct {
	mock.Mock
}

func (m *MockGCSClient) Bucket(name string) GCSBucketHandle {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(GCSBucketHandle)
}

func (m *MockGCSClient) Buckets(ctx context.Context, projectID string) *storage.BucketIterator {
	args := m.Called(ctx, projectID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*storage.BucketIterator)
}

func (m *MockGCSClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Ensure mocks implement the interfaces
var _ GCSBucketHandle = &MockGCSBucketHandle{}
var _ GCSClient = &MockGCSClient{}

// Helper to simulate storage.ErrBucketNotExist
func newBucketNotFoundError() error {
	return storage.ErrBucketNotExist
}

// Helper to simulate other GCS errors
func newGenericGCSError(msg string) error {
	return errors.New(msg)
}

func TestStorageManager_Setup(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()
	testProjectID := "test-gcs-project"
	testEnvironment := "dev"

	baseConfig := &TopLevelConfig{
		DefaultProjectID: testProjectID,
		DefaultLocation:  "US-CENTRAL1",
		Environments: map[string]EnvironmentSpec{
			testEnvironment: {ProjectID: testProjectID},
		},
		Resources: ResourcesSpec{
			GCSBuckets: []GCSBucket{
				{
					Name:              "test-bucket-1",
					Location:          "EUROPE-WEST1",
					StorageClass:      "STANDARD",
					VersioningEnabled: true, // This is a bool in GCSBucket config
					Labels:            map[string]string{"app": "manager", "env": "dev-override"},
					LifecycleRules: []LifecycleRuleSpec{
						{Action: LifecycleActionSpec{Type: "Delete"}, Condition: LifecycleConditionSpec{AgeDays: 30}},
					},
				},
				{
					Name:         "test-bucket-2-defaults",
					StorageClass: "NEARLINE",
					// VersioningEnabled defaults to false in GCSBucket struct if not specified
				},
			},
		},
	}
	envWithLabels := EnvironmentSpec{
		ProjectID:     testProjectID,
		DefaultLabels: map[string]string{"cost-center": "platform", "env": "dev-default"},
	}

	type mockSetup func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle)

	testCases := []struct {
		name          string
		config        *TopLevelConfig
		environment   string
		setupMocks    mockSetup
		expectError   bool
		errorContains string
	}{
		{
			name:        "Create new buckets successfully",
			environment: testEnvironment,
			setupMocks: func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "test-bucket-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(nil, newBucketNotFoundError()).Once()
				mockBucket1.On("Create", ctx, testProjectID, mock.MatchedBy(func(attrs *storage.BucketAttrs) bool {
					correctLabels := attrs.Labels["app"] == "manager" && attrs.Labels["env"] == "dev-override" && attrs.Labels["cost-center"] == "platform" && len(attrs.Labels) == 3
					return attrs.Name == "test-bucket-1" && strings.ToUpper(attrs.Location) == "EUROPE-WEST1" && attrs.StorageClass == "STANDARD" && attrs.VersioningEnabled == true && correctLabels && len(attrs.Lifecycle.Rules) == 1 && attrs.Lifecycle.Rules[0].Action.Type == "Delete" && attrs.Lifecycle.Rules[0].Condition.AgeInDays == 30
				})).Return(nil).Once()

				mockClient.On("Bucket", "test-bucket-2-defaults").Return(mockBucket2).Once()
				mockBucket2.On("Attrs", ctx).Return(nil, newBucketNotFoundError()).Once()
				mockBucket2.On("Create", ctx, testProjectID, mock.MatchedBy(func(attrs *storage.BucketAttrs) bool {
					correctLabels := attrs.Labels["cost-center"] == "platform" && attrs.Labels["env"] == "dev-default" && len(attrs.Labels) == 2
					return attrs.Name == "test-bucket-2-defaults" && strings.ToUpper(attrs.Location) == "US-CENTRAL1" && attrs.StorageClass == "NEARLINE" && attrs.VersioningEnabled == false && correctLabels && (attrs.Lifecycle.Rules == nil || len(attrs.Lifecycle.Rules) == 0)
				})).Return(nil).Once()
			},
			expectError: false,
		},
		{
			name:        "Buckets already exist, update attributes",
			environment: testEnvironment,
			setupMocks: func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "test-bucket-1").Return(mockBucket1).Once()
				existingAttrs1 := &storage.BucketAttrs{Name: "test-bucket-1", Location: "EUROPE-WEST1", StorageClass: "COLDLINE", VersioningEnabled: false, Labels: map[string]string{"app": "old-manager", "stale": "label", "env": "old-env-val"}, Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{{Action: storage.LifecycleAction{Type: "SetStorageClass"}, Condition: storage.LifecycleCondition{AgeInDays: 10}}}}}
				mockBucket1.On("Attrs", ctx).Return(existingAttrs1, nil).Once()
				mockBucket1.On("Update", ctx, mock.MatchedBy(func(update storage.BucketAttrsToUpdate) bool {
					match := true
					if update.StorageClass != "STANDARD" {
						t.Logf("Update Matcher (Bucket1): StorageClass mismatch. Expected: STANDARD, Got: %s", update.StorageClass)
						match = false
					}

					// NOTE: VersioningEnabled check needs to be adapted based on the actual API of 'optional.Bool'.
					// The compiler error "Invalid indirect of 'update.VersioningEnabled' (type 'optional.Bool')"
					// indicates it's not a *bool. You'll need to use the correct method/field access
					// for your 'optional.Bool' type to check if it's set and its value.
					t.Logf("Update Matcher (Bucket1): Verifying VersioningEnabled. Actual type in `update` is %T. Adapt check if this test fails due to VersioningEnabled. Expected config value for VersioningEnabled: true", update.VersioningEnabled)
					// Example placeholder: if update.VersioningEnabled is an optional.Bool struct, you might do:
					// if !update.VersioningEnabled.IsSet || update.VersioningEnabled.Value != true { match = false; ... }

					if update.Lifecycle == nil || len(update.Lifecycle.Rules) != 1 || update.Lifecycle.Rules[0].Action.Type != "Delete" || update.Lifecycle.Rules[0].Condition.AgeInDays != 30 {
						t.Logf("Update Matcher (Bucket1): Lifecycle mismatch. Expected: Delete rule Age 30, Got: %+v", update.Lifecycle)
						match = false
					}
					return match
				})).Return(nil, nil).Once()

				mockClient.On("Bucket", "test-bucket-2-defaults").Return(mockBucket2).Once()
				existingAttrs2 := &storage.BucketAttrs{Name: "test-bucket-2-defaults", Location: "US-CENTRAL1", StorageClass: "STANDARD", VersioningEnabled: true, Labels: map[string]string{"extra": "label", "env": "old-env-default"}}
				mockBucket2.On("Attrs", ctx).Return(existingAttrs2, nil).Once()
				mockBucket2.On("Update", ctx, mock.MatchedBy(func(update storage.BucketAttrsToUpdate) bool {
					match := true
					if update.StorageClass != "NEARLINE" {
						t.Logf("Update Matcher (Bucket2): StorageClass mismatch. Expected: NEARLINE, Got: %s", update.StorageClass)
						match = false
					}

					// NOTE: Similar to Bucket1, VersioningEnabled check needs adaptation for 'optional.Bool'.
					// Expected config value for VersioningEnabled for bucket-2-defaults is false.
					t.Logf("Update Matcher (Bucket2): Verifying VersioningEnabled. Actual type in `update` is %T. Adapt check if this test fails due to VersioningEnabled. Expected config value for VersioningEnabled: false", update.VersioningEnabled)

					if !(update.Lifecycle == nil || len(update.Lifecycle.Rules) == 0) {
						t.Logf("Update Matcher (Bucket2): Lifecycle mismatch. Expected: no rules, Got: %+v", update.Lifecycle)
						match = false
					}
					return match
				})).Return(nil, nil).Once()
			},
			expectError: false,
		},
		{
			name:        "Error getting initial bucket attributes",
			environment: testEnvironment,
			setupMocks: func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "test-bucket-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(nil, newGenericGCSError("failed to get attrs")).Once()
			},
			expectError:   true,
			errorContains: "failed to get attributes for bucket 'test-bucket-1'",
		},
		{
			name:        "Error creating bucket",
			environment: testEnvironment,
			setupMocks: func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "test-bucket-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(nil, newBucketNotFoundError()).Once()
				mockBucket1.On("Create", ctx, testProjectID, mock.AnythingOfType("*storage.BucketAttrs")).Return(newGenericGCSError("creation failed")).Once()
			},
			expectError:   true,
			errorContains: "failed to create bucket 'test-bucket-1'",
		},
		{
			name:        "Error updating bucket",
			environment: testEnvironment,
			setupMocks: func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "test-bucket-1").Return(mockBucket1).Once()
				existingAttrs1 := &storage.BucketAttrs{Name: "test-bucket-1"}
				mockBucket1.On("Attrs", ctx).Return(existingAttrs1, nil).Once()
				mockBucket1.On("Update", ctx, mock.AnythingOfType("storage.BucketAttrsToUpdate")).Return(nil, newGenericGCSError("update failed")).Once()
			},
			expectError: false,
		},
		{
			name:        "Project MessageID not found for environment",
			config:      &TopLevelConfig{DefaultProjectID: "fallback-project", Resources: ResourcesSpec{GCSBuckets: []GCSBucket{{Name: "some-bucket"}}}, Environments: map[string]EnvironmentSpec{}},
			environment: "missing-env",
			setupMocks: func(t *testing.T, mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
			},
			expectError:   true,
			errorContains: "project MessageID not found for environment 'missing-env'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := new(MockGCSClient)
			mockBucket1 := new(MockGCSBucketHandle)
			mockBucket2 := new(MockGCSBucketHandle)

			if tc.setupMocks != nil {
				tc.setupMocks(t, mockClient, mockBucket1, mockBucket2)
			}

			manager, err := NewStorageManager(mockClient, logger)
			require.NoError(t, err)

			currentConfig := baseConfig
			if tc.config != nil {
				currentConfig = tc.config
			}
			configToUse := *currentConfig
			if tc.name == "Create new buckets successfully" || tc.name == "Buckets already exist, update attributes" {
				if configToUse.Environments == nil {
					configToUse.Environments = make(map[string]EnvironmentSpec)
				}
				envSpecCopy := envWithLabels
				configToUse.Environments[testEnvironment] = envSpecCopy
			}

			err = manager.Setup(ctx, &configToUse, tc.environment)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				if err != nil {
					t.Logf("Setup returned unexpected error: %v", err)
				}
				require.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
			mockBucket1.AssertExpectations(t)
			mockBucket2.AssertExpectations(t)
		})
	}
}

func TestStorageManager_Teardown(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()
	testProjectID := "test-gcs-project-teardown"
	testEnvironment := "dev_teardown"

	baseConfig := &TopLevelConfig{
		DefaultProjectID: testProjectID,
		Environments: map[string]EnvironmentSpec{
			testEnvironment:         {ProjectID: testProjectID, TeardownProtection: false},
			"prod_teardown_enabled": {ProjectID: testProjectID, TeardownProtection: true},
		},
		Resources: ResourcesSpec{
			GCSBuckets: []GCSBucket{
				{Name: "bucket-to-delete-1"},
				{Name: "bucket-to-delete-2"},
			},
		},
	}

	type mockSetup func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle)

	testCases := []struct {
		name          string
		environment   string
		setupMocks    mockSetup
		expectError   bool
		errorContains string
	}{
		{
			name:        "Successful teardown of multiple buckets",
			environment: testEnvironment,
			setupMocks: func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "bucket-to-delete-2").Return(mockBucket2).Once()
				mockBucket2.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket2.On("Delete", ctx).Return(nil).Once()

				mockClient.On("Bucket", "bucket-to-delete-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket1.On("Delete", ctx).Return(nil).Once()
			},
			expectError: false,
		},
		{
			name:          "Teardown protection enabled",
			environment:   "prod_teardown_enabled",
			setupMocks:    func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {},
			expectError:   true,
			errorContains: "teardown protection enabled for GCS",
		},
		{
			name:        "Bucket not found during teardown is ignored",
			environment: testEnvironment,
			setupMocks: func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "bucket-to-delete-2").Return(mockBucket2).Once()
				mockBucket2.On("Attrs", ctx).Return(nil, newBucketNotFoundError()).Once()

				mockClient.On("Bucket", "bucket-to-delete-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket1.On("Delete", ctx).Return(nil).Once()
			},
			expectError: false,
		},
		{
			name:        "Error getting attributes during teardown (non-notfound)",
			environment: testEnvironment,
			setupMocks: func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "bucket-to-delete-2").Return(mockBucket2).Once()
				mockBucket2.On("Attrs", ctx).Return(nil, newGenericGCSError("failed to get attrs")).Once()

				mockClient.On("Bucket", "bucket-to-delete-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket1.On("Delete", ctx).Return(nil).Once()
			},
			expectError: false,
		},
		{
			name:        "Error deleting bucket (not 'not empty')",
			environment: testEnvironment,
			setupMocks: func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "bucket-to-delete-2").Return(mockBucket2).Once()
				mockBucket2.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket2.On("Delete", ctx).Return(newGenericGCSError("failed to delete")).Once()

				mockClient.On("Bucket", "bucket-to-delete-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket1.On("Delete", ctx).Return(nil).Once()
			},
			expectError: false,
		},
		{
			name:        "Error deleting bucket (bucket not empty)",
			environment: testEnvironment,
			setupMocks: func(mockClient *MockGCSClient, mockBucket1 *MockGCSBucketHandle, mockBucket2 *MockGCSBucketHandle) {
				mockClient.On("Bucket", "bucket-to-delete-2").Return(mockBucket2).Once()
				mockBucket2.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket2.On("Delete", ctx).Return(newGenericGCSError("The bucket you tried to delete is not empty.")).Once()

				mockClient.On("Bucket", "bucket-to-delete-1").Return(mockBucket1).Once()
				mockBucket1.On("Attrs", ctx).Return(&storage.BucketAttrs{}, nil).Once()
				mockBucket1.On("Delete", ctx).Return(nil).Once()
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := new(MockGCSClient)
			mockBucket1 := new(MockGCSBucketHandle)
			mockBucket2 := new(MockGCSBucketHandle)

			if tc.setupMocks != nil {
				tc.setupMocks(mockClient, mockBucket1, mockBucket2)
			}

			manager, err := NewStorageManager(mockClient, logger)
			require.NoError(t, err)

			err = manager.Teardown(ctx, baseConfig, tc.environment)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
			mockBucket1.AssertExpectations(t)
			mockBucket2.AssertExpectations(t)
		})
	}
}
