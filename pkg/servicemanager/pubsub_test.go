package servicemanager

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---
// We use the testify/mock package to create mock implementations of our interfaces.
// This allows us to control the behavior of the dependencies during tests.

// MockPSTopic is a mock implementation of the PSTopic interface.
type MockPSTopic struct {
	mock.Mock
}

func (m *MockPSTopic) ID() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockPSTopic) Exists(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}
func (m *MockPSTopic) Update(ctx context.Context, cfg pubsub.TopicConfigToUpdate) (pubsub.TopicConfig, error) {
	args := m.Called(ctx, cfg)
	return args.Get(0).(pubsub.TopicConfig), args.Error(1)
}
func (m *MockPSTopic) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockPSSubscription is a mock implementation of the PSSubscription interface.
type MockPSSubscription struct {
	mock.Mock
}

func (m *MockPSSubscription) ID() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockPSSubscription) Exists(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}
func (m *MockPSSubscription) Update(ctx context.Context, cfg pubsub.SubscriptionConfigToUpdate) (pubsub.SubscriptionConfig, error) {
	args := m.Called(ctx, cfg)
	return args.Get(0).(pubsub.SubscriptionConfig), args.Error(1)
}
func (m *MockPSSubscription) Delete(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockPSClient is a mock implementation of the PSClient interface.
type MockPSClient struct {
	mock.Mock
}

func (m *MockPSClient) Topic(id string) PSTopic {
	args := m.Called(id)
	return args.Get(0).(PSTopic)
}
func (m *MockPSClient) Subscription(id string) PSSubscription {
	args := m.Called(id)
	return args.Get(0).(PSSubscription)
}
func (m *MockPSClient) CreateTopic(ctx context.Context, topicID string) (PSTopic, error) {
	args := m.Called(ctx, topicID)
	return args.Get(0).(PSTopic), args.Error(1)
}
func (m *MockPSClient) CreateTopicWithConfig(ctx context.Context, topicID string, config *pubsub.TopicConfig) (PSTopic, error) {
	args := m.Called(ctx, topicID, config)
	return args.Get(0).(PSTopic), args.Error(1)
}
func (m *MockPSClient) CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (PSSubscription, error) {
	args := m.Called(ctx, id, cfg)
	return args.Get(0).(PSSubscription), args.Error(1)
}
func (m *MockPSClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- Helper Functions ---
func newTestPubSubManager(client PSClient) *PubSubManager {
	logger := zerolog.New(io.Discard) // Send logs to nowhere during tests
	manager, _ := NewPubSubManager(client, logger)
	return manager
}

func getTestConfig() *TopLevelConfig {
	return &TopLevelConfig{
		DefaultProjectID: "test-project",
		Environments: map[string]EnvironmentSpec{
			"test": {ProjectID: "test-project"},
		},
		Resources: ResourcesSpec{
			PubSubTopics: []PubSubTopic{
				{Name: "test-topic-1"},
				{Name: "test-topic-2", Labels: map[string]string{"env": "test"}},
			},
			PubSubSubscriptions: []PubSubSubscription{
				{Name: "test-sub-1", Topic: "test-topic-1", AckDeadlineSeconds: 30},
			},
		},
	}
}

// --- Test Cases ---

func TestPubSubManager_Setup_CreateNewResources(t *testing.T) {
	// Arrange
	mockClient := new(MockPSClient)
	manager := newTestPubSubManager(mockClient)
	config := getTestConfig()
	ctx := context.Background()

	// Mock Topic "test-topic-1"
	mockTopic1 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Exists", ctx).Return(false, nil) // Doesn't exist
	mockClient.On("CreateTopic", ctx, "test-topic-1").Return(mockTopic1, nil)
	mockTopic1.On("ID").Return("test-topic-1")

	// Mock Topic "test-topic-2" with labels
	mockTopic2 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Exists", ctx).Return(false, nil)
	mockClient.On("CreateTopicWithConfig", ctx, "test-topic-2", &pubsub.TopicConfig{Labels: map[string]string{"env": "test"}}).Return(mockTopic2, nil)
	mockTopic2.On("ID").Return("test-topic-2")

	// Mock Subscription "test-sub-1"
	mockSub1 := new(MockPSSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Exists", ctx).Return(false, nil) // Doesn't exist
	// We need to use mock.Anything for the config because the topic inside is created by a real client.
	mockClient.On("CreateSubscription", ctx, "test-sub-1", mock.AnythingOfType("pubsub.SubscriptionConfig")).Return(mockSub1, nil)
	mockSub1.On("ID").Return("test-sub-1")

	// Act
	err := manager.Setup(ctx, config, "test")

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockTopic1.AssertExpectations(t)
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
}

func TestPubSubManager_Setup_UpdateExistingResources(t *testing.T) {
	// Arrange
	mockClient := new(MockPSClient)
	manager := newTestPubSubManager(mockClient)
	config := getTestConfig()
	ctx := context.Background()

	// Mock Topic "test-topic-1" (exists, no labels to update)
	mockTopic1 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Exists", ctx).Return(true, nil) // Already exists

	// Mock Topic "test-topic-2" (exists, will be updated with labels)
	mockTopic2 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Exists", ctx).Return(true, nil)
	mockTopic2.On("Update", ctx, pubsub.TopicConfigToUpdate{Labels: map[string]string{"env": "test"}}).Return(pubsub.TopicConfig{}, nil)

	// Mock Subscription "test-sub-1" (exists, will be updated)
	mockSub1 := new(MockPSSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Exists", ctx).Return(true, nil)
	expectedSubUpdate := pubsub.SubscriptionConfigToUpdate{AckDeadline: 30 * time.Second}
	mockSub1.On("Update", ctx, expectedSubUpdate).Return(pubsub.SubscriptionConfig{}, nil)

	// Act
	err := manager.Setup(ctx, config, "test")

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockTopic1.AssertNotCalled(t, "Update") // Ensure update isn't called if no labels
	mockTopic2.AssertExpectations(t)
	mockSub1.AssertExpectations(t)
}

func TestPubSubManager_Setup_TopicCheckFailsForSubscription(t *testing.T) {
	// Arrange
	mockClient := new(MockPSClient)
	manager := newTestPubSubManager(mockClient)
	config := getTestConfig()
	ctx := context.Background()

	// Mock Topic "test-topic-1" (all good)
	mockTopic1 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Exists", ctx).Return(true, nil)

	// Mock Topic "test-topic-2" (all good)
	mockTopic2 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Exists", ctx).Return(true, nil)
	mockTopic2.On("Update", ctx, mock.Anything).Return(pubsub.TopicConfig{}, nil)

	// Make the topic for the subscription not exist
	config.Resources.PubSubSubscriptions[0].Topic = "non-existent-topic"
	mockNonExistentTopic := new(MockPSTopic)
	mockClient.On("Topic", "non-existent-topic").Return(mockNonExistentTopic)
	mockNonExistentTopic.On("Exists", ctx).Return(false, nil) // It does not exist

	// Act
	err := manager.Setup(ctx, config, "test")

	// Assert
	assert.NoError(t, err) // The function should log an error but not fail the whole setup
	mockClient.AssertExpectations(t)
	mockClient.AssertNotCalled(t, "CreateSubscription") // Crucially, we don't try to create the sub
}

func TestPubSubManager_Setup_FailsOnApiError(t *testing.T) {
	// Arrange
	mockClient := new(MockPSClient)
	manager := newTestPubSubManager(mockClient)
	config := getTestConfig()
	ctx := context.Background()

	expectedError := errors.New("GCP API is down")

	// Mock Topic "test-topic-1" to fail on Exists check
	mockTopic1 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Exists", ctx).Return(false, expectedError)

	// Act
	err := manager.Setup(ctx, config, "test")

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockClient.AssertExpectations(t)
}

func TestPubSubManager_Teardown(t *testing.T) {
	// Arrange
	mockClient := new(MockPSClient)
	manager := newTestPubSubManager(mockClient)
	config := getTestConfig()
	ctx := context.Background()

	// Mock Topic "test-topic-1"
	mockTopic1 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Delete", ctx).Return(nil)

	// Mock Topic "test-topic-2"
	mockTopic2 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Delete", ctx).Return(nil)

	// Mock Subscription "test-sub-1"
	mockSub1 := new(MockPSSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Delete", ctx).Return(nil)

	// Act
	err := manager.Teardown(ctx, config, "test")

	// Assert
	assert.NoError(t, err)
	// Since the teardown logic processes subscriptions before topics,
	// we can simply verify that the delete method was called on each mock.
	// This correctly tests the logic without relying on AssertOrder.
	mockSub1.AssertCalled(t, "Delete", ctx)
	mockTopic1.AssertCalled(t, "Delete", ctx)
	mockTopic2.AssertCalled(t, "Delete", ctx)

	// And ensure all other mock expectations are met
	mockClient.AssertExpectations(t)
}

func TestPubSubManager_Teardown_HandlesNotFoundGracefully(t *testing.T) {
	// Arrange
	mockClient := new(MockPSClient)
	manager := newTestPubSubManager(mockClient)
	config := getTestConfig()
	ctx := context.Background()

	// Simulate an error that contains "NotFound"
	notFoundError := errors.New("rpc error: code = NotFound desc = resource not found")

	// Mock all resources to return a "NotFound" error on delete
	mockTopic1 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-1").Return(mockTopic1)
	mockTopic1.On("Delete", ctx).Return(notFoundError)

	mockTopic2 := new(MockPSTopic)
	mockClient.On("Topic", "test-topic-2").Return(mockTopic2)
	mockTopic2.On("Delete", ctx).Return(notFoundError)

	mockSub1 := new(MockPSSubscription)
	mockClient.On("Subscription", "test-sub-1").Return(mockSub1)
	mockSub1.On("Delete", ctx).Return(notFoundError)

	// Act
	err := manager.Teardown(ctx, config, "test")

	// Assert
	assert.NoError(t, err) // Should not fail the whole teardown
	mockClient.AssertExpectations(t)
}
