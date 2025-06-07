package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// mockMetadataFetcher implements DeviceMetadataFetcher for testing.
type mockMetadataFetcher struct {
	CalledWithEUI string
	TimesCalled   int
	RetClientID   string
	RetLocationID string
	RetCategory   string
	RetError      error
	mutex         sync.Mutex
}

func (m *mockMetadataFetcher) Fetch(deviceEUI string) (clientID, locationID, category string, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.CalledWithEUI = deviceEUI
	m.TimesCalled++
	return m.RetClientID, m.RetLocationID, m.RetCategory, m.RetError
}

func (m *mockMetadataFetcher) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.CalledWithEUI = ""
	m.TimesCalled = 0
	m.RetClientID = ""
	m.RetLocationID = ""
	m.RetCategory = ""
	m.RetError = nil
}

// mockMessagePublisher implements MessagePublisher for testing.
type mockMessagePublisher struct {
	PublishEnrichedCalledWith      *EnrichedMessage
	PublishUnidentifiedCalledWith  *UnidentifiedDeviceMessage
	StopCalled                     bool
	RetPublishEnrichedError        error
	RetPublishUnidentifiedError    error
	PublishEnrichedTimesCalled     int
	PublishUnidentifiedTimesCalled int
	mutex                          sync.Mutex
}

func (m *mockMessagePublisher) PublishEnriched(ctx context.Context, message *EnrichedMessage) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.PublishEnrichedCalledWith = message
	m.PublishEnrichedTimesCalled++
	return m.RetPublishEnrichedError
}

func (m *mockMessagePublisher) PublishUnidentified(ctx context.Context, message *UnidentifiedDeviceMessage) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.PublishUnidentifiedCalledWith = message
	m.PublishUnidentifiedTimesCalled++
	return m.RetPublishUnidentifiedError
}

func (m *mockMessagePublisher) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.StopCalled = true
}

func (m *mockMessagePublisher) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.PublishEnrichedCalledWith = nil
	m.PublishUnidentifiedCalledWith = nil
	m.StopCalled = false
	m.RetPublishEnrichedError = nil
	m.RetPublishUnidentifiedError = nil
	m.PublishEnrichedTimesCalled = 0
	m.PublishUnidentifiedTimesCalled = 0
}

func TestEnrichmentService_ProcessPubSubMessage(t *testing.T) {
	baseTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	const testMessageID = "test-msg-id" // Consistent message ID for tests

	tests := []struct {
		name                          string
		mqttMessage                   MQTTMessage // Used to generate pubsubMsg.Data
		pubsubMsgDataOverride         []byte      // If non-nil, use this directly as pubsubMsg.Data
		setupFetcher                  func(*mockMetadataFetcher)
		setupEnrichedPublisher        func(*mockMessagePublisher)
		setupUnidentifiedPublisher    func(*mockMessagePublisher)
		expectedErrorInServiceChan    bool
		expectedErrorSubstring        string // For service error chan
		expectedEnrichedPublished     bool
		expectedUnidentifiedPublished bool
		expectedEnrichedMessage       *EnrichedMessage
		expectedUnidentifiedMessage   *UnidentifiedDeviceMessage
	}{
		{
			name: "successful enrichment and publish",
			mqttMessage: MQTTMessage{
				DeviceInfo:       DeviceInfo{DeviceEUI: "eui-123"},
				RawPayload:       "payload1",
				MessageTimestamp: baseTime,
				LoRaWAN: LoRaWANData{
					ReceivedAt: baseTime.Add(-1 * time.Hour),
				},
			},
			setupFetcher: func(mf *mockMetadataFetcher) {
				mf.RetClientID = "client-abc"
				mf.RetLocationID = "loc-xyz"
				mf.RetCategory = "cat-001"
			},
			setupEnrichedPublisher:     func(mp *mockMessagePublisher) {},
			setupUnidentifiedPublisher: func(mp *mockMessagePublisher) {},
			expectedEnrichedPublished:  true,
			expectedEnrichedMessage: &EnrichedMessage{
				RawPayload:        "payload1",
				DeviceEUI:         "eui-123",
				OriginalMQTTTime:  baseTime,
				LoRaWANReceivedAt: baseTime.Add(-1 * time.Hour),
				ClientID:          "client-abc",
				LocationID:        "loc-xyz",
				DeviceCategory:    "cat-001",
				// IngestionTimestamp is checked for non-zero
			},
		},
		{
			name: "metadata not found",
			mqttMessage: MQTTMessage{
				DeviceInfo:       DeviceInfo{DeviceEUI: "eui-456"},
				RawPayload:       "payload2",
				MessageTimestamp: baseTime,
			},
			setupFetcher: func(mf *mockMetadataFetcher) {
				mf.RetError = ErrMetadataNotFound
			},
			setupEnrichedPublisher:        func(mp *mockMessagePublisher) {},
			setupUnidentifiedPublisher:    func(mp *mockMessagePublisher) {},
			expectedUnidentifiedPublished: true,
			expectedUnidentifiedMessage: &UnidentifiedDeviceMessage{
				DeviceEUI:        "eui-456",
				RawPayload:       "payload2",
				OriginalMQTTTime: baseTime,
				ProcessingError:  ErrMetadataNotFound.Error(),
				// IngestionTimestamp is checked for non-zero
			},
		},
		{
			name: "metadata fetcher returns other error",
			mqttMessage: MQTTMessage{
				DeviceInfo: DeviceInfo{DeviceEUI: "eui-789"},
				RawPayload: "payload3",
			},
			setupFetcher: func(mf *mockMetadataFetcher) {
				mf.RetError = errors.New("firestore unavailable")
			},
			setupEnrichedPublisher:     func(mp *mockMessagePublisher) {},
			setupUnidentifiedPublisher: func(mp *mockMessagePublisher) {},
			expectedErrorInServiceChan: true,
			expectedErrorSubstring:     "firestore unavailable",
		},
		{
			name: "failed to publish enriched message",
			mqttMessage: MQTTMessage{
				DeviceInfo: DeviceInfo{DeviceEUI: "eui-101"},
				RawPayload: "payload4",
			},
			setupFetcher: func(mf *mockMetadataFetcher) {
				mf.RetClientID = "client-def"
				mf.RetLocationID = "loc-101"
				mf.RetCategory = "cat-101"
			},
			setupEnrichedPublisher: func(mp *mockMessagePublisher) {
				mp.RetPublishEnrichedError = errors.New("pubsub topic error")
			},
			setupUnidentifiedPublisher: func(mp *mockMessagePublisher) {},
			expectedErrorInServiceChan: true,
			expectedErrorSubstring:     "pubsub topic error",
			expectedEnrichedPublished:  true, // Still attempted
		},
		{
			name: "failed to publish unidentified message",
			mqttMessage: MQTTMessage{
				DeviceInfo: DeviceInfo{DeviceEUI: "eui-112"},
				RawPayload: "payload5",
			},
			setupFetcher: func(mf *mockMetadataFetcher) {
				mf.RetError = ErrMetadataNotFound
			},
			setupEnrichedPublisher: func(mp *mockMessagePublisher) {},
			setupUnidentifiedPublisher: func(mp *mockMessagePublisher) {
				mp.RetPublishUnidentifiedError = errors.New("unidentified pubsub error")
			},
			expectedErrorInServiceChan:    true,
			expectedErrorSubstring:        "unidentified pubsub error",
			expectedUnidentifiedPublished: true, // Still attempted
		},
		{
			name:                       "invalid pubsub message data (unmarshal error)",
			pubsubMsgDataOverride:      []byte("this is not valid json {"), // Override to force bad JSON
			setupFetcher:               func(mf *mockMetadataFetcher) {},
			setupEnrichedPublisher:     func(mp *mockMessagePublisher) {},
			setupUnidentifiedPublisher: func(mp *mockMessagePublisher) {},
			expectedErrorInServiceChan: true,
			expectedErrorSubstring:     fmt.Sprintf("unmarshal error for msg %s", testMessageID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetcher := &mockMetadataFetcher{}
			enrichedPub := &mockMessagePublisher{}
			unidentifiedPub := &mockMessagePublisher{}

			fetcher.Reset()
			enrichedPub.Reset()
			unidentifiedPub.Reset()

			tt.setupFetcher(fetcher)
			tt.setupEnrichedPublisher(enrichedPub)
			tt.setupUnidentifiedPublisher(unidentifiedPub)

			svcErrorChan := make(chan error, 10)
			svcCtx, svcCancel := context.WithCancel(context.Background())
			defer svcCancel()

			svc := &EnrichmentService{
				config:                DefaultEnrichmentServiceConfig(),
				logger:                zerolog.Nop(),
				metadataFetcher:       fetcher.Fetch,
				enrichedPublisher:     enrichedPub,
				unidentifiedPublisher: unidentifiedPub,
				errorChan:             svcErrorChan,
				cancelCtx:             svcCtx,
			}

			var pubsubMsgData []byte
			if tt.pubsubMsgDataOverride != nil {
				pubsubMsgData = tt.pubsubMsgDataOverride
			} else {
				var err error
				pubsubMsgData, err = json.Marshal(tt.mqttMessage)
				assert.NoError(t, err, "Failed to marshal mqttMessage for test setup")
			}

			pubsubMsg := &pubsub.Message{
				ID:   testMessageID,
				Data: pubsubMsgData,
			}

			workerID := 0
			svc.processPubSubMessage(context.Background(), pubsubMsg, workerID)

			// Special case for "invalid pubsub message data" - fetcher should not be called
			if tt.name != "invalid pubsub message data (unmarshal error)" {
				if tt.mqttMessage.DeviceInfo.DeviceEUI != "" { // Only expect calls if EUI is present
					assert.Equal(t, 1, fetcher.TimesCalled, "MetadataFetcher call count mismatch")
					assert.Equal(t, tt.mqttMessage.DeviceInfo.DeviceEUI, fetcher.CalledWithEUI, "MetadataFetcher called with wrong EUI")
				} else {
					assert.Equal(t, 0, fetcher.TimesCalled, "MetadataFetcher should not be called for empty EUI")
				}
			} else {
				assert.Equal(t, 0, fetcher.TimesCalled, "MetadataFetcher should not be called for invalid JSON")
			}

			// --- Assertions for publishers ---
			if tt.expectedEnrichedPublished {
				assert.Equal(t, 1, enrichedPub.PublishEnrichedTimesCalled, "PublishEnrichedTimesCalled count mismatch")
				if tt.expectedEnrichedMessage != nil && enrichedPub.PublishEnrichedCalledWith != nil {
					assert.Equal(t, tt.expectedEnrichedMessage.RawPayload, enrichedPub.PublishEnrichedCalledWith.RawPayload)
					assert.Equal(t, tt.expectedEnrichedMessage.DeviceEUI, enrichedPub.PublishEnrichedCalledWith.DeviceEUI)
					assert.Equal(t, tt.expectedEnrichedMessage.OriginalMQTTTime, enrichedPub.PublishEnrichedCalledWith.OriginalMQTTTime)
					assert.Equal(t, tt.expectedEnrichedMessage.LoRaWANReceivedAt, enrichedPub.PublishEnrichedCalledWith.LoRaWANReceivedAt)
					assert.Equal(t, tt.expectedEnrichedMessage.ClientID, enrichedPub.PublishEnrichedCalledWith.ClientID)
					assert.Equal(t, tt.expectedEnrichedMessage.LocationID, enrichedPub.PublishEnrichedCalledWith.LocationID)
					assert.Equal(t, tt.expectedEnrichedMessage.DeviceCategory, enrichedPub.PublishEnrichedCalledWith.DeviceCategory)
					assert.NotZero(t, enrichedPub.PublishEnrichedCalledWith.IngestionTimestamp, "IngestionTimestamp should be set for enriched message")
				}
			} else {
				assert.Equal(t, 0, enrichedPub.PublishEnrichedTimesCalled, "PublishEnrichedTimesCalled should be 0")
			}

			if tt.expectedUnidentifiedPublished {
				assert.Equal(t, 1, unidentifiedPub.PublishUnidentifiedTimesCalled, "PublishUnidentifiedTimesCalled count mismatch")
				if tt.expectedUnidentifiedMessage != nil && unidentifiedPub.PublishUnidentifiedCalledWith != nil {
					assert.Equal(t, tt.expectedUnidentifiedMessage.DeviceEUI, unidentifiedPub.PublishUnidentifiedCalledWith.DeviceEUI)
					assert.Equal(t, tt.expectedUnidentifiedMessage.RawPayload, unidentifiedPub.PublishUnidentifiedCalledWith.RawPayload)
					assert.Equal(t, tt.expectedUnidentifiedMessage.OriginalMQTTTime, unidentifiedPub.PublishUnidentifiedCalledWith.OriginalMQTTTime)
					assert.Equal(t, tt.expectedUnidentifiedMessage.ProcessingError, unidentifiedPub.PublishUnidentifiedCalledWith.ProcessingError)
					assert.NotZero(t, unidentifiedPub.PublishUnidentifiedCalledWith.IngestionTimestamp, "IngestionTimestamp should be set for unidentified message")
				}
			} else {
				assert.Equal(t, 0, unidentifiedPub.PublishUnidentifiedTimesCalled, "PublishUnidentifiedTimesCalled should be 0")
			}

			// --- Assertions for error channel ---
			if tt.expectedErrorInServiceChan {
				select {
				case errReceived := <-svcErrorChan:
					assert.Error(t, errReceived, "Expected an error in service errorChan")
					if tt.expectedErrorSubstring != "" {
						assert.Contains(t, errReceived.Error(), tt.expectedErrorSubstring, "Error message substring mismatch")
					}
				case <-time.After(100 * time.Millisecond): // Timeout
					t.Fatalf("Expected error in service errorChan, but none received for test: %s", tt.name)
				}
			} else {
				select {
				case errReceived := <-svcErrorChan:
					t.Fatalf("Unexpected error in service errorChan: %v for test: %s", errReceived, tt.name)
				default:
					// No error, as expected
				}
			}
		})
	}
}
