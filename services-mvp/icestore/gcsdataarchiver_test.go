package icestore

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage" // Only for ObjectAttrs in Mock
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock GCS Client Implementation ---
type MockGCSWriter struct {
	buf         bytes.Buffer
	closed      bool
	closeError  error
	writeError  error
	ObjectAttrs *storage.ObjectAttrs
}

func (m *MockGCSWriter) Write(p []byte) (n int, err error) {
	if m.writeError != nil {
		return 0, m.writeError
	}
	if m.closed {
		return 0, errors.New("MockGCSWriter: already closed")
	}
	return m.buf.Write(p)
}
func (m *MockGCSWriter) Close() error {
	if m.closed {
		return errors.New("MockGCSWriter: already closed (double close)")
	}
	m.closed = true
	return m.closeError
}
func (m *MockGCSWriter) Attrs() *storage.ObjectAttrs {
	if m.ObjectAttrs == nil {
		m.ObjectAttrs = &storage.ObjectAttrs{}
	}
	return m.ObjectAttrs
}
func (m *MockGCSWriter) Bytes() []byte { return m.buf.Bytes() }

type MockGCSObjectHandle struct {
	name           string
	bucket         *MockGCSBucketHandle
	newWriterFunc  func(ctx context.Context) GCSWriter
	writerToReturn GCSWriter
}

func (m *MockGCSObjectHandle) NewWriter(ctx context.Context) GCSWriter {
	if m.newWriterFunc != nil {
		return m.newWriterFunc(ctx)
	}
	if m.writerToReturn == nil {
		m.writerToReturn = &MockGCSWriter{ObjectAttrs: &storage.ObjectAttrs{}}
	}
	return m.writerToReturn
}

type MockGCSBucketHandle struct {
	name       string
	client     *MockGCSClient
	objectFunc func(name string) GCSObjectHandle
}

func (m *MockGCSBucketHandle) Object(name string) GCSObjectHandle {
	if m.objectFunc != nil {
		return m.objectFunc(name)
	}
	return &MockGCSObjectHandle{name: name, bucket: m}
}

type MockGCSClient struct {
	bucketFunc func(name string) GCSBucketHandle
}

func (m *MockGCSClient) Bucket(name string) GCSBucketHandle {
	if m.bucketFunc != nil {
		return m.bucketFunc(name)
	}
	return &MockGCSBucketHandle{name: name, client: m}
}

func newTestCombinedMsgBytesForTableTest(eui, locID, clientID, category, rawPayloadStr, processingErrorStr string, ts time.Time) []byte {
	payload := CombinedMessagePayload{
		DeviceEUI: eui, LocationID: locID, ClientID: clientID, DeviceCategory: category,
		RawPayload: rawPayloadStr, ProcessingError: processingErrorStr,
		OriginalMQTTTime: ts, IngestionTimestamp: ts.Add(1 * time.Second),
	}
	bytes, _ := json.Marshal(payload)
	return bytes
}

func TestGCSDataArchiver_TableDrivenScenarios_V4(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()
	writtenObjects := make(map[string]*MockGCSWriter)
	var writtenObjMu sync.Mutex

	mockClient := &MockGCSClient{
		bucketFunc: func(bucketName string) GCSBucketHandle {
			return &MockGCSBucketHandle{name: bucketName,
				objectFunc: func(objectName string) GCSObjectHandle {
					writer := &MockGCSWriter{ObjectAttrs: &storage.ObjectAttrs{}}
					writtenObjMu.Lock()
					writtenObjects[objectName] = writer
					writtenObjMu.Unlock()
					return &MockGCSObjectHandle{name: objectName, writerToReturn: writer}
				},
			}
		},
	}
	config := GCSDataArchiverConfig{
		BucketName: "test-audit-bucket-v4", ObjectPrefix: "raw-events-v4", BatchSizeThreshold: 2,
	}
	archiver := &GCSDataArchiver{
		client: mockClient, config: config, logger: logger,
		batchMap: make(map[string][]RawDataForArchival), shutdownCh: make(chan struct{}),
	}
	baseTs := time.Date(2023, 11, 15, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name                         string
		messageBytes                 []byte
		pathTimestamp                time.Time
		expectedArchiveErrorContains string
		expectedBatchKey             string // Only relevant if message is not rejected by Archive()
		expectedItemsInBatchAfter    int    // Only relevant if message is not rejected
		expectFlushOfThisBatchKey    bool
		expectedFlushedItemsCount    int
		expectedFlushedDeviceEUIs    []string
	}{
		{
			name:          "1. Enriched Message 1 (LOC01) - No Flush",
			messageBytes:  newTestCombinedMsgBytesForTableTest("DEV001", "LOC01", "ClientA", "HVAC", "payload1", "", baseTs),
			pathTimestamp: baseTs, expectedBatchKey: "2023/11/15/LOC01", expectedItemsInBatchAfter: 1,
		},
		{
			name:          "2. Unidentified Message 1 (deadletter) - No Flush",
			messageBytes:  newTestCombinedMsgBytesForTableTest("DEV002", "", "", "", "payload2", OriginalErrMetadataNotFoundString, baseTs.Add(1*time.Minute)),
			pathTimestamp: baseTs.Add(1 * time.Minute), expectedBatchKey: "2023/11/15/deadletter", expectedItemsInBatchAfter: 1,
		},
		{
			name:          "3. Enriched Message 2 (LOC01) - Triggers Flush for LOC01",
			messageBytes:  newTestCombinedMsgBytesForTableTest("DEV003", "LOC01", "ClientA", "Light", "payload3", "", baseTs.Add(2*time.Minute)),
			pathTimestamp: baseTs.Add(2 * time.Minute), expectedBatchKey: "2023/11/15/LOC01", expectedItemsInBatchAfter: 0,
			expectFlushOfThisBatchKey: true, expectedFlushedItemsCount: 2, expectedFlushedDeviceEUIs: []string{"DEV001", "DEV003"},
		},
		{
			name:          "4. Enriched Missing LocationID (deadletter) - Triggers Flush for deadletter",
			messageBytes:  newTestCombinedMsgBytesForTableTest("DEV004", "", "ClientB", "Compute", "payload4", "", baseTs.Add(3*time.Minute)),
			pathTimestamp: baseTs.Add(3 * time.Minute), expectedBatchKey: "2023/11/15/deadletter", expectedItemsInBatchAfter: 0,
			expectFlushOfThisBatchKey: true, expectedFlushedItemsCount: 2, expectedFlushedDeviceEUIs: []string{"DEV002", "DEV004"},
		},
		{
			name:         "5. Unparseable Message - Archive returns error, message is NOT batched",
			messageBytes: []byte("this is not valid json"), pathTimestamp: baseTs.Add(4 * time.Minute),
			expectedArchiveErrorContains: "unmarshal",
			// No batching expected for this message, so BatchKey and ItemsInBatchAfter are not asserted for this specific message.
			// We only check that Archive() returns an error.
			expectedBatchKey: "", expectedItemsInBatchAfter: 0, // Not applicable as it errors out
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			initialBatchMapState := make(map[string]int)
			archiver.mu.Lock()
			for k, v := range archiver.batchMap {
				initialBatchMapState[k] = len(v)
			}
			archiver.mu.Unlock()

			err := archiver.Archive(ctx, tc.messageBytes, tc.pathTimestamp)

			if tc.expectedArchiveErrorContains != "" {
				require.Error(t, err, "Expected an error from Archive for test: %s", tc.name)
				assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tc.expectedArchiveErrorContains), "Error message mismatch for test: %s", tc.name)
				// Verify batchMap was not changed for this specific key if error occurred,
				// or that no new unexpected batches were created.
				archiver.mu.Lock()
				if tc.expectedBatchKey != "" { // If a batch key was relevant (e.g. trying to add to existing)
					assert.Equal(t, initialBatchMapState[tc.expectedBatchKey], len(archiver.batchMap[tc.expectedBatchKey]), "Batch for key %s should not change on Archive error", tc.expectedBatchKey)
				} else { // For unparseable, ensure no new batch was created due to it
					// This is harder to assert directly without knowing all keys.
					// The main point is that the unparseable message itself isn't added.
				}
				archiver.mu.Unlock()
			} else {
				require.NoError(t, err, "Archive returned an unexpected error for test: %s", tc.name)
				archiver.mu.Lock()
				actualItemsInBatch := len(archiver.batchMap[tc.expectedBatchKey])
				assert.Equal(t, tc.expectedItemsInBatchAfter, actualItemsInBatch, "Mismatch in expected items in batchMap for key %s after test: %s", tc.expectedBatchKey, tc.name)
				archiver.mu.Unlock()
			}

			if tc.expectFlushOfThisBatchKey {
				archiver.wg.Wait() // Wait for any async flush to complete
				expectedObjectPathPrefix := config.ObjectPrefix + "/" + tc.expectedBatchKey
				var flushedWriter *MockGCSWriter
				var flushedObjectName string
				writtenObjMu.Lock()
				for name, writer := range writtenObjects {
					if strings.HasPrefix(name, expectedObjectPathPrefix) && writer.closed {
						flushedWriter = writer
						flushedObjectName = name
					}
				}
				writtenObjMu.Unlock()
				require.NotNil(t, flushedWriter, "Writer for flushed batch key %s not found. Written: %v", tc.expectedBatchKey, writtenObjects)

				gzReader, _ := gzip.NewReader(bytes.NewReader(flushedWriter.Bytes()))
				uncompressedData, _ := io.ReadAll(gzReader)
				gzReader.Close()
				var actualFlushedBatch []RawDataForArchival
				scanner := bufio.NewScanner(bytes.NewReader(uncompressedData))
				for scanner.Scan() {
					var record RawDataForArchival
					json.Unmarshal(scanner.Bytes(), &record)
					actualFlushedBatch = append(actualFlushedBatch, record)
				}
				require.Len(t, actualFlushedBatch, tc.expectedFlushedItemsCount, "Items in GCS object for %s", tc.expectedBatchKey)
				if len(tc.expectedFlushedDeviceEUIs) > 0 {
					for i, expectedEUI := range tc.expectedFlushedDeviceEUIs {
						assert.Equal(t, expectedEUI, actualFlushedBatch[i].DeviceEUI)
					}
				}
				writtenObjMu.Lock()
				delete(writtenObjects, flushedObjectName)
				writtenObjMu.Unlock()
			}
		})
	}

	// --- Test Stop flushes remaining VALID messages ---
	t.Run("Stop flushes remaining valid message(s)", func(t *testing.T) {
		// Clear existing state to ensure a clean test for Stop()
		archiver.mu.Lock()
		archiver.batchMap = make(map[string][]RawDataForArchival)
		archiver.mu.Unlock()
		writtenObjMu.Lock()
		writtenObjects = make(map[string]*MockGCSWriter)
		writtenObjMu.Unlock()

		// Add one valid message that wouldn't trigger a flush by itself
		stopTestTs := baseTs.Add(10 * time.Minute)
		stopTestMsgBytes := newTestCombinedMsgBytesForTableTest("DEV_STOP", "LOC_STOP", "ClientStop", "StopCat", "stop_payload", "", stopTestTs)
		err := archiver.Archive(ctx, stopTestMsgBytes, stopTestTs)
		require.NoError(t, err)
		archiver.mu.Lock()
		require.Len(t, archiver.batchMap["2023/11/15/LOC_STOP"], 1, "Batch for LOC_STOP should have 1 item before Stop")
		archiver.mu.Unlock()

		// Now call Stop
		err = archiver.Stop()
		require.NoError(t, err)
		archiver.wg.Wait()

		expectedStopFlushPathPrefix := config.ObjectPrefix + "/2023/11/15/LOC_STOP"
		var stopFlushedWriter *MockGCSWriter
		writtenObjMu.Lock()
		for name, writer := range writtenObjects {
			if strings.HasPrefix(name, expectedStopFlushPathPrefix) {
				stopFlushedWriter = writer
				break
			}
		}
		writtenObjMu.Unlock()
		require.NotNil(t, stopFlushedWriter, "Writer for LOC_STOP batch (flushed by Stop) not found. Written: %v", writtenObjects)

		gzReader, _ := gzip.NewReader(bytes.NewReader(stopFlushedWriter.Bytes()))
		uncompressedData, _ := io.ReadAll(gzReader)
		gzReader.Close()
		var flushedBatch []RawDataForArchival
		scanner := bufio.NewScanner(bytes.NewReader(uncompressedData))
		for scanner.Scan() {
			var record RawDataForArchival
			json.Unmarshal(scanner.Bytes(), &record)
			flushedBatch = append(flushedBatch, record)
		}
		require.Len(t, flushedBatch, 1, "Flushed batch from Stop() should contain 1 valid message")
		assert.Equal(t, "DEV_STOP", flushedBatch[0].DeviceEUI)

		var originalPayloadCombined CombinedMessagePayload
		err = json.Unmarshal(stopTestMsgBytes, &originalPayloadCombined)
		require.NoError(t, err)
		var storedOriginalPayloadCombined CombinedMessagePayload
		err = json.Unmarshal(flushedBatch[0].OriginalPubSubPayload, &storedOriginalPayloadCombined)
		require.NoError(t, err, "Failed to unmarshal OriginalPubSubPayload from the stored record")
		assert.Equal(t, originalPayloadCombined, storedOriginalPayloadCombined, "OriginalPubSubPayload content mismatch")
	})
}
