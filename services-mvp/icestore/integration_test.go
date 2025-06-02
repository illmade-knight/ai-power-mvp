//go:build integration

package icestore // Corrected package name

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage" // Only for ObjectAttrs in mock
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to find a specific writer from the map, used for verification
// It should find the writer whose name CONTAINS the expected path key, as UUID is appended.
func findWriterForBatchKey(t *testing.T, writtenObjects map[string]*MockGCSWriter, expectedPathKeyPrefix string) (*MockGCSWriter, string) {
	for name, writer := range writtenObjects {
		if strings.Contains(name, expectedPathKeyPrefix) {
			return writer, name // Return the writer and its full object name
		}
	}
	t.Logf("Available object names in writtenObjects for prefix '%s':", expectedPathKeyPrefix)
	for name := range writtenObjects {
		t.Logf("- %s", name)
	}
	return nil, ""
}

func TestGCSDataArchiver_TableDrivenScenarios(t *testing.T) {
	logger := zerolog.Nop()
	ctx := context.Background()

	writtenObjects := make(map[string]*MockGCSWriter)
	var writtenObjMu sync.Mutex

	mockClient := &MockGCSClient{
		bucketFunc: func(bucketName string) GCSBucketHandle {
			return &MockGCSBucketHandle{
				name: bucketName,
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
		BucketName:         "test-audit-bucket-table",
		ObjectPrefix:       "raw-events-table",
		BatchSizeThreshold: 2,
	}

	archiver := &GCSDataArchiver{
		client:     mockClient,
		config:     config,
		logger:     logger,
		batchMap:   make(map[string][]RawDataForArchival),
		shutdownCh: make(chan struct{}),
	}

	baseTs := time.Date(2023, 11, 15, 10, 0, 0, 0, time.UTC)
	// unparseableMessageOriginalPathTs := baseTs.Add(4 * time.Minute) // No longer needed for a dedicated stop test for unparseable

	testCases := []struct {
		name                         string
		messageBytes                 []byte
		pathTimestamp                time.Time
		expectedArchiveErrorContains string
		expectedBatchKey             string
		expectedItemsInBatchAfter    int
		expectFlushOfThisBatchKey    bool
		expectedFlushedItemsCount    int
		expectedFlushedDeviceEUIs    []string
	}{
		{
			name:                      "1. Enriched Message 1 (LOC01) - No Flush",
			messageBytes:              newTestCombinedMsgBytesForTableTest("DEV001", "LOC01", "ClientA", "HVAC", "payload1_content", "", baseTs),
			pathTimestamp:             baseTs,
			expectedBatchKey:          "2023/11/15/LOC01",
			expectedItemsInBatchAfter: 1,
			expectFlushOfThisBatchKey: false,
		},
		{
			name:                      "2. Unidentified Message 1 (deadletter) - No Flush",
			messageBytes:              newTestCombinedMsgBytesForTableTest("DEV002", "", "", "", "payload2_content", OriginalErrMetadataNotFoundString, baseTs.Add(1*time.Minute)),
			pathTimestamp:             baseTs.Add(1 * time.Minute),
			expectedBatchKey:          "2023/11/15/deadletter",
			expectedItemsInBatchAfter: 1,
			expectFlushOfThisBatchKey: false,
		},
		{
			name:                      "3. Enriched Message 2 (LOC01) - Triggers Flush for LOC01",
			messageBytes:              newTestCombinedMsgBytesForTableTest("DEV003", "LOC01", "ClientA", "Lighting", "payload3_content", "", baseTs.Add(2*time.Minute)),
			pathTimestamp:             baseTs.Add(2 * time.Minute),
			expectedBatchKey:          "2023/11/15/LOC01",
			expectedItemsInBatchAfter: 0,
			expectFlushOfThisBatchKey: true,
			expectedFlushedItemsCount: 2,
			expectedFlushedDeviceEUIs: []string{"DEV001", "DEV003"},
		},
		{
			name:                      "4. Enriched Missing LocationID (deadletter) - Triggers Flush for deadletter",
			messageBytes:              newTestCombinedMsgBytesForTableTest("DEV004", "", "ClientB", "Compute", "payload4_content", "", baseTs.Add(3*time.Minute)),
			pathTimestamp:             baseTs.Add(3 * time.Minute),
			expectedBatchKey:          "2023/11/15/deadletter",
			expectedItemsInBatchAfter: 0,
			expectFlushOfThisBatchKey: true,
			expectedFlushedItemsCount: 2,
			expectedFlushedDeviceEUIs: []string{"DEV002", "DEV004"},
		},
		{
			name:                         "5. Unparseable Message - Archive returns error, message is NOT batched",
			messageBytes:                 []byte("this is not valid json"),
			pathTimestamp:                baseTs.Add(4 * time.Minute),
			expectedArchiveErrorContains: "unmarshal",
			expectedBatchKey:             "",
			expectedItemsInBatchAfter:    0,
			expectFlushOfThisBatchKey:    false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := archiver.Archive(ctx, tc.messageBytes, tc.pathTimestamp)

			if tc.expectedArchiveErrorContains != "" {
				require.Error(t, err)
				assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tc.expectedArchiveErrorContains))
			} else {
				require.NoError(t, err)
				archiver.mu.Lock()
				actualItemsInBatch := len(archiver.batchMap[tc.expectedBatchKey])
				assert.Equal(t, tc.expectedItemsInBatchAfter, actualItemsInBatch, "Mismatch in expected items in batchMap for key %s", tc.expectedBatchKey)
				archiver.mu.Unlock()
			}

			if tc.expectFlushOfThisBatchKey {
				archiver.wg.Wait()

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

				require.NotNil(t, flushedWriter, "Writer for flushed batch key %s not found. Written objects: %v", tc.expectedBatchKey, writtenObjects)
				assert.True(t, flushedWriter.closed, "Writer for batch key %s should be closed", tc.expectedBatchKey)

				gzReader, _ := gzip.NewReader(bytes.NewReader(flushedWriter.Bytes()))
				uncompressedData, _ := io.ReadAll(gzReader)
				gzReader.Close()

				var actualFlushedBatch []RawDataForArchival
				scanner := bufio.NewScanner(bytes.NewReader(uncompressedData))
				for scanner.Scan() {
					var record RawDataForArchival
					err := json.Unmarshal(scanner.Bytes(), &record)
					require.NoError(t, err, "Failed to unmarshal record from flushed GCS object")
					actualFlushedBatch = append(actualFlushedBatch, record)
				}

				require.Len(t, actualFlushedBatch, tc.expectedFlushedItemsCount, "Mismatch in number of items in flushed GCS object for batch key %s", tc.expectedBatchKey)
				if len(tc.expectedFlushedDeviceEUIs) > 0 {
					for i, expectedEUI := range tc.expectedFlushedDeviceEUIs {
						assert.Equal(t, expectedEUI, actualFlushedBatch[i].DeviceEUI, "Mismatch in DeviceEUI for flushed item %d", i)
					}
				}
				writtenObjMu.Lock()
				delete(writtenObjects, flushedObjectName)
				writtenObjMu.Unlock()
			}
		})
	}

	// --- Test Stop flushes remaining valid messages ---
	t.Run("Stop flushes remaining valid message", func(t *testing.T) {
		archiver.mu.Lock()
		archiver.batchMap = make(map[string][]RawDataForArchival)
		archiver.mu.Unlock()
		writtenObjMu.Lock()
		writtenObjects = make(map[string]*MockGCSWriter)
		writtenObjMu.Unlock()

		stopTestTs := baseTs.Add(10 * time.Minute)
		stopTestMsgBytes := newTestCombinedMsgBytesForTableTest("DEV_STOP", "LOC_STOP", "ClientStop", "StopCat", "stop_payload", "", stopTestTs)
		err := archiver.Archive(ctx, stopTestMsgBytes, stopTestTs)
		require.NoError(t, err)
		archiver.mu.Lock()
		require.Len(t, archiver.batchMap["2023/11/15/LOC_STOP"], 1, "Batch for LOC_STOP should have 1 item before Stop")
		archiver.mu.Unlock()

		err = archiver.Stop()
		require.NoError(t, err)
		archiver.wg.Wait()

		expectedStopFlushPathPrefix := config.ObjectPrefix + "/" + "2023/11/15/LOC_STOP"

		var stopFlushedWriter *MockGCSWriter
		//var capturedStopObjectName string
		writtenObjMu.Lock()
		for name, writer := range writtenObjects {
			if strings.HasPrefix(name, expectedStopFlushPathPrefix) {
				stopFlushedWriter = writer
				//capturedStopObjectName = name
				break
			}
		}
		writtenObjMu.Unlock()

		require.NotNil(t, stopFlushedWriter, "Writer for LOC_STOP batch (flushed by Stop) not found. Expected prefix: %s. Written objects: %v", expectedStopFlushPathPrefix, writtenObjects)
		assert.True(t, stopFlushedWriter.closed, "Writer for LOC_STOP (flushed by Stop) should be closed")

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
