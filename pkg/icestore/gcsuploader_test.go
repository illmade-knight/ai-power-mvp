package icestore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Mock Implementations for Testing ---

type mockGCSWriter struct {
	buf      *bytes.Buffer
	closeErr error
}

func (m *mockGCSWriter) Write(p []byte) (n int, err error) { return m.buf.Write(p) }
func (m *mockGCSWriter) Close() error                      { return m.closeErr }

type mockGCSObjectHandle struct{ mock.Mock }

func (m *mockGCSObjectHandle) NewWriter(ctx context.Context) GCSWriter {
	args := m.Called(ctx)
	return args.Get(0).(GCSWriter)
}

type mockGCSBucketHandle struct{ mock.Mock }

func (m *mockGCSBucketHandle) Object(name string) GCSObjectHandle {
	args := m.Called(name)
	return args.Get(0).(GCSObjectHandle)
}

type mockGCSClient struct{ mock.Mock }

func (m *mockGCSClient) Bucket(name string) GCSBucketHandle {
	args := m.Called(name)
	return args.Get(0).(GCSBucketHandle)
}

// --- Test-specific Batchable Type ---

// testBatchableItem is a simple struct that implements the Batchable interface for testing.
type testBatchableItem struct {
	ID       int    `json:"id"`
	Data     string `json:"data"`
	batchKey string
}

func (i testBatchableItem) GetBatchKey() string {
	return i.batchKey
}

// ====================================================================================
// Test Cases for the Generic GCSBatchUploader
// ====================================================================================

func TestGCSBatchUploader_UploadBatch_TableDriven(t *testing.T) {
	testCases := []struct {
		name                   string
		batch                  []*testBatchableItem
		expectedGroup1Contents []string
		expectedGroup2Contents []string
		setupMock              func(t *testing.T, mockBucket *mockGCSBucketHandle, writer1, writer2 *mockGCSWriter)
	}{
		{
			name: "Mixed batch with multiple messages for two groups",
			batch: []*testBatchableItem{
				{batchKey: "group1", ID: 1},
				{batchKey: "group2", ID: 2},
				{batchKey: "group1", ID: 3},
			},
			expectedGroup1Contents: []string{`"id":1`, `"id":3`},
			expectedGroup2Contents: []string{`"id":2`},
			setupMock: func(t *testing.T, mockBucket *mockGCSBucketHandle, writer1, writer2 *mockGCSWriter) {
				obj1 := new(mockGCSObjectHandle)
				obj1.On("NewWriter", mock.Anything).Return(writer1)
				mockBucket.On("Object", mock.MatchedBy(func(s string) bool { return strings.Contains(s, "group1") })).Return(obj1).Once()
				obj2 := new(mockGCSObjectHandle)
				obj2.On("NewWriter", mock.Anything).Return(writer2)
				mockBucket.On("Object", mock.MatchedBy(func(s string) bool { return strings.Contains(s, "group2") })).Return(obj2).Once()
			},
		},
		{
			name:  "Empty batch",
			batch: []*testBatchableItem{},
			setupMock: func(t *testing.T, mockBucket *mockGCSBucketHandle, writer1, writer2 *mockGCSWriter) {
				// No calls expected
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := zerolog.Nop()
			mockWriter1 := &mockGCSWriter{buf: &bytes.Buffer{}}
			mockWriter2 := &mockGCSWriter{buf: &bytes.Buffer{}}
			mockBucket := new(mockGCSBucketHandle)
			mockClient := new(mockGCSClient)
			mockClient.On("Bucket", "test-bucket").Return(mockBucket)
			tc.setupMock(t, mockBucket, mockWriter1, mockWriter2)

			// Instantiate the generic uploader with our test type
			uploader, err := NewGCSBatchUploader[testBatchableItem](mockClient, GCSBatchUploaderConfig{BucketName: "test-bucket"}, logger)
			require.NoError(t, err)

			err = uploader.UploadBatch(ctx, tc.batch)
			require.NoError(t, err)

			mockBucket.AssertExpectations(t)
			if len(tc.expectedGroup1Contents) > 0 {
				for _, content := range tc.expectedGroup1Contents {
					assertCompressedJSONContains(t, mockWriter1.buf, content)
				}
			} else {
				assert.Zero(t, mockWriter1.buf.Len())
			}
			if len(tc.expectedGroup2Contents) > 0 {
				for _, content := range tc.expectedGroup2Contents {
					assertCompressedJSONContains(t, mockWriter2.buf, content)
				}
			} else {
				assert.Zero(t, mockWriter2.buf.Len())
			}
		})
	}
}

func TestGCSBatchUploader_WriterCloseError(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()
	closeErr := errors.New("gcs writer close failure")
	mockWriter := &mockGCSWriter{buf: &bytes.Buffer{}, closeErr: closeErr}
	mockObject := new(mockGCSObjectHandle)
	mockObject.On("NewWriter", ctx).Return(mockWriter)
	mockBucket := new(mockGCSBucketHandle)
	mockBucket.On("Object", mock.Anything).Return(mockObject)
	mockClient := new(mockGCSClient)
	mockClient.On("Bucket", "test-bucket").Return(mockBucket)

	uploader, err := NewGCSBatchUploader[testBatchableItem](mockClient, GCSBatchUploaderConfig{BucketName: "test-bucket"}, logger)
	require.NoError(t, err)

	batch := []*testBatchableItem{{batchKey: "group1", ID: 1}}
	err = uploader.UploadBatch(ctx, batch)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, closeErr))
}

// --- Test Helper Functions ---

func getDecompressedString(t *testing.T, buf *bytes.Buffer) string {
	t.Helper()
	if buf.Len() == 0 {
		return ""
	}
	gzReader, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()
	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}
	return string(decompressed)
}

func assertCompressedJSONContains(t *testing.T, buf *bytes.Buffer, expectedSubstring string) {
	t.Helper()
	decompressedContent := getDecompressedString(t, buf)
	assert.Contains(t, decompressedContent, expectedSubstring)
}
