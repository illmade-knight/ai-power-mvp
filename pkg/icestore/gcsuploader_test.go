package icestore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- GCSBatchUploader Test Cases ---

func TestGCSBatchUploader_UploadBatch_SingleGroup(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockClient := newMockGCSClient()
	config := GCSBatchUploaderConfig{
		BucketName:   "test-bucket",
		ObjectPrefix: "uploads",
	}
	uploader, err := NewGCSBatchUploader(mockClient, config, logger)
	require.NoError(t, err)

	batch := []*ArchivalData{
		{ID: "msg-1", BatchKey: "2025/06/13/loc-a", OriginalPubSubPayload: []byte(`{"data":"one"}`)},
		{ID: "msg-2", BatchKey: "2025/06/13/loc-a", OriginalPubSubPayload: []byte(`{"data":"two"}`)},
	}

	// Act
	err = uploader.UploadBatch(context.Background(), batch)
	require.NoError(t, err)

	// Assert
	mockClient.bucket.Lock()
	defer mockClient.bucket.Unlock()
	// Should create one object because both items have the same BatchKey.
	assert.Len(t, mockClient.bucket.objects, 1, "Expected one object to be created")

	for objectName, handle := range mockClient.bucket.objects {
		assert.Contains(t, objectName, "uploads/2025/06/13/loc-a/", "Object path is incorrect")

		// Decompress and verify content
		gzReader, err := gzip.NewReader(&handle.writer.buf)
		require.NoError(t, err)
		content, err := io.ReadAll(gzReader)
		require.NoError(t, err)

		// Content should be two lines of JSON (JSONL format)
		lines := bytes.Split(bytes.TrimSpace(content), []byte("\n"))
		require.Len(t, lines, 2, "Expected two JSON records in the file")

		var record1, record2 ArchivalData
		err = json.Unmarshal(lines[0], &record1)
		require.NoError(t, err)
		err = json.Unmarshal(lines[1], &record2)
		require.NoError(t, err)

		assert.Equal(t, "msg-1", record1.ID)
		assert.Equal(t, "msg-2", record2.ID)
	}
}

func TestGCSBatchUploader_UploadBatch_MultipleGroups(t *testing.T) {
	// Arrange
	logger := zerolog.Nop()
	mockClient := newMockGCSClient()
	config := GCSBatchUploaderConfig{
		BucketName:   "test-bucket",
		ObjectPrefix: "uploads",
	}
	uploader, err := NewGCSBatchUploader(mockClient, config, logger)
	require.NoError(t, err)

	batch := []*ArchivalData{
		{ID: "msg-a1", BatchKey: "2025/06/14/loc-a"},
		{ID: "msg-b1", BatchKey: "2025/06/14/loc-b"},
		{ID: "msg-a2", BatchKey: "2025/06/14/loc-a"},
	}

	// Act
	err = uploader.UploadBatch(context.Background(), batch)
	require.NoError(t, err)

	// Assert
	mockClient.bucket.Lock()
	defer mockClient.bucket.Unlock()
	// Should create two objects because there are two unique BatchKeys.
	assert.Len(t, mockClient.bucket.objects, 2, "Expected two objects to be created for two unique batch keys")

	foundA, foundB := false, false
	for objectName := range mockClient.bucket.objects {
		if strings.Contains(objectName, "loc-a") {
			foundA = true
		}
		if strings.Contains(objectName, "loc-b") {
			foundB = true
		}
	}
	assert.True(t, foundA, "Object for loc-a was not created")
	assert.True(t, foundB, "Object for loc-b was not created")
}

func TestGCSBatchUploader_EmptyBatch(t *testing.T) {
	logger := zerolog.Nop()
	mockClient := newMockGCSClient()
	config := GCSBatchUploaderConfig{
		BucketName:   "test-bucket",
		ObjectPrefix: "uploads",
	}
	uploader, err := NewGCSBatchUploader(mockClient, config, logger)
	require.NoError(t, err)

	// Act
	err = uploader.UploadBatch(context.Background(), []*ArchivalData{})
	require.NoError(t, err)

	// Assert
	assert.Len(t, mockClient.bucket.objects, 0, "Should not create any objects for an empty batch")
}
