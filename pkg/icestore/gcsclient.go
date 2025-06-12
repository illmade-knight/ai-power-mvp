package icestore

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

// ====================================================================================
// This file defines a set of interfaces to abstract the Google Cloud Storage client.
// This abstraction allows the GCSBatchUploader to be tested without needing a real
// GCS client, improving unit test quality and speed. The pattern is adapted from
// the original gcsdataarchiver.go.
// ====================================================================================

// --- GCS Client Abstraction Interfaces ---

// GCSClient abstracts the top-level GCS client.
type GCSClient interface {
	Bucket(name string) GCSBucketHandle
}

// GCSBucketHandle abstracts a GCS bucket.
type GCSBucketHandle interface {
	Object(name string) GCSObjectHandle
}

// GCSObjectHandle abstracts a GCS object.
type GCSObjectHandle interface {
	NewWriter(ctx context.Context) GCSWriter
}

// GCSWriter abstracts a GCS object writer. It must satisfy the io.WriteCloser interface.
type GCSWriter interface {
	io.WriteCloser
}

// --- Adapters to wrap the concrete Google Cloud Storage client ---

// gcsClientAdapter wraps a *storage.Client to satisfy the GCSClient interface.
type gcsClientAdapter struct {
	client *storage.Client
}

// NewGCSClientAdapter creates an adapter that makes the concrete *storage.Client
// conform to the GCSClient interface.
func NewGCSClientAdapter(client *storage.Client) GCSClient {
	if client == nil {
		return nil
	}
	return &gcsClientAdapter{client: client}
}

func (a *gcsClientAdapter) Bucket(name string) GCSBucketHandle {
	return &gcsBucketHandleAdapter{handle: a.client.Bucket(name)}
}

// gcsBucketHandleAdapter wraps a *storage.BucketHandle.
type gcsBucketHandleAdapter struct {
	handle *storage.BucketHandle
}

func (a *gcsBucketHandleAdapter) Object(name string) GCSObjectHandle {
	return &gcsObjectHandleAdapter{handle: a.handle.Object(name)}
}

// gcsObjectHandleAdapter wraps a *storage.ObjectHandle.
type gcsObjectHandleAdapter struct {
	handle *storage.ObjectHandle
}

func (a *gcsObjectHandleAdapter) NewWriter(ctx context.Context) GCSWriter {
	// The concrete *storage.Writer returned by the real client already implements
	// io.WriteCloser, so it automatically satisfies our GCSWriter interface.
	return a.handle.NewWriter(ctx)
}
