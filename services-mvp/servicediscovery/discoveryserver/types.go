package discoveryserver

// This file defines the JSON data structures for the API responses.

// ServiceResourceInfo holds information about a Pub/Sub topic a service interacts with.
type ServiceResourceInfo struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
}

// ServiceSubscriptionInfo holds information about a Pub/Sub subscription a service consumes from.
type ServiceSubscriptionInfo struct {
	Name               string            `json:"name"`
	Topic              string            `json:"topic"`
	AckDeadlineSeconds int               `json:"ackDeadlineSeconds,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"`
}

// ServiceGCSBucketInfo holds information about a GCS bucket a service accesses.
type ServiceGCSBucketInfo struct {
	Name           string   `json:"name"`
	Location       string   `json:"location,omitempty"`
	DeclaredAccess []string `json:"declaredAccess,omitempty"`
}

// ServiceBigQueryTableInfo holds information about a BigQueryConfig table a service accesses.
type ServiceBigQueryTableInfo struct {
	ProjectID      string   `json:"projectId"`
	DatasetID      string   `json:"datasetId"`
	TableID        string   `json:"tableId"`
	DeclaredAccess []string `json:"declaredAccess,omitempty"`
}

// ServiceConfigurationResponse is the structure returned by the config access API.
type ServiceConfigurationResponse struct {
	ServiceName               string                     `json:"serviceName"`
	Environment               string                     `json:"environment"`
	GCPProjectID              string                     `json:"gcpProjectId"`
	PublishesToTopics         []ServiceResourceInfo      `json:"publishesToTopics"`
	ConsumesFromSubscriptions []ServiceSubscriptionInfo  `json:"consumesFromSubscriptions"`
	AccessesGCSBuckets        []ServiceGCSBucketInfo     `json:"accessesGCSBuckets"`
	AccessesBigQueryTables    []ServiceBigQueryTableInfo `json:"accessesBigQueryTables"`
}
