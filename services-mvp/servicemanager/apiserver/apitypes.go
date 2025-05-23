package apiserver

// This file defines the data structures used for the API responses
// of the configuration access service.

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

// ServiceBigQueryTableInfo holds information about a BigQuery table a service accesses.
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

type T struct {
	ServiceName       string `json:"serviceName"`
	Environment       string `json:"environment"`
	GcpProjectId      string `json:"gcpProjectId"`
	PublishesToTopics []struct {
		Name   string `json:"name"`
		Labels struct {
			ServiceGroup string `json:"service-group"`
			DeviceType   string `json:"device-type"`
			DataType     string `json:"data-type"`
		} `json:"labels"`
	} `json:"publishesToTopics"`
	ConsumesFromSubscriptions []struct {
		Name               string `json:"name"`
		Topic              string `json:"topic"`
		AckDeadlineSeconds int    `json:"ackDeadlineSeconds"`
		Labels             struct {
			SubscriberService string `json:"subscriber-service"`
		} `json:"labels"`
	} `json:"consumesFromSubscriptions"`
	AccessesGCSBuckets     []interface{} `json:"accessesGCSBuckets"`
	AccessesBigQueryTables []struct {
		ProjectId      string   `json:"projectId"`
		DatasetId      string   `json:"datasetId"`
		TableId        string   `json:"tableId"`
		DeclaredAccess []string `json:"declaredAccess"`
	} `json:"accessesBigQueryTables"`
}
