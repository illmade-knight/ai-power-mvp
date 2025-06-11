package servicemanager

import "fmt"

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

// GetConfigurationForService is a reusable library function that constructs
// the configuration for a specific service. It can be called directly from tests
// or other packages without needing an HTTP server.
func GetConfigurationForService(def ServicesDefinition, serviceName, environment string) (*ServiceConfigurationResponse, error) {
	// Get the full configuration and the target project ID from the definition.
	fullConfig, err := def.GetTopLevelConfig()
	if err != nil {
		return nil, fmt.Errorf("could not retrieve full config: %w", err)
	}

	targetProjectID, err := def.GetProjectID(environment)
	if err != nil {
		return nil, fmt.Errorf("could not determine project ID for environment '%s': %w", environment, err)
	}

	response := &ServiceConfigurationResponse{
		ServiceName:               serviceName,
		Environment:               environment,
		GCPProjectID:              targetProjectID,
		PublishesToTopics:         make([]ServiceResourceInfo, 0),
		ConsumesFromSubscriptions: make([]ServiceSubscriptionInfo, 0),
		AccessesGCSBuckets:        make([]ServiceGCSBucketInfo, 0),
		AccessesBigQueryTables:    make([]ServiceBigQueryTableInfo, 0),
	}

	// Populate PublishesToTopics
	for _, topicCfg := range fullConfig.Resources.PubSubTopics {
		if topicCfg.ProducerService == serviceName {
			response.PublishesToTopics = append(response.PublishesToTopics, ServiceResourceInfo{
				Name:   fmt.Sprintf("projects/%s/topics/%s", targetProjectID, topicCfg.Name),
				Labels: topicCfg.Labels,
			})
		}
	}

	// Populate ConsumesFromSubscriptions
	for _, subCfg := range fullConfig.Resources.PubSubSubscriptions {
		if subCfg.ConsumerService == serviceName {
			fullTopicName := fmt.Sprintf("projects/%s/topics/%s", targetProjectID, subCfg.Topic)
			response.ConsumesFromSubscriptions = append(response.ConsumesFromSubscriptions, ServiceSubscriptionInfo{
				Name:               fmt.Sprintf("projects/%s/subscriptions/%s", targetProjectID, subCfg.Name),
				Topic:              fullTopicName,
				AckDeadlineSeconds: subCfg.AckDeadlineSeconds,
				Labels:             subCfg.Labels,
			})
		}
	}

	// Populate AccessesGCSBuckets
	for _, bucketCfg := range fullConfig.Resources.GCSBuckets {
		for _, accessor := range bucketCfg.AccessingServices {
			if accessor == serviceName {
				response.AccessesGCSBuckets = append(response.AccessesGCSBuckets, ServiceGCSBucketInfo{
					Name:           bucketCfg.Name,
					Location:       bucketCfg.Location,
					DeclaredAccess: bucketCfg.AccessingServices,
				})
				break
			}
		}
	}

	// Populate AccessesBigQueryTables
	for _, tableCfg := range fullConfig.Resources.BigQueryTables {
		for _, accessor := range tableCfg.AccessingServices {
			if accessor == serviceName {
				response.AccessesBigQueryTables = append(response.AccessesBigQueryTables, ServiceBigQueryTableInfo{
					ProjectID:      targetProjectID,
					DatasetID:      tableCfg.Dataset,
					TableID:        tableCfg.Name,
					DeclaredAccess: tableCfg.AccessingServices,
				})
				break
			}
		}
	}

	return response, nil
}
