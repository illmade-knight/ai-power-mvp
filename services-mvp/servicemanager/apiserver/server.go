package apiserver

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rs/zerolog"
	// Replace "your_module_path/servicemanager/initialization" with the actual import path for your initialization package.
	// This package should export TopLevelConfig, GetTargetProjectID, and the resource config structs
	// (PubSubTopic, PubSubSubscription, GCSBucket, BigQueryTable, etc.).
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/initialization"
)

// ConfigServer serves configuration information to microservices.
type ConfigServer struct {
	config *initialization.TopLevelConfig // The fully parsed YAML config from the initialization package
	logger zerolog.Logger
}

// NewConfigServer creates a new ConfigServer.
// It expects the already loaded and validated TopLevelConfig.
func NewConfigServer(cfg *initialization.TopLevelConfig, logger zerolog.Logger) *ConfigServer {
	return &ConfigServer{
		config: cfg,
		logger: logger.With().Str("component", "ConfigServer").Logger(),
	}
}

// GetServiceConfigHandler is an HTTP handler that returns the configuration for a specific service and environment.
func (cs *ConfigServer) GetServiceConfigHandler(w http.ResponseWriter, r *http.Request) {
	serviceName := r.URL.Query().Get("serviceName")
	environment := r.URL.Query().Get("env")

	if serviceName == "" {
		http.Error(w, "query parameter 'serviceName' is required", http.StatusBadRequest)
		return
	}
	if environment == "" {
		http.Error(w, "query parameter 'env' is required", http.StatusBadRequest)
		return
	}

	cs.logger.Info().Str("service", serviceName).Str("environment", environment).Msg("Processing configuration request")

	// GetTargetProjectID is assumed to be an exported function from the initialization package.
	targetProjectID, err := initialization.GetTargetProjectID(cs.config, environment)
	if err != nil {
		cs.logger.Error().Err(err).Str("service", serviceName).Str("environment", environment).Msg("Failed to determine target project ID")
		http.Error(w, fmt.Sprintf("Failed to determine project ID for environment '%s': %v", environment, err), http.StatusInternalServerError)
		return
	}

	response := ServiceConfigurationResponse{ // This type is defined in api_types.go (same apiserver package)
		ServiceName:               serviceName,
		Environment:               environment,
		GCPProjectID:              targetProjectID,
		PublishesToTopics:         make([]ServiceResourceInfo, 0),
		ConsumesFromSubscriptions: make([]ServiceSubscriptionInfo, 0),
		AccessesGCSBuckets:        make([]ServiceGCSBucketInfo, 0),
		AccessesBigQueryTables:    make([]ServiceBigQueryTableInfo, 0),
	}

	// Populate PublishesToTopics
	// Assumes initialization.PubSubTopic has a 'ProducerService string' field.
	for _, topicCfg := range cs.config.Resources.PubSubTopics {
		if topicCfg.ProducerService == serviceName { // Direct string comparison
			response.PublishesToTopics = append(response.PublishesToTopics, ServiceResourceInfo{
				Name:   fmt.Sprintf("projects/%s/topics/%s", targetProjectID, topicCfg.Name),
				Labels: topicCfg.Labels,
			})
		}
	}

	// Populate ConsumesFromSubscriptions
	// Assumes initialization.PubSubSubscription has a 'ConsumerService string' field.
	for _, subCfg := range cs.config.Resources.PubSubSubscriptions {
		if subCfg.ConsumerService == serviceName { // Direct string comparison
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
	// Assumes initialization.GCSBucket has an 'AccessingServices []string' field.
	for _, bucketCfg := range cs.config.Resources.GCSBuckets {
		isAccessor := false
		for _, accessor := range bucketCfg.AccessingServices {
			if accessor == serviceName {
				isAccessor = true
				break
			}
		}
		if isAccessor {
			bucketLocation := bucketCfg.Location
			if bucketLocation == "" { // If not specified on bucket, check environment default
				if envSpec, ok := cs.config.Environments[environment]; ok && envSpec.DefaultLocation != "" {
					bucketLocation = envSpec.DefaultLocation
				} else if cs.config.DefaultLocation != "" { // Fallback to global default
					bucketLocation = cs.config.DefaultLocation
				}
			}

			response.AccessesGCSBuckets = append(response.AccessesGCSBuckets, ServiceGCSBucketInfo{
				Name:           bucketCfg.Name,
				Location:       bucketLocation,
				DeclaredAccess: bucketCfg.AccessingServices,
			})
		}
	}

	// Populate AccessesBigQueryTables
	// Assumes initialization.BigQueryTable has an 'AccessingServices []string' field.
	for _, tableCfg := range cs.config.Resources.BigQueryTables {
		isAccessor := false
		for _, accessor := range tableCfg.AccessingServices {
			if accessor == serviceName {
				isAccessor = true
				break
			}
		}
		if isAccessor {
			response.AccessesBigQueryTables = append(response.AccessesBigQueryTables, ServiceBigQueryTableInfo{
				ProjectID:      targetProjectID,            // Table is assumed to be in the service's target project
				DatasetID:      tableCfg.Dataset,           // Uses 'Dataset' field from initialization.BigQueryTable
				TableID:        tableCfg.Name,              // Uses 'Name' field from initialization.BigQueryTable
				DeclaredAccess: tableCfg.AccessingServices, //AccessingServices,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		cs.logger.Error().Err(err).Str("service", serviceName).Str("environment", environment).Msg("Failed to encode and write response")
		// Avoid writing to http.Error if headers have already been partially written by json.NewEncoder
	}
}
