package initialization

// TopLevelConfig, EnvironmentSpec, ResourcesSpec remain the same

// PubSubTopic defines the configuration for a Pub/Sub topic.
type PubSubTopic struct {
	Name            string            `yaml:"name"`
	Labels          map[string]string `yaml:"labels,omitempty"`
	ProducerService string            `yaml:"producer_service,omitempty"` // NEW
}

// PubSubSubscription defines the configuration for a Pub/Sub subscription.
type PubSubSubscription struct {
	Name               string            `yaml:"name"`
	Topic              string            `yaml:"topic"`
	AckDeadlineSeconds int               `yaml:"ack_deadline_seconds,omitempty"`
	MessageRetention   string            `yaml:"message_retention_duration,omitempty"`
	RetryPolicy        *RetryPolicySpec  `yaml:"retry_policy,omitempty"`
	Labels             map[string]string `yaml:"labels,omitempty"`
	ConsumerService    string            `yaml:"consumer_service,omitempty"` // NEW
}

// RetryPolicySpec remains the same
type RetryPolicySpec struct {
	MinimumBackoff string `yaml:"minimum_backoff"`
	MaximumBackoff string `yaml:"maximum_backoff"`
}

// BigQueryDataset remains the same
type BigQueryDataset struct {
	Name        string            `yaml:"name"`
	Location    string            `yaml:"location,omitempty"`
	Description string            `yaml:"description,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
}

// BigQueryTable defines the configuration for a BigQueryConfig table.
type BigQueryTable struct {
	Name                   string   `yaml:"name"`
	Dataset                string   `yaml:"dataset"`
	Description            string   `yaml:"description,omitempty"`
	SchemaSourceType       string   `yaml:"schema_source_type"`
	SchemaSourceIdentifier string   `yaml:"schema_source_identifier"`
	TimePartitioningField  string   `yaml:"time_partitioning_field,omitempty"`
	TimePartitioningType   string   `yaml:"time_partitioning_type,omitempty"`
	ClusteringFields       []string `yaml:"clustering_fields,omitempty"`
	AccessingServices      []string `yaml:"accessing_services,omitempty"` // NEW
}

// GCSBucket defines the configuration for a GCS bucket.
type GCSBucket struct {
	Name              string              `yaml:"name"`
	Location          string              `yaml:"location,omitempty"`
	StorageClass      string              `yaml:"storage_class,omitempty"`
	VersioningEnabled bool                `yaml:"versioning_enabled,omitempty"`
	LifecycleRules    []LifecycleRuleSpec `yaml:"lifecycle_rules,omitempty"`
	Labels            map[string]string   `yaml:"labels,omitempty"`
	AccessingServices []string            `yaml:"accessing_services,omitempty"` // NEW
}

// LifecycleRuleSpec, LifecycleActionSpec, LifecycleConditionSpec remain the same
type LifecycleRuleSpec struct {
	Action    LifecycleActionSpec    `yaml:"action"`
	Condition LifecycleConditionSpec `yaml:"condition"`
}
type LifecycleActionSpec struct {
	Type string `yaml:"type"`
}
type LifecycleConditionSpec struct {
	AgeDays int `yaml:"age_days,omitempty"`
}

// Ensure TopLevelConfig, EnvironmentSpec, ResourcesSpec are also present
// (assuming they are in the same file or package from previous context)
type TopLevelConfig struct {
	DefaultProjectID string                     `yaml:"default_project_id"`
	DefaultLocation  string                     `yaml:"default_location"`
	Environments     map[string]EnvironmentSpec `yaml:"environments"`
	Resources        ResourcesSpec              `yaml:"resources"`
}

type EnvironmentSpec struct {
	ProjectID          string            `yaml:"project_id"`
	DefaultLocation    string            `yaml:"default_location,omitempty"`
	DefaultLabels      map[string]string `yaml:"default_labels,omitempty"`
	TeardownProtection bool              `yaml:"teardown_protection,omitempty"`
}

type ResourcesSpec struct {
	PubSubTopics        []PubSubTopic        `yaml:"pubsub_topics"`
	PubSubSubscriptions []PubSubSubscription `yaml:"pubsub_subscriptions"`
	BigQueryDatasets    []BigQueryDataset    `yaml:"bigquery_datasets"`
	BigQueryTables      []BigQueryTable      `yaml:"bigquery_tables"`
	GCSBuckets          []GCSBucket          `yaml:"gcs_buckets"`
}
