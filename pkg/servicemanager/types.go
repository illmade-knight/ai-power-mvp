package servicemanager

// LifecycleStrategy defines how the ServiceManager should treat a dataflow's resources.
type LifecycleStrategy string

const (
	// LifecycleStrategyPermanent indicates that resources are long-lived and should be protected from accidental deletion.
	LifecycleStrategyPermanent LifecycleStrategy = "permanent"
	// LifecycleStrategyEphemeral indicates that resources are temporary and should be torn down after use (e.g., after a test).
	LifecycleStrategyEphemeral LifecycleStrategy = "ephemeral"
)

// LifecyclePolicy defines the lifecycle management rules for a dataflow.
type LifecyclePolicy struct {
	// Strategy determines whether the dataflow's resources are permanent or temporary.
	Strategy LifecycleStrategy `yaml:"strategy"`

	// KeepDatasetOnTest, if true, prevents the BigQuery dataset associated with an
	// ephemeral dataflow from being deleted during teardown. Useful for debugging test results.
	KeepDatasetOnTest bool `yaml:"keep_dataset_on_test,omitempty"` // NEW

	// AutoTeardownAfter is a string representation of a duration (e.g., "2h", "30m").
	// This could be used by a separate cleanup process to find and remove stale test environments.
	AutoTeardownAfter string `yaml:"auto_teardown_after,omitempty"`
}

// ServiceSpec defines a microservice's identity within the system.
type ServiceSpec struct {
	Name           string `yaml:"name"`
	ServiceAccount string `yaml:"service_account"`
}

// DataflowSpec defines a logical grouping of services that work together.
type DataflowSpec struct {
	Name        string           `yaml:"name"`
	Description string           `yaml:"description,omitempty"`
	Services    []string         `yaml:"services"`
	Lifecycle   *LifecyclePolicy `yaml:"lifecycle,omitempty"` // Use a pointer to make it optional
}

// TopLevelConfig is the root of the configuration structure.
type TopLevelConfig struct {
	DefaultProjectID string                     `yaml:"default_project_id"`
	DefaultLocation  string                     `yaml:"default_location"`
	Environments     map[string]EnvironmentSpec `yaml:"environments"`
	Services         []ServiceSpec              `yaml:"services"`
	Dataflows        []DataflowSpec             `yaml:"dataflows"`
	Resources        ResourcesSpec              `yaml:"resources"`
}

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

// BigQueryTable defines the configuration for a BigQuery table.
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
