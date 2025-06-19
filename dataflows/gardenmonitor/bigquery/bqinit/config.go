package bqinit

import (
	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for the application.
// It's structured to neatly group settings for different components.
type Config struct {
	// LogLevel for the application-wide logger (e.g., "debug", "info", "warn", "error").
	LogLevel string `mapstructure:"log_level"`

	// HTTPPort is the port for the health check server.
	HTTPPort string `mapstructure:"http_port"`

	// GCP project ID, used by both Pub/Sub and BigQueryConfig clients.
	ProjectID string `mapstructure:"project_id"`

	// Consumer holds settings for the Pub/Sub subscriber.
	Consumer struct {
		SubscriptionID  string `mapstructure:"subscription_id"`
		CredentialsFile string `mapstructure:"credentials_file"`
	} `mapstructure:"consumer"`

	// BigQueryConfig holds settings for the BigQueryConfig inserter.
	BigQueryConfig bqstore.BigQueryDatasetConfig `mapstructure:"bigquery"`

	// BatchProcessing holds settings for the processing and batchProcessing logic.
	BatchProcessing struct {
		bqstore.BatchInserterConfig `mapstructure:"datasetup"`
		NumWorkers                  int `mapstructure:"num_workers"`
	} `mapstructure:"batchProcessing"`
}

// LoadConfig initializes and loads the application configuration.
// It sets defaults, binds command-line flags, and reads from a config file.
func LoadConfig() (*Config, error) {
	v := viper.New()

	// --- 1. Set Defaults ---
	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8080")
	v.SetDefault("project_id", "default-project-id")
	v.SetDefault("batchProcessing.num_workers", 5)
	v.SetDefault("batchProcessing.batch_size", 100)
	v.SetDefault("batchProcessing.flush_timeout", 5*time.Second)
	v.SetDefault("bigQuery.dataset_id", "default-dataset")
	v.SetDefault("bigQuery.dataset_id", "default-bq-dataset")
	v.SetDefault("bigQuery.table_id", "default-bq-table")
	v.SetDefault("bigQuery.credentials_file", "")
	v.SetDefault("consumer.subscription_id", "default-subscription-id")
	v.SetDefault("consumer.credentials_file", "")

	// --- 2. Set up pflag for command-line overrides ---
	pflag.String("config", "", "Path to config file")
	pflag.String("log-level", "", "Log level (debug, info, warn, error)")
	pflag.String("project-id", "", "GCP Project ID")
	pflag.String("subscription-id", "", "Pub/Sub Subscription ID")
	pflag.String("bq-dataset-id", "", "BigQueryConfig Dataset ID")
	pflag.String("bq-table-id", "", "BigQueryConfig Table ID")
	pflag.Parse()
	err := v.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to bind flags")
	}

	// --- 3. Set up Viper to read from file ---
	// Read the config file path from the flag.
	configFile := v.GetString("config")
	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")

	// Attempt to read the config file if it exists.
	if err := v.ReadInConfig(); err != nil {
		// It's okay if the config file doesn't exist, we can rely on flags/env.
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Info().Msg("config file not found using defaults, flags or environment")
		}
	}

	// --- 4. Set up environment variable support ---
	// Allow environment variables to override settings.
	// e.g., APP_LOG_LEVEL will override the log_level config.
	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// --- 5. Unmarshal config into our struct ---
	var cfg Config
	// Manually map flag values to the struct fields where names differ.
	cfg.LogLevel = v.GetString("log-level")
	cfg.ProjectID = v.GetString("project-id")
	cfg.Consumer.SubscriptionID = v.GetString("subscription-id")
	cfg.BigQueryConfig.DatasetID = v.GetString("bq-dataset-id")
	cfg.BigQueryConfig.TableID = v.GetString("bq-table-id")

	// Unmarshal the rest of the config.
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// --- 6. Explicitly check for Cloud Run PORT environment variable ---
	// The PORT env var is set by the Cloud Run environment.
	// It should take precedence over any other configuration.
	if port := os.Getenv("PORT"); port != "" {
		log.Info().Str("old", cfg.HTTPPort).Str("new", port).Msg("Prefer deployment port")
		cfg.HTTPPort = ":" + port
	}

	return &cfg, nil
}
