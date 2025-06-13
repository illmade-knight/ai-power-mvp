package mqinit

import (
	"github.com/rs/zerolog/log"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
)

// Config holds all configuration for the MQTT ingestion service.
type Config struct {
	LogLevel string `mapstructure:"log_level"`
	HTTPPort string `mapstructure:"http_port"`

	// GCP project ID, used by the Pub/Sub publisher.
	ProjectID string `mapstructure:"project_id"`

	// Publisher holds settings for the Pub/Sub topic to publish to.
	Publisher struct {
		TopicID         string `mapstructure:"topic_id"`
		CredentialsFile string `mapstructure:"credentials_file"`
	} `mapstructure:"publisher"`

	// MQTT holds settings for the MQTT client connection.
	MQTT mqttconverter.MQTTClientConfig `mapstructure:"mqtt"`

	// Service holds settings for the ingestion service workers.
	Service mqttconverter.IngestionServiceConfig `mapstructure:"service"`
}

// LoadConfig initializes and loads the application configuration.
func LoadConfig() (*Config, error) {
	v := viper.New()

	// --- 1. Set Defaults ---
	v.SetDefault("project_id", "default-project-id")

	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8081") // Different default port
	v.SetDefault("mqtt.broker_url", "default-broker-url")
	v.SetDefault("mqtt.broker_topic", "default-topic")
	v.SetDefault("mqtt.client_id_prefix", "mqtt-ingestion-service-")
	v.SetDefault("mqtt.keep_alive", 60*time.Second)
	v.SetDefault("mqtt.connect_timeout", 10*time.Second)

	v.SetDefault("service.input_chan_capacity", 5000)
	v.SetDefault("service.num_processing_workers", 20)
	v.SetDefault("publisher.topic_id", "default-subscription-id")
	v.SetDefault("publisher.credentials_file", "")

	// --- 2. Set up pflag for command-line overrides ---
	pflag.String("config", "config-mqtt.yaml", "Path to MQTT config file")
	pflag.String("log-level", v.GetString("log_level"), "Log level (debug, info, warn, error)")
	pflag.String("project-id", "", "GCP Project ID")
	pflag.String("http-port", v.GetString("http_port"), "HTTP health check port")

	// MQTT Flags
	pflag.String("mqtt-broker-url", "", "MQTT Broker URL (e.g., tls://host:port)")
	pflag.String("mqtt-topic", "", "MQTT Topic to subscribe to")
	pflag.String("mqtt-username", "", "MQTT Username")
	pflag.String("mqtt-client-id-prefix", v.GetString("mqtt.client_id_prefix"), "MQTT Client ID Prefix")

	// Publisher Flags
	pflag.String("pubsub-topic-id", "", "Google Pub/Sub Topic ID to publish to")

	pflag.Parse()
	v.BindPFlags(pflag.CommandLine)

	// --- 3. Set up Viper to read from file ---
	configFile := v.GetString("config")
	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Info().Msg("config file not found using defaults, flags or environment")
		}
	}

	// --- 4. Set up environment variable support ---
	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// --- 5. Unmarshal config into our struct ---
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// Manually override with flags after unmarshalling to ensure priority
	// This is useful for Kubernetes/Docker environments where flags are common
	if v.IsSet("log-level") {
		cfg.LogLevel = v.GetString("log-level")
	}
	if v.IsSet("http-port") {
		cfg.HTTPPort = v.GetString("http-port")
	}
	if v.IsSet("project-id") {
		cfg.ProjectID = v.GetString("project-id")
	}
	if v.IsSet("pubsub-topic-id") {
		cfg.Publisher.TopicID = v.GetString("pubsub-topic-id")
	}
	if v.IsSet("mqtt-broker-url") {
		cfg.MQTT.BrokerURL = v.GetString("mqtt-broker-url")
	}
	if v.IsSet("mqtt-topic") {
		cfg.MQTT.Topic = v.GetString("mqtt-topic")
	}
	if v.IsSet("mqtt-username") {
		cfg.MQTT.Username = v.GetString("mqtt-username")
	}
	if v.IsSet("mqtt-client-id-prefix") {
		cfg.MQTT.ClientIDPrefix = v.GetString("mqtt-client-id-prefix")
	}
	// For secret-like values, prefer environment variables
	cfg.MQTT.Password = v.GetString("mqtt_password")

	// --- 6. Explicitly check for Cloud Run PORT environment variable ---
	// The PORT env var is set by the Cloud Run environment.
	// It should take precedence over any other configuration.
	if port := os.Getenv("PORT"); port != "" {
		log.Info().Str("old", cfg.HTTPPort).Str("new", port).Msg("Prefer deployment port")
		cfg.HTTPPort = ":" + port
	}

	return &cfg, nil
}
