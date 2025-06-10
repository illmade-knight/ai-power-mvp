package mqttconverter

import (
	"errors"
	"os"
	"time"
)

// MQTTClientConfig holds configuration for the Paho MQTT client.
type MQTTClientConfig struct {
	BrokerURL          string        // e.g., "tls://mqtt.example.com:8883"
	Topic              string        // e.g., "devices/+/up"
	ClientIDPrefix     string        // A prefix for the client MessageID, a unique suffix will be added.
	Username           string        // MQTT username (from env)
	Password           string        // MQTT password (from env)
	KeepAlive          time.Duration // MQTT KeepAlive interval
	ConnectTimeout     time.Duration // Timeout for establishing connection
	ReconnectWaitMin   time.Duration // Minimum wait time before attempting reconnect
	ReconnectWaitMax   time.Duration // Maximum wait time before attempting reconnect
	CACertFile         string        // Optional: Path to CA certificate file for custom CA
	ClientCertFile     string        // Optional: Path to client certificate file for mTLS
	ClientKeyFile      string        // Optional: Path to client key file for mTLS
	InsecureSkipVerify bool          // Optional: Skip TLS verification (NOT recommended for production)
}

// LoadMQTTClientConfigFromEnv loads MQTT configuration from environment variables.
func LoadMQTTClientConfigFromEnv() (*MQTTClientConfig, error) {
	cfg := &MQTTClientConfig{
		BrokerURL:        os.Getenv("MQTT_BROKER_URL"),
		Topic:            os.Getenv("MQTT_TOPIC"),
		ClientIDPrefix:   os.Getenv("MQTT_CLIENT_ID_PREFIX"),
		Username:         os.Getenv("MQTT_USERNAME"),
		Password:         os.Getenv("MQTT_PASSWORD"),
		KeepAlive:        60 * time.Second,  // Default
		ConnectTimeout:   10 * time.Second,  // Default
		ReconnectWaitMin: 1 * time.Second,   // Default
		ReconnectWaitMax: 120 * time.Second, // Default
		CACertFile:       os.Getenv("MQTT_CA_CERT_FILE"),
		ClientCertFile:   os.Getenv("MQTT_CLIENT_CERT_FILE"),
		ClientKeyFile:    os.Getenv("MQTT_CLIENT_KEY_FILE"),
	}
	if skipVerify := os.Getenv("MQTT_INSECURE_SKIP_VERIFY"); skipVerify == "true" {
		cfg.InsecureSkipVerify = true
	}

	if cfg.BrokerURL == "" {
		return nil, errors.New("MQTT_BROKER_URL environment variable not set")
	}
	if cfg.Topic == "" {
		return nil, errors.New("MQTT_TOPIC environment variable not set")
	}
	if cfg.ClientIDPrefix == "" {
		cfg.ClientIDPrefix = "ingestion-service-" // Default prefix
	}
	// Username and Password can be empty for brokers that don't require auth.

	// Parse durations if set in env, otherwise use defaults
	if ka := os.Getenv("MQTT_KEEP_ALIVE_SECONDS"); ka != "" {
		if s, err := time.ParseDuration(ka + "s"); err == nil {
			cfg.KeepAlive = s
		}
	}
	if ct := os.Getenv("MQTT_CONNECT_TIMEOUT_SECONDS"); ct != "" {
		if s, err := time.ParseDuration(ct + "s"); err == nil {
			cfg.ConnectTimeout = s
		}
	}

	return cfg, nil
}
