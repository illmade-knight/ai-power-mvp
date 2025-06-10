package mqttconverter

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadMQTTClientConfigFromEnv tests the LoadMQTTClientConfigFromEnv function.
func TestLoadMQTTClientConfigFromEnv(t *testing.T) {
	// Helper to clear environment variables for testing
	resetEnv := func(vars ...string) {
		for _, v := range vars {
			os.Unsetenv(v)
		}
	}

	// Store original values to restore them later
	originalBrokerURL := os.Getenv("MQTT_BROKER_URL")
	originalTopic := os.Getenv("MQTT_TOPIC")
	originalClientIDPrefix := os.Getenv("MQTT_CLIENT_ID_PREFIX")
	originalUsername := os.Getenv("MQTT_USERNAME")
	originalPassword := os.Getenv("MQTT_PASSWORD")
	originalKeepAlive := os.Getenv("MQTT_KEEP_ALIVE_SECONDS")
	originalConnectTimeout := os.Getenv("MQTT_CONNECT_TIMEOUT_SECONDS")
	originalCACert := os.Getenv("MQTT_CA_CERT_FILE")
	originalClientCert := os.Getenv("MQTT_CLIENT_CERT_FILE")
	originalClientKey := os.Getenv("MQTT_CLIENT_KEY_FILE")
	originalSkipVerify := os.Getenv("MQTT_INSECURE_SKIP_VERIFY")

	// Defer restoration of original environment
	defer func() {
		os.Setenv("MQTT_BROKER_URL", originalBrokerURL)
		os.Setenv("MQTT_TOPIC", originalTopic)
		os.Setenv("MQTT_CLIENT_ID_PREFIX", originalClientIDPrefix)
		os.Setenv("MQTT_USERNAME", originalUsername)
		os.Setenv("MQTT_PASSWORD", originalPassword)
		os.Setenv("MQTT_KEEP_ALIVE_SECONDS", originalKeepAlive)
		os.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", originalConnectTimeout)
		os.Setenv("MQTT_CA_CERT_FILE", originalCACert)
		os.Setenv("MQTT_CLIENT_CERT_FILE", originalClientCert)
		os.Setenv("MQTT_CLIENT_KEY_FILE", originalClientKey)
		os.Setenv("MQTT_INSECURE_SKIP_VERIFY", originalSkipVerify)
	}()

	t.Run("SuccessfulLoad", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")
		t.Setenv("MQTT_CLIENT_ID_PREFIX", "test-client")
		t.Setenv("MQTT_USERNAME", "user")
		t.Setenv("MQTT_PASSWORD", "pass")
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "30")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "5")
		t.Setenv("MQTT_CA_CERT_FILE", "/path/to/ca.pem")
		t.Setenv("MQTT_CLIENT_CERT_FILE", "/path/to/client.crt")
		t.Setenv("MQTT_CLIENT_KEY_FILE", "/path/to/client.key")
		t.Setenv("MQTT_INSECURE_SKIP_VERIFY", "true")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err, "LoadMQTTClientConfigFromEnv should not return an error with all env vars set")
		require.NotNil(t, cfg, "Config should not be nil")

		assert.Equal(t, "tcp://localhost:1883", cfg.BrokerURL)
		assert.Equal(t, "test/topic", cfg.Topic)
		assert.Equal(t, "test-client", cfg.ClientIDPrefix)
		assert.Equal(t, "user", cfg.Username)
		assert.Equal(t, "pass", cfg.Password)
		assert.Equal(t, 30*time.Second, cfg.KeepAlive)
		assert.Equal(t, 5*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, 1*time.Second, cfg.ReconnectWaitMin)   // Default
		assert.Equal(t, 120*time.Second, cfg.ReconnectWaitMax) // Default
		assert.Equal(t, "/path/to/ca.pem", cfg.CACertFile)
		assert.Equal(t, "/path/to/client.crt", cfg.ClientCertFile)
		assert.Equal(t, "/path/to/client.key", cfg.ClientKeyFile)
		assert.True(t, cfg.InsecureSkipVerify)

		// Clean up env vars for next test if any run in parallel or if t.Setenv doesn't isolate perfectly
		// Though t.Setenv should handle this.
		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC", "MQTT_CLIENT_ID_PREFIX", "MQTT_USERNAME", "MQTT_PASSWORD",
			"MQTT_KEEP_ALIVE_SECONDS", "MQTT_CONNECT_TIMEOUT_SECONDS", "MQTT_CA_CERT_FILE",
			"MQTT_CLIENT_CERT_FILE", "MQTT_CLIENT_KEY_FILE", "MQTT_INSECURE_SKIP_VERIFY")
	})

	t.Run("MissingBrokerURL", func(t *testing.T) {
		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC") // Clear required vars
		t.Setenv("MQTT_TOPIC", "test/topic")      // Set one required var

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.Error(t, err, "LoadMQTTClientConfigFromEnv should return an error if MQTT_BROKER_URL is not set")
		assert.Nil(t, cfg, "Config should be nil on error")
		assert.Contains(t, err.Error(), "MQTT_BROKER_URL environment variable not set")

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC")
	})

	t.Run("MissingTopic", func(t *testing.T) {
		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC") // Clear required vars
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.Error(t, err, "LoadMQTTClientConfigFromEnv should return an error if MQTT_TOPIC is not set")
		assert.Nil(t, cfg, "Config should be nil on error")
		assert.Contains(t, err.Error(), "MQTT_TOPIC environment variable not set")

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC")
	})

	t.Run("DefaultClientIDPrefix", func(t *testing.T) {
		resetEnv("MQTT_CLIENT_ID_PREFIX")
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "ingestion-service-", cfg.ClientIDPrefix)

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC", "MQTT_CLIENT_ID_PREFIX")
	})

	t.Run("DefaultDurations", func(t *testing.T) {
		resetEnv("MQTT_KEEP_ALIVE_SECONDS", "MQTT_CONNECT_TIMEOUT_SECONDS")
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC", "MQTT_KEEP_ALIVE_SECONDS", "MQTT_CONNECT_TIMEOUT_SECONDS")
	})

	t.Run("InvalidKeepAliveDuration", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "not-a-duration")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err) // Should not error, uses default
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive, "KeepAlive should default if env var is invalid")

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC", "MQTT_KEEP_ALIVE_SECONDS")
	})

	t.Run("InvalidConnectTimeoutDuration", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "not-a-duration")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err) // Should not error, uses default
		require.NotNil(t, cfg)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout, "ConnectTimeout should default if env var is invalid")

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC", "MQTT_CONNECT_TIMEOUT_SECONDS")
	})

	t.Run("OptionalFieldsNotSet", func(t *testing.T) {
		resetEnv("MQTT_USERNAME", "MQTT_PASSWORD", "MQTT_CA_CERT_FILE", "MQTT_CLIENT_CERT_FILE", "MQTT_CLIENT_KEY_FILE", "MQTT_INSECURE_SKIP_VERIFY")
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg)

		assert.Empty(t, cfg.Username)
		assert.Empty(t, cfg.Password)
		assert.Empty(t, cfg.CACertFile)
		assert.Empty(t, cfg.ClientCertFile)
		assert.Empty(t, cfg.ClientKeyFile)
		assert.False(t, cfg.InsecureSkipVerify)

		resetEnv("MQTT_BROKER_URL", "MQTT_TOPIC", "MQTT_USERNAME", "MQTT_PASSWORD", "MQTT_CA_CERT_FILE", "MQTT_CLIENT_CERT_FILE", "MQTT_CLIENT_KEY_FILE", "MQTT_INSECURE_SKIP_VERIFY")
	})
}
