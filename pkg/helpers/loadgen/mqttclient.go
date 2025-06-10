// mqtt/client.go

package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// MqttClient implements the Client interface for MQTT.
type MqttClient struct {
	client       mqtt.Client
	brokerURL    string
	topicPattern string
	qos          byte
	logger       zerolog.Logger
}

// NewMqttClient creates a new MQTT client.
func NewMqttClient(brokerURL, topicPattern string, qos byte, logger zerolog.Logger) Client {
	return &MqttClient{
		brokerURL:    brokerURL,
		topicPattern: topicPattern,
		qos:          qos,
		logger:       logger,
	}
}

// Connect establishes a connection to the MQTT broker.
func (c *MqttClient) Connect() error {
	opts := mqtt.NewClientOptions().
		AddBroker(c.brokerURL).
		SetClientID(fmt.Sprintf("loadgen-client-%s", uuid.New().String())).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			c.logger.Error().Err(err).Msg("MQTT Connection lost")
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			c.logger.Info().Str("broker", c.brokerURL).Msg("Successfully connected to MQTT broker")
		})

	c.client = mqtt.NewClient(opts)
	if token := c.client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		c.logger.Error().Err(token.Error()).Msg("Failed to connect to MQTT broker")
		return token.Error()
	}

	if !c.client.IsConnected() {
		err := fmt.Errorf("failed to connect to %s", c.brokerURL)
		c.logger.Error().Err(err).Msg("MQTT connection check failed")
		return err
	}

	return nil
}

// Disconnect closes the connection to the MQTT broker.
func (c *MqttClient) Disconnect() {
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(250)
		c.logger.Info().Msg("MQTT client disconnected")
	}
}

// Publish generates a payload and sends a message to the MQTT broker
// in the format expected by the application's backend services.
func (c *MqttClient) Publish(ctx context.Context, device *Device) error {
	// Generate the application-specific payload (e.g., GardenMonitorPayload JSON).
	// This now passes the device pointer, allowing the generator to use the device ID.
	payloadBytes, err := device.PayloadGenerator.GeneratePayload(device)
	if err != nil {
		return fmt.Errorf("failed to generate payload for device %s: %w", device.ID, err)
	}

	// Define the wrapper structure that the backend services expect.
	// This matches the format used in the working e2e test: {"payload":{...}}.
	// We use json.RawMessage to embed the already-marshalled payloadBytes.
	messageToPublish := struct {
		Payload json.RawMessage `json:"payload"`
	}{
		Payload: payloadBytes,
	}

	// Marshal the final, correctly-structured message.
	finalJSON, err := json.Marshal(messageToPublish)
	if err != nil {
		return fmt.Errorf("failed to marshal final message for device %s: %w", device.ID, err)
	}

	// Publish to a specific topic, replacing the wildcard '+' with the device's actual ID.
	topic := strings.Replace(c.topicPattern, "+", device.ID, 1)

	token := c.client.Publish(topic, c.qos, false, finalJSON)

	// Asynchronously wait for the token to complete, or the context to be cancelled.
	select {
	case <-token.Done():
		if token.Error() != nil {
			return fmt.Errorf("mqtt publish error for device %s: %w", device.ID, token.Error())
		}
		c.logger.Debug().Str("device_id", device.ID).Str("topic", topic).Msg("Message published")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context cancelled while publishing for device %s: %w", device.ID, ctx.Err())
	}
}
