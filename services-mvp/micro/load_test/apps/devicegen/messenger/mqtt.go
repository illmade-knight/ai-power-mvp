package messenger

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"time"
)

// --- Interfaces for MQTT Client and Token ---

// MQTTClient defines the interface for MQTT clients, matching paho.mqtt.Client methods used.
type MQTTClient interface {
	IsConnected() bool
	Connect() mqtt.Token // Connect now returns our MQTTToken interface
	Disconnect(quiesce uint)
	Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token // Publish returns our MQTTToken
	// Add other methods from mqtt.Client that your publisher uses, if any.
	// For example, if you were using Subscribe:
	// Subscribe(topic string, qos byte, callback mqtt.MessageHandler) MQTTToken
	// OptionsReader() mqtt.ClientOptionsReader // If used directly by publisher logic
}

// --- MQTT Client Factory ---

// MQTTClientFactory defines an interface for creating MQTT clients.
type MQTTClientFactory interface {
	NewClient(opts *mqtt.ClientOptions) MQTTClient
}

// PahoMQTTClientFactory is a concrete implementation of MQTTClientFactory
// using the Paho MQTT library.
type PahoMQTTClientFactory struct{}

// NewClient creates a new Paho MQTT client that conforms to the MQTTClient interface.
// The concrete *mqtt.Client returned by paho.mqtt.NewClient implicitly satisfies
// our MQTTClient interface if MQTTClient is a subset of *mqtt.Client's methods.
// Similarly, *mqtt.Token satisfies MQTTToken.
func (f *PahoMQTTClientFactory) NewClient(opts *mqtt.ClientOptions) MQTTClient {
	return mqtt.NewClient(opts) // paho.mqtt.NewClient returns *mqtt.Client
}

// MQTTMessage structure for publishing.
type MQTTMessage struct {
	DeviceInfo       DeviceInfo  `json:"device_info"`
	LoRaWAN          LoRaWANData `json:"lorawan_data,omitempty"`
	RawPayload       string      `json:"raw_payload"`
	MessageTimestamp time.Time   `json:"message_timestamp"`
	ClientMessageID  string      `json:"client_message_id"`
}

type DeviceInfo struct {
	DeviceEUI string `json:"device_eui"`
}

type LoRaWANData struct {
	ReceivedAt time.Time `json:"received_at,omitempty"`
}
