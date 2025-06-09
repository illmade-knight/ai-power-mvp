package messenger

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"strings"
	"sync"
	"time"
)

// --- Publisher Definition ---

// Publisher defines the interface for publishing messages.
type Publisher interface {
	Publish(devices []*Device, duration time.Duration, ctx context.Context) error
}

// mqttPublisher implements the Publisher interface using MQTT.
type mqttPublisher struct {
	topicPattern  string
	brokerURL     string
	clientID      string
	logger        zerolog.Logger
	mqttClient    MQTTClient // Use the MQTTClient interface
	QOS           int
	clientFactory MQTTClientFactory // Holds the factory instance
}

// NewPublisher creates a new MQTT publisher.
// It now requires an MQTTClientFactory to be passed in.
func NewPublisher(
	topicPattern string,
	brokerURL string,
	clientID string,
	qos int,
	logger zerolog.Logger,
	factory MQTTClientFactory, // Factory is now a required parameter
) Publisher {
	if factory == nil {
		// Or handle this as an error: log.Fatal().Msg("MQTTClientFactory cannot be nil")
		// For robustness, let's default if a test or old code accidentally passes nil,
		// though ideally, the caller should always provide it.
		logger.Warn().Msg("MQTTClientFactory is nil, defaulting to PahoMQTTClientFactory. This should be provided by the caller.")
		factory = &PahoMQTTClientFactory{}
	}
	return &mqttPublisher{
		topicPattern:  topicPattern,
		brokerURL:     brokerURL,
		clientID:      clientID,
		logger:        logger,
		QOS:           qos,
		clientFactory: factory, // Store the provided factory
	}
}

// Publish connects to the MQTT broker and starts publishing messages from all devices concurrently.
func (p *mqttPublisher) Publish(devices []*Device, duration time.Duration, ctx context.Context) error {
	p.logger.Info().Int("num_devices", len(devices)).Dur("target_duration", duration).
		Msg("Publisher starting all Devices concurrently...")

	err := p.connectMQTT()
	if err != nil {
		p.logger.Error().Err(err).Msg("Failed to connect MQTT client for publisher")
		return err
	}
	defer p.disconnectMQTT()

	var wg sync.WaitGroup
	for _, device := range devices {
		wg.Add(1)
		go func(d *Device) {
			defer wg.Done()
			p.publishMessages(d, duration, ctx)
		}(device)
	}
	p.logger.Info().Int("num_goroutines_launched", len(devices)).Msg("All device publishing goroutines launched.")
	wg.Wait()
	p.logger.Info().Msg("All device publishing goroutines have completed.")
	return nil
}

// connectMQTT establishes a connection to the MQTT broker using the client factory.
func (p *mqttPublisher) connectMQTT() error {
	opts := mqtt.NewClientOptions().
		AddBroker(p.brokerURL).
		SetClientID(p.clientID).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(client mqtt.Client, err error) { // paho.mqtt.Client here for handler signature
			p.logger.Error().Err(err).Msg("MQTT Connection lost")
		}).SetOnConnectHandler(func(client mqtt.Client) { // paho.mqtt.Client here for handler signature
		p.logger.Info().Str("broker", p.brokerURL).Msg("Successfully connected to MQTT broker")
	})

	// Use the factory to create the client.
	p.mqttClient = p.clientFactory.NewClient(opts)
	if p.mqttClient == nil {
		return errors.New("client factory returned a nil client")
	}

	connectToken := p.mqttClient.Connect() // This now returns MQTTToken
	if connectToken == nil {
		return errors.New("mqtt client Connect() returned a nil token")
	}

	if connectToken.WaitTimeout(10*time.Second) && connectToken.Error() != nil {
		err := connectToken.Error()
		p.logger.Error().Err(err).Str("broker", p.brokerURL).Msg("Failed to connect to MQTT broker")
		return err
	}
	if !p.mqttClient.IsConnected() {
		p.logger.Error().Str("broker", p.brokerURL).Msg("MQTT client did not connect successfully, though no immediate error was reported by token.")
		return errors.New("mqtt client failed to connect without explicit token error")
	}
	return nil
}

// disconnectMQTT disconnects the MQTT client if it is connected.
func (p *mqttPublisher) disconnectMQTT() {
	if p.mqttClient != nil && p.mqttClient.IsConnected() {
		p.logger.Debug().Msg("Disconnecting MQTT client")
		p.mqttClient.Disconnect(250)
		p.logger.Info().Msg("MQTT client disconnected.")
	} else {
		p.logger.Debug().Msg("MQTT client was nil or not connected, skipping disconnect.")
	}
}

// publishMessages handles the message publishing loop for a single device.
func (p *mqttPublisher) publishMessages(d *Device, testLoopDuration time.Duration, ctx context.Context) {
	topic := strings.ReplaceAll(p.topicPattern, "{DEVICE_EUI}", d.eui)
	var interval time.Duration
	if d.messageRate > 0 {
		interval = time.Duration(float64(time.Second) / d.messageRate)
	} else {
		p.logger.Warn().Str("device_eui", d.eui).Msg("Message rate per device is zero or negative, no messages will be published by this device.")
		return
	}

	d.delivery = MessageDelivery{
		ExpectedCount: int(testLoopDuration.Seconds() * d.messageRate),
		Published:     0,
	}

	p.logger.Info().Str("device_eui", d.eui).Str("topic", topic).Float64("rate_hz", d.messageRate).Dur("interval_ms", interval).
		Dur("device_lifetime_from_config", testLoopDuration).Msg("Device started publishing loop.")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	msgCount := 0
	for {
		select {
		case <-ctx.Done():
			p.logger.Info().Str("device_eui", d.eui).Int("total_messages_sent_by_device", msgCount).
				Msg("Device publishing loop stopping due to context cancellation.")
			return
		case tickTime := <-ticker.C:
			if ctx.Err() != nil {
				p.logger.Debug().Str("device_eui", d.eui).Msg("Context cancelled just before publish attempt, skipping.")
				continue
			}

			rawPayloadBytes, err := d.GeneratePayload()
			if err != nil {
				p.logger.Err(err).Msg("failed to generate payload")
				continue
			}
			rawPayloadHex := hex.EncodeToString(rawPayloadBytes)
			mqttMsg := MQTTMessage{
				DeviceInfo:       DeviceInfo{DeviceEUI: d.eui},
				RawPayload:       rawPayloadHex,
				MessageTimestamp: time.Now().UTC(),
				ClientMessageID:  uuid.NewString(),
				LoRaWAN:          LoRaWANData{ReceivedAt: time.Now().UTC().Add(-1 * time.Second)},
			}
			jsonData, err := json.Marshal(mqttMsg)
			if err != nil {
				p.logger.Error().Err(err).Str("device_eui", d.eui).Msg("Error marshalling MQTT message to JSON")
				continue
			}

			p.logger.Debug().Str("device_eui", d.eui).Time("tick_time", tickTime).Int("msg_count_attempt_device", msgCount+1).
				Str("topic", topic).Msg("Ticker fired, attempting to publish message.")

			if p.mqttClient == nil || !p.mqttClient.IsConnected() {
				p.logger.Error().Str("device_eui", d.eui).Msg("MQTT client is nil or not connected. Cannot publish message.")
				continue
			}

			pubToken := p.mqttClient.Publish(topic, byte(p.QOS), false, jsonData) // pubToken is MQTTToken
			if pubToken == nil {
				p.logger.Error().Str("device_eui", d.eui).Str("topic", topic).Msg("Publish returned a nil token")
				continue
			}

			if pubToken.Error() != nil {
				p.logger.Error().Err(pubToken.Error()).Str("device_eui", d.eui).Str("topic", topic).Msg("Error publishing message")
			} else {
				msgCount++
				p.logger.Debug().Str("device_eui", d.eui).Int("msg_count_sent_device", msgCount).Str("topic", topic).Msg("Message published successfully")
			}
		}
	}
}
