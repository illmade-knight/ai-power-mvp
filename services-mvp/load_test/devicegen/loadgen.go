package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// MQTTMessage represents the structure of the JSON message to be published.
// This should align with what your ingestionservice expects.
type MQTTMessage struct {
	DeviceInfo       DeviceInfo  `json:"device_info"`
	LoRaWAN          LoRaWANData `json:"lorawan_data,omitempty"`
	RawPayload       string      `json:"raw_payload"`       // Hex-encoded string
	MessageTimestamp time.Time   `json:"message_timestamp"` // Client-side publish timestamp
	ClientMessageID  string      `json:"client_message_id"` // Unique ID for tracking
}

// DeviceInfo contains identifiers for the end device.
type DeviceInfo struct {
	DeviceEUI string `json:"device_eui"`
}

// LoRaWANData is placeholder, can be expanded if needed for your tests.
type LoRaWANData struct {
	ReceivedAt time.Time `json:"received_at,omitempty"`
}

// XDeviceDecodedPayload is the structure we're aiming to simulate
// before it gets hex-encoded into MQTTMessage.RawPayload.
// This mirrors the structure from your uploaded xdevice.decoder.go.
type XDeviceDecodedPayload struct {
	UID            string // 4-byte string
	Reading        float32
	AverageCurrent float32
	MaxCurrent     float32
	MaxVoltage     float32
	AverageVoltage float32
}

const expectedXDevicePayloadLengthBytes = 24

// generateXDeviceRawPayloadBytes creates a byte slice representing the XDevice payload.
func generateXDeviceRawPayloadBytes(deviceIndex int) ([]byte, error) {
	payload := XDeviceDecodedPayload{
		UID:            fmt.Sprintf("DEV%01d", deviceIndex%10), // Cycle through DEV0-DEV9 for the 4-char UID
		Reading:        rand.Float32() * 100,
		AverageCurrent: rand.Float32() * 10,
		MaxCurrent:     rand.Float32() * 15,
		MaxVoltage:     rand.Float32() * 250,
		AverageVoltage: rand.Float32() * 240,
	}

	if len(payload.UID) > 4 {
		payload.UID = payload.UID[:4]
	} else if len(payload.UID) < 4 {
		payload.UID = fmt.Sprintf("%-4s", payload.UID) // Pad with spaces if too short
	}

	buf := make([]byte, expectedXDevicePayloadLengthBytes)
	offset := 0

	copy(buf[offset:offset+4], []byte(payload.UID))
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.Reading))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.AverageCurrent))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.MaxCurrent))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.MaxVoltage))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:offset+4], math.Float32bits(payload.AverageVoltage))

	return buf, nil
}

func runDevice(
	ctx context.Context,
	wg *sync.WaitGroup,
	deviceIndex int,
	brokerURL string,
	topicPattern string,
	msgRatePerSec float64,
	qos byte,
	clientIDPrefix string,
) {
	defer wg.Done()

	deviceEUI := fmt.Sprintf("LOADTEST-%06d", deviceIndex) // Example: LOADTEST-000001
	clientID := fmt.Sprintf("%s-%s-%d", clientIDPrefix, deviceEUI, time.Now().UnixNano())
	topic := strings.ReplaceAll(topicPattern, "{DEVICE_EUI}", deviceEUI)

	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectTimeout(10 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Printf("[%s] Connection lost: %v", deviceEUI, err)
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			log.Printf("[%s] Connected to MQTT broker", deviceEUI)
		})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		log.Printf("[%s] Failed to connect to MQTT broker: %v", deviceEUI, token.Error())
		return
	}
	defer client.Disconnect(250)

	log.Printf("[%s] Started publishing to topic: %s at %.2f msg/sec", deviceEUI, topic, msgRatePerSec)

	var interval time.Duration
	if msgRatePerSec > 0 {
		interval = time.Duration(float64(time.Second) / msgRatePerSec)
	} else {
		interval = 1 * time.Second // Default to 1 msg/sec if rate is zero or negative
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	msgCount := 0
	for {
		select {
		case <-ticker.C:
			rawPayloadBytes, err := generateXDeviceRawPayloadBytes(deviceIndex)
			if err != nil {
				log.Printf("[%s] Error generating raw payload bytes: %v", deviceEUI, err)
				continue
			}
			rawPayloadHex := hex.EncodeToString(rawPayloadBytes)

			mqttMsg := MQTTMessage{
				DeviceInfo:       DeviceInfo{DeviceEUI: deviceEUI},
				RawPayload:       rawPayloadHex,
				MessageTimestamp: time.Now().UTC(),
				ClientMessageID:  uuid.NewString(),
				LoRaWAN:          LoRaWANData{ReceivedAt: time.Now().UTC().Add(-1 * time.Second)}, // Example
			}

			jsonData, err := json.Marshal(mqttMsg)
			if err != nil {
				log.Printf("[%s] Error marshalling MQTT message: %v", deviceEUI, err)
				continue
			}

			token := client.Publish(topic, qos, false, jsonData)
			// token.WaitTimeout(2 * time.Second) // Optional: wait for ack for QoS 1/2, but can slow down
			if token.Error() != nil {
				log.Printf("[%s] Error publishing message: %v", deviceEUI, token.Error())
			} else {
				msgCount++
				if msgCount%100 == 0 { // Log progress every 100 messages per device
					log.Printf("[%s] Published %d messages", deviceEUI, msgCount)
				}
			}

		case <-ctx.Done():
			log.Printf("[%s] Stopping device publisher. Total messages published: %d", deviceEUI, msgCount)
			return
		}
	}
}

func main() {
	brokerURL := flag.String("broker", "tcp://localhost:1883", "MQTT broker URL (e.g., tcp://localhost:1883, tls://mqtt.example.com:8883)")
	topicPattern := flag.String("topic", "devices/{DEVICE_EUI}/up", "MQTT topic pattern (use {DEVICE_EUI} as placeholder)")
	numDevices := flag.Int("devices", 10, "Number of concurrent devices to simulate")
	msgRate := flag.Float64("rate", 1.0, "Messages per second per device")
	duration := flag.Duration("duration", 1*time.Minute, "Total duration for the load test (e.g., 30s, 2m, 1h)")
	qos := flag.Int("qos", 0, "MQTT QoS level (0, 1, or 2)")
	clientIDPrefix := flag.String("clientid-prefix", "loadgen", "Prefix for MQTT client IDs")
	flag.Parse()

	if *numDevices <= 0 {
		log.Fatal("Number of devices must be greater than 0")
	}
	if *msgRate <= 0 {
		log.Printf("Warning: Message rate is %.2f, defaulting to 1 msg/sec for actual publishing interval calculation if rate is non-positive.", *msgRate)
	}
	if *duration <= 0 {
		log.Fatal("Duration must be positive")
	}
	if !strings.Contains(*topicPattern, "{DEVICE_EUI}") {
		log.Fatal("Topic pattern must contain '{DEVICE_EUI}' placeholder")
	}

	log.Printf("Starting MQTT Load Generator...")
	log.Printf("Broker: %s", *brokerURL)
	log.Printf("Topic Pattern: %s", *topicPattern)
	log.Printf("Number of Devices: %d", *numDevices)
	log.Printf("Message Rate per Device: %.2f msg/sec", *msgRate)
	log.Printf("Test Duration: %v", *duration)
	log.Printf("QoS: %d", *qos)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Handle SIGINT and SIGTERM for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down...", sig)
		cancel() // Trigger context cancellation
	}()

	var wg sync.WaitGroup
	for i := 0; i < *numDevices; i++ {
		wg.Add(1)
		go runDevice(ctx, &wg, i, *brokerURL, *topicPattern, *msgRate, byte(*qos), *clientIDPrefix)
		// Small stagger to avoid all clients connecting at the exact same microsecond
		if *numDevices > 10 && i%10 == 0 { // Stagger every 10 clients if many are being started
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}

	log.Printf("All %d device simulators launched. Running for %v.", *numDevices, *duration)
	wg.Wait() // Wait for all device goroutines to finish (either by duration or signal)
	log.Println("Load generation complete.")
}
