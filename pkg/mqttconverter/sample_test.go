package mqttconverter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// To run this test, you must have an MQTT broker accessible.
// It uses the public mosquitto broker by default.
// You must also set the environment variable:
// RUN_INTEGRATION_TESTS=true go test -v -run TestSampler_Integration

const (
	testBrokerURL = "tcp://test.mosquitto.org:1883"
	testTopicBase = "go-sampler-test"
)

func TestSampler_Integration(t *testing.T) {
	t.Setenv("RUN_INTEGRATION_TESTS", "true")
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run.")
	}

	// --- Test Setup ---
	// Create a unique topic for this test run to avoid interference
	testTopic := fmt.Sprintf("%s/%d", testTopicBase, time.Now().UnixNano())
	numTestMessages := 3

	// Configure the sampler
	cfg := MQTTClientConfig{
		BrokerURL:      testBrokerURL,
		Topic:          testTopic,
		ClientIDPrefix: "sampler-integration-test-",
		ConnectTimeout: 10 * time.Second,
	}

	// Use a logger that writes to the test output for better debugging
	logger := zerolog.New(zerolog.NewTestWriter(t))
	sampler := NewSampler(cfg, logger, numTestMessages)

	// Context with a timeout to prevent the test from hanging indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	// --- Run the Sampler in a goroutine ---
	go func() {
		defer wg.Done()
		err := sampler.Run(ctx)
		assert.NoError(t, err, "Sampler.Run should not return an error")
	}()

	// --- Give the sampler a moment to connect and subscribe ---
	time.Sleep(3 * time.Second)

	// --- Create a separate publisher client to send test messages ---
	publisherClient := createTestPublisher(t, testBrokerURL)
	require.NotNil(t, publisherClient)
	defer publisherClient.Disconnect(250)

	// --- Publish test messages ---
	for i := 0; i < numTestMessages; i++ {
		payload := fmt.Sprintf(`{"message_number": %d, "test_id": "sampler-integration"}`, i+1)
		token := publisherClient.Publish(testTopic, 1, false, []byte(payload))
		token.Wait()
		require.NoError(t, token.Error(), "Publisher should successfully publish message")
		logger.Info().Int("message_number", i+1).Msg("Published test message")
	}

	// --- Wait for the sampler to finish its run ---
	wg.Wait()

	// --- Assertions ---
	captured := sampler.Messages()
	assert.Len(t, captured, numTestMessages, "Should have captured the exact number of messages sent")

	// Verify the content of one of the messages
	if len(captured) > 0 {
		assert.Contains(t, string(captured[0].Payload), `"message_number": 1`)
		assert.Equal(t, testTopic, captured[0].Topic)
	}
}

// createTestPublisher is a helper to create a simple MQTT client for publishing.
func createTestPublisher(t *testing.T, brokerURL string) mqtt.Client {
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(fmt.Sprintf("test-publisher-%d", time.Now().UnixNano()))

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.WaitTimeout(10*time.Second) && token.Error() != nil {
		require.NoError(t, token.Error(), "Test publisher should connect without error")
		return nil
	}
	return client
}
