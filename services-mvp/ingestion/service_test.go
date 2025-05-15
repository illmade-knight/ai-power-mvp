package ingestion

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog/log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// mockMetadataFetcher (can be reused from previous tests or defined here if preferred)
func mockMetadataFetcherFunc(deviceEUI string, expectedEUI string, clientID, locationID, category string, returnErr error) DeviceMetadataFetcher {
	return func(eui string) (string, string, string, error) {
		if eui != expectedEUI {
			return "", "", "", errors.New("mock fetcher called with unexpected EUI: " + eui + ", expected: " + expectedEUI)
		}
		return clientID, locationID, category, returnErr
	}
}

// --- MockMessagePublisher ---
type MockMessagePublisher struct {
	PublishedMessagesChan chan *EnrichedMessage
	logger                zerolog.Logger
	stopCalled            bool
	mu                    sync.Mutex
	PublishError          error // Optional: Set this to simulate publish errors
}

func NewMockMessagePublisher(bufferSize int, logger zerolog.Logger) *MockMessagePublisher {
	return &MockMessagePublisher{
		PublishedMessagesChan: make(chan *EnrichedMessage, bufferSize),
		logger:                logger,
	}
}

func (mp *MockMessagePublisher) Publish(_ context.Context, message *EnrichedMessage) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.PublishError != nil {
		mp.logger.Warn().Err(mp.PublishError).Msg("MockMessagePublisher: Simulating publish error")
		return mp.PublishError
	}

	if mp.stopCalled {
		mp.logger.Warn().Msg("MockMessagePublisher: Publish called after Stop")
		return errors.New("publisher stopped")
	}

	select {
	case mp.PublishedMessagesChan <- message:
		mp.logger.Debug().Str("device_eui", message.DeviceEUI).Msg("MockMessagePublisher: Message sent to PublishedMessagesChan")
		return nil
	default:
		// This case means the channel is full, simulating a downstream blockage
		// or a test not consuming messages quickly enough.
		mp.logger.Error().Str("device_eui", message.DeviceEUI).Msg("MockMessagePublisher: PublishedMessagesChan is full, message dropped")
		return errors.New("mock publisher channel full")
	}
}

func (mp *MockMessagePublisher) Stop() {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if !mp.stopCalled {
		close(mp.PublishedMessagesChan)
		mp.stopCalled = true
		mp.logger.Info().Msg("MockMessagePublisher: Stopped and PublishedMessagesChan closed")
	}
}

// --- Test Setup Helpers (createTestPublisherClient, setupMosquittoContainer, integrationMockFetcher) ---
// These helpers can remain largely the same as in the previous version of this file.
// For brevity, I'll assume they are present and correct.
// Ensure setupMosquittoContainer uses the constants defined above for this test file if needed.
// Helper to create a simple Paho publisher client for sending test messages.
func createTestPublisherClientForMockTest(brokerURL string, clientID string, logger zerolog.Logger) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetConnectTimeout(5 * time.Second)
	opts.SetWriteTimeout(5 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		logger.Error().Err(err).Str("client_id", clientID).Msg("Test publisher lost connection")
	}
	opts.OnConnect = func(client mqtt.Client) {
		logger.Info().Str("client_id", clientID).Str("broker", brokerURL).Msg("Test publisher connected")
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(10*time.Second) && token.Error() != nil {
		return nil, fmt.Errorf("test publisher (client ID: %s) failed to connect to %s: %w", clientID, brokerURL, token.Error())
	}
	return client, nil
}

func TestIngestionService_SuccessfulProcessing(t *testing.T) {
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel)

	fetcher := mockMetadataFetcherFunc("DEV001", "DEV001", "ClientA", "Loc1", "Sensor", nil)
	cfg := DefaultIngestionServiceConfig()
	cfg.NumProcessingWorkers = 1 // Easier to reason about for single message test
	mp := NewMockMessagePublisher(cfg.NumProcessingWorkers, logger)
	service := NewIngestionService(fetcher, mp, logger, cfg, nil)

	service.Start()

	sampleTime := time.Now().Truncate(time.Second) // Truncate for easier comparison
	rawMsgBytes := makeSampleRawMQTTMsgBytes("DEV001", "TestData", sampleTime)

	// Send message to service
	service.RawMessagesChan <- rawMsgBytes

	var enrichedMsg *EnrichedMessage
	select {
	case enrichedMsg = <-mp.PublishedMessagesChan:
		// Message received
	case err := <-service.ErrorChan:
		t.Fatalf("Received unexpected error on ErrorChan: %v. Log: %s", err, logBuf.String())
	case <-time.After(2 * time.Second): // Timeout
		t.Fatalf("Timeout waiting for enriched message. Log: %s", logBuf.String())
	}

	if enrichedMsg == nil {
		t.Fatal("Enriched message is nil")
	}

	if enrichedMsg.DeviceEUI != "DEV001" {
		t.Errorf("Expected DeviceEUI 'DEV001', got '%s'", enrichedMsg.DeviceEUI)
	}
	if enrichedMsg.ClientID != "ClientA" {
		t.Errorf("Expected ClientID 'ClientA', got '%s'", enrichedMsg.ClientID)
	}
	if enrichedMsg.RawPayload != "TestData" {
		t.Errorf("Expected RawPayload 'TestData', got '%s'", enrichedMsg.RawPayload)
	}
	// Compare OriginalMQTTTime carefully due to potential time zone differences if not handled consistently
	if !enrichedMsg.OriginalMQTTTime.Equal(sampleTime) {
		t.Errorf("Expected OriginalMQTTTime %v, got %v", sampleTime, enrichedMsg.OriginalMQTTTime)
	}

	service.Stop() // Gracefully stop the service

	// Check logs for success (optional)
	if !strings.Contains(logBuf.String(), "Successfully processed and published enriched message") {
		t.Errorf("Missing success log. Logs: %s", logBuf.String())
	}
}

func TestIngestionService_ParsingError(t *testing.T) {
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel)

	// Fetcher won't be called if parsing fails
	fetcher := mockMetadataFetcherFunc("any", "any", "", "", "", errors.New("fetcher should not be called"))
	cfg := DefaultIngestionServiceConfig()
	cfg.NumProcessingWorkers = 1
	mp := NewMockMessagePublisher(cfg.NumProcessingWorkers, logger)
	service := NewIngestionService(fetcher, mp, logger, cfg, nil)
	service.Start()

	malformedJSON := []byte("this is not json")
	service.RawMessagesChan <- malformedJSON

	select {
	case em := <-mp.PublishedMessagesChan:
		t.Fatalf("Received unexpected enriched message: %v", em)
	case err := <-service.ErrorChan:
		t.Logf("Received expected error on ErrorChan: %v", err)
		// Optionally check if errors.Is(err, &json.SyntaxError{}) or similar
		if err == nil { // Should be an error
			t.Error("ErrorChan received nil error")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for error message on ErrorChan")
	}

	service.Stop()

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "Failed to parse raw MQTT message") {
		t.Errorf("Expected log for parsing failure, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "this is not json") { // Check snippet
		t.Errorf("Expected raw message snippet in log, got: %s", logOutput)
	}
}

func TestIngestionService_MetadataFetchingError(t *testing.T) {
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel)

	// Mock fetcher to return an error
	fetcher := mockMetadataFetcherFunc("DEV002", "DEV002", "", "", "", ErrMetadataNotFound)
	cfg := DefaultIngestionServiceConfig()
	cfg.NumProcessingWorkers = 1
	mp := NewMockMessagePublisher(cfg.NumProcessingWorkers, logger)
	service := NewIngestionService(fetcher, mp, logger, cfg, nil)
	service.Start()

	rawMsgBytes := makeSampleRawMQTTMsgBytes("DEV002", "TestData2", time.Now())
	service.RawMessagesChan <- rawMsgBytes

	select {
	case em := <-mp.PublishedMessagesChan:
		t.Fatalf("Received unexpected enriched message: %v", em)
	case err := <-service.ErrorChan:
		t.Logf("Received expected error on ErrorChan: %v", err)
		if !errors.Is(err, ErrMetadataNotFound) {
			t.Errorf("Expected ErrMetadataNotFound, got %T: %v", err, err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for error message on ErrorChan")
	}

	service.Stop()

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "Failed to enrich MQTT message") {
		t.Errorf("Expected log for enrichment failure, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, ErrMetadataNotFound.Error()) {
		t.Errorf("Expected metadata error string in log, got: %s", logOutput)
	}
}

func TestIngestionService_StartStopMultipleWorkers(t *testing.T) {
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.InfoLevel) // Use Info to reduce log noise

	fetcher := mockMetadataFetcherFunc("DEV001", "DEV001", "ClientA", "Loc1", "Sensor", nil)
	cfg := DefaultIngestionServiceConfig()
	cfg.NumProcessingWorkers = 3 // Test with multiple workers
	cfg.InputChanCapacity = 10
	cfg.OutputChanCapacity = 10

	mp := NewMockMessagePublisher(cfg.NumProcessingWorkers, logger)
	service := NewIngestionService(fetcher, mp, logger, cfg, nil)
	service.Start()

	numMessages := 5
	var receivedMessages []*EnrichedMessage
	var wgReceivers sync.WaitGroup
	wgReceivers.Add(1)

	go func() { // Goroutine to collect enriched messages
		defer wgReceivers.Done()
		for msg := range mp.PublishedMessagesChan { // Drains the channel until it's closed by Stop()
			receivedMessages = append(receivedMessages, msg)
		}
	}()

	var errorCount int
	var wgErrors sync.WaitGroup
	wgErrors.Add(1)
	go func() { // Goroutine to collect errors
		defer wgErrors.Done()
		for err := range service.ErrorChan {
			if err != nil {
				errorCount++
			}
		}
	}()

	for i := 0; i < numMessages; i++ {
		rawMsgBytes := makeSampleRawMQTTMsgBytes("DEV001", "TestData", time.Now())
		service.RawMessagesChan <- rawMsgBytes
	}

	// Give some time for messages to be processed before stopping
	// This is a bit arbitrary; a more robust way would be to count outputs.
	time.Sleep(1500 * time.Millisecond)

	service.Stop()     // This will close EnrichedMessagesChan, ending the receiver goroutine's loop
	wgReceivers.Wait() // Wait for receiver to finish
	wgErrors.Wait()    // Wait for error collector to finish

	if len(receivedMessages) != numMessages {
		t.Errorf("Expected %d enriched messages, got %d. Logs: %s", numMessages, len(receivedMessages), logBuf.String())
	}
	if errorCount != 0 {
		t.Errorf("Expected 0 errors, got %d. Logs: %s", errorCount, logBuf.String())
	}

	logOutput := logBuf.String()
	log.Debug().Str("buf", logOutput).Msg("log output")
	if !strings.Contains(logOutput, "Starting IngestionService...") || !strings.Contains(logOutput, "IngestionService started") {
		t.Errorf("Missing service start logs. Logs: %s", logOutput)
	}
	if !strings.Contains(logOutput, "Stopping IngestionService...") || !strings.Contains(logOutput, "IngestionService stopped") {
		t.Errorf("Missing service stop logs. Logs: %s", logOutput)
	}
	if strings.Count(logOutput, "Starting processing worker") != cfg.NumProcessingWorkers {
		t.Errorf("Expected %d worker start logs, got different. Logs: %s", cfg.NumProcessingWorkers, logOutput)
	}
}

func TestIngestionService_ShutdownWithPendingMessages(t *testing.T) {
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel)

	// This fetcher will introduce a small delay, simulating work
	delayedFetcher := func(deviceEUI string) (string, string, string, error) {
		time.Sleep(50 * time.Millisecond) // Simulate work
		if deviceEUI == "DEV_OK" {
			return "ClientB", "Loc2", "TypeX", nil
		}
		return "", "", "", ErrMetadataNotFound
	}

	cfg := DefaultIngestionServiceConfig()
	cfg.NumProcessingWorkers = 2
	cfg.InputChanCapacity = 5
	cfg.OutputChanCapacity = 5
	mp := NewMockMessagePublisher(cfg.NumProcessingWorkers, logger)
	service := NewIngestionService(delayedFetcher, mp, logger, cfg, nil)
	service.Start()

	// Send some messages
	service.RawMessagesChan <- makeSampleRawMQTTMsgBytes("DEV_OK", "Payload1", time.Now())
	service.RawMessagesChan <- makeSampleRawMQTTMsgBytes("DEV_OK", "Payload2", time.Now())
	service.RawMessagesChan <- makeSampleRawMQTTMsgBytes("DEV_BAD", "Payload3", time.Now()) // This will cause a fetch error

	// Immediately stop the service
	// The goal is to see if Stop() waits for in-flight messages to be processed
	// or at least for workers to attempt processing them.
	go service.Stop() // Run stop in a goroutine as it blocks until workers are done

	var processedCount int
	var errorCount int
	var wgDone sync.WaitGroup
	wgDone.Add(2) // For EnrichedMessagesChan and ErrorChan consumers

	// Drain EnrichedMessagesChan
	go func() {
		defer wgDone.Done()
		for range mp.PublishedMessagesChan {
			processedCount++
		}
	}()

	// Drain ErrorChan
	go func() {
		defer wgDone.Done()
		for range service.ErrorChan {
			errorCount++
		}
	}()

	wgDone.Wait() // Wait for both channels to be fully drained (closed by Stop)

	// Assertions:
	// We expect 2 successful messages and 1 error.
	// The exact timing and whether all make it through before Stop completes can be tricky,
	// but Stop should ensure workers attempt to process what's in their immediate buffers.
	if processedCount != 2 {
		processedCount += strings.Count(logBuf.String(), "Shutdown signaled while trying to send enriched message")
		if processedCount != 2 {
			t.Errorf("Expected 2 processed messages, got %d. Logs:\n%s", processedCount, logBuf.String())
		}
	}
	// this is very specific for the conditions around the error message we've sent
	if errorCount != 1 {
		errorCount += strings.Count(logBuf.String(), "Shutdown signaled, Paho message dropped")
		if errorCount != 1 {
			t.Errorf("Expected 1 error, got %d. Logs:\n%s", errorCount, logBuf.String())
		}
	}

	if !strings.Contains(logBuf.String(), "Processing worker shutting down") {
		t.Errorf("Missing worker shutdown logs. Logs:\n%s", logBuf.String())
	}
}
