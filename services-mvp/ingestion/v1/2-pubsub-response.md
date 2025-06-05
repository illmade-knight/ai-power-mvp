Okay, that's a very smart way to adapt your existing integration tests\! By creating a MockMessagePublisher that uses a channel internally, you can effectively simulate the old EnrichedMessagesChan behavior and reuse a good portion of your test logic, including the shutdown handling.

I'll update the ingestion\_integration\_test\_go artifact. The main changes will be:

1. Introducing a MockMessagePublisher struct that implements the MessagePublisher interface.
2. Modifying the TestIngestionService\_Integration\_ReceiveAndProcessMessage to use this mock publisher.
3. Adjusting how the test collects and verifies the output (now from the mock publisher's channel).

Here's the updated code:

**Key Changes:**

1. **MockMessagePublisher Struct:**
    * Implements MessagePublisher.
    * PublishedMessagesChan: An unbuffered channel (or buffered, as you prefer) to send EnrichedMessages to.
    * Publish(): Sends the message to PublishedMessagesChan. Includes a default case in select to handle potential blocking if the channel is full (useful if the test doesn't consume messages quickly enough).
    * Stop(): Closes PublishedMessagesChan.
    * PublishError: An optional field you can set in your tests if you want to simulate the publisher failing.
2. **TestIngestionService\_Integration\_WithMockPublisher\_ReceiveAndProcessMessage:**
    * **Initialization**:
        * mockPublisher := NewMockMessagePublisher(...) is created.
        * This mockPublisher instance is passed to NewIngestionService.
    * **Verification**:
        * Instead of service.EnrichedMessagesChan, the test now reads from mockPublisher.PublishedMessagesChan to get the processed message.
        * The assertions on the content of receivedEnrichedMsg remain the same.
    * **Logging**: You can check for logs from the MockMessagePublisher itself to confirm it received and attempted to "publish" (i.e., send to its internal channel).

**Adapting TestIngestionService\_ShutdownWithPendingMessages:**

You would indeed adapt your unit test that handles shutdown races.

1. **Make it an Integration Test**: Add the //go:build integration tag.
2. **Setup**: Include Mosquitto setup via testcontainers-go and configure the IngestionService with the MockMessagePublisher.
3. **Message Sending**: Publish raw messages to Mosquitto using a test Paho client.
4. **Collecting Output**:
    * Drain mockPublisher.PublishedMessagesChan in a goroutine to count processedCount.
    * Drain service.ErrorChan to count errorCount.
5. **Log Counting for Dropped Messages**:
    * The log "Shutdown signaled while trying to send enriched message" is in IngestionService.processSingleMessage when s.publisher.Publish(...) is about to be called but the context is cancelled. This log is still relevant.
    * If s.publisher.Publish() itself returns an error because the *mock publisher's context* (passed to its Publish method) is cancelled, or if the mock publisher's internal channel send fails due to shutdown, that's another scenario.
    * Your existing logic for processedCount should largely work:  
      Go  
      // After draining mockPublisher.PublishedMessagesChan into processedItems list  
      processedCount := len(processedItems)  
      if processedCount \!= expectedSuccessful {  
      // Count logs from IngestionService indicating a message was enriched but publish was preempted by shutdown  
      processedCount \+= strings.Count(logBuf.String(), "Failed to publish enriched message") // if this log also captures context cancelled  
      // OR a more specific log if you add one for "publish attempt aborted by shutdown"  
      // OR the existing "Shutdown signaled while trying to send enriched message" if that's still the one that fires  
      // before s.publisher.Publish is even called.  
      // You need to be precise which log indicates "processed by EnrichMQTTData but not successfully given to publisher.Publish() due to shutdown"

          if processedCount \!= expectedSuccessful {  
             t.Errorf("Expected %d processed messages (including those dropped at publish during shutdown), got %d. Logs:\\n%s", expectedSuccessful, processedCount, logBuf.String())  
          }  
      }

    * For errorCount, you'd count items from service.ErrorChan. If a message that should have errored during enrichment was instead dropped before EnrichMQTTData completed due to shutdown, that's a similar "dropped during processing" scenario.

This mock publisher approach gives you good control for testing the IngestionService's interaction with its publishing dependency without needing a live Pub/Sub connection for *all* integration tests, while still testing the MQTT consumption part realistically.