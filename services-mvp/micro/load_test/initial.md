Load Testing Strategy: IoT Ingestion Service
Objective: To evaluate the performance (throughput, latency, resource utilization) of the ingestionservice under various load conditions, mimicking a large number of devices sending messages via MQTT, which are then processed and published to Google Cloud Pub/Sub.

1. Define Key Performance Indicators (KPIs) & Goals
   Before testing, define what success looks like:

Target Throughput:

Messages per second (MPS) the ingestionservice should sustain.

Consider average and peak load expectations.

Latency:

End-to-End Latency: Time from an MQTT message published by a "device" to its corresponding message appearing on the target Pub/Sub topic (enriched or unidentified). Define acceptable P50, P90, P99 latencies.

Internal Processing Latency: Time taken within ingestionservice (MQTT receive -> metadata fetch -> enrichment -> Pub/Sub publish call).

Resource Utilization:

CPU and memory usage of the ingestionservice instance(s) under load.

Resource usage of the MQTT broker.

Error Rates:

MQTT connection/publish errors (from load generator).

ingestionservice internal processing errors (e.g., metadata not found, marshalling issues).

Pub/Sub publish errors.

Acceptable error rate (e.g., < 0.01%).

Scalability (Future Goal): How does the system perform as you scale out ingestionservice instances (if applicable)?

2. Test Environment Setup
   System Under Test (SUT):

Deploy the ingestionservice (using the server.go wrapper). This can be a local binary build or a Docker container running locally.

Ensure it's configured to connect to your real (but test-dedicated) GCP Pub/Sub topics and Firestore instance.

MQTT Broker:

Recommendation: Run a high-performance MQTT broker locally, typically in Docker (e.g., eclipse-mosquitto, emqx). This minimizes network latency between the load generator and the broker for initial tests.

Configure it to handle a large number of connections and messages.

Google Cloud Platform (Real, Test-Dedicated Resources):

Pub/Sub: Create dedicated Pub/Sub topics in a test GCP project for "enriched" and "unidentified" messages. These should be separate from your development or production topics.

Firestore: Use a Firestore instance (ideally in a test project or with a specific test collection prefix) for metadata lookups. Populate it with a representative dataset of device metadata (including EUIs that will be simulated, and some that won't to test the "unidentified" path).

Cloud Monitoring: Ensure you have access to Cloud Monitoring for your test GCP project to observe Pub/Sub metrics (publish rates, unacked messages, etc.).

Network:

If running the load generator and ingestionservice locally, ensure sufficient network bandwidth if connecting to GCP services over the internet. For more controlled tests, consider running the SUT on a GCE VM in the same region as your Pub/Sub/Firestore to minimize external network variables.

3. Load Generation
   The goal is to simulate many devices sending MQTT messages.

Tooling:

Custom Go Load Generator (Recommended for Control):

Develop a Go application that can spawn a configurable number of goroutines.

Each goroutine acts as an independent MQTT client, representing a unique device EUI.

Each client connects to the MQTT broker and publishes messages on a device-specific topic (e.g., devices/<DeviceEUI>/up).

Parameters: Number of devices, message rate per device, message payload template, test duration, ramp-up time.

Existing MQTT Load Tools:

MQTTX CLI (can be scripted for benchmarks).

emqtt-bench.

k6 with an MQTT extension (more general-purpose load testing tool).

Message Characteristics:

Payloads: Use realistic JSON payloads for your connectors.MQTTMessage struct. Vary payloads if necessary.

Device EUIs: Generate a large set of unique device EUIs for the simulated devices. Ensure a portion of these EUIs exist in your test Firestore database and a portion do not, to test both enrichment paths.

Timestamps: Embed a client_publish_timestamp in each MQTT message payload. This will be crucial for measuring end-to-end latency.

Message ID: Include a unique message ID in the payload for tracking.

Load Profile:

Ramp-up: Gradually increase the number of messages per second to identify at what point performance degrades.

Sustained Load: Run tests at a target MPS for an extended period (e.g., 15-60 minutes) to check stability and resource consumption over time.

Spike/Burst Test: Simulate sudden bursts of messages.

4. Metrics Collection & Measurement
   A. End-to-End Latency:

Method:

The load generator embeds client_publish_timestamp in the MQTT message.

A separate "Verifier" application (a Go program you'll write) subscribes to your load test "enriched" and "unidentified" Pub/Sub topics.

When the Verifier receives a message, it extracts the client_publish_timestamp from the payload and compares it to the current time (or the Pub/Sub message's publish time if you want to isolate ingestionservice + Pub/Sub publish latency).

Reporting: The Verifier should calculate and log/report P50, P90, P99 latencies, and total messages received per topic.

B. ingestionservice Throughput & Internal Latencies:

Structured Logging: Add detailed structured logs (e.g., using zerolog) within ingestionservice at key stages:

MQTT message received.

Metadata fetch attempt (start/end/success/failure/cache-hit/miss).

Enrichment complete.

Pub/Sub publish attempt (start/end/success/failure for each topic).

Include message IDs and timestamps in logs.

Application Metrics (Optional but Recommended):

Use a library like Prometheus Go client to expose internal counters and histograms from ingestionservice (e.g., messages processed, metadata cache hit rate, Pub/Sub publish latency per topic).

C. MQTT Broker Metrics:

Messages in/out, active connections, queue lengths (if applicable). Most brokers expose these.

D. Google Cloud Pub/Sub Metrics (via Cloud Monitoring):

topic/send_request_count

topic/send_request_bytes

topic/send_request_latencies

subscription/num_undelivered_messages (on the output topics – a growing number indicates the Verifier can't keep up or there's an issue).

E. ingestionservice System Resource Usage:

Monitor CPU and Memory utilization of the machine/container running ingestionservice.

F. Error Rates:

Track errors logged by the load generator (MQTT publish failures).

Track errors logged by ingestionservice.

Track errors reported by the Pub/Sub Verifier (e.g., malformed messages).

5. Test Execution & Analysis Workflow
   Prepare Environment: Start emulators (if using any for pre-testing), ensure GCP resources are ready, seed Firestore.

Start Verifier Application: Ensure it's subscribing to the correct output Pub/Sub topics.

Start ingestionservice: With appropriate logging levels.

Start Load Generator: Begin with a conservative load profile.

Monitor: Observe live metrics (Cloud Monitoring, ingestionservice logs/metrics, Verifier logs).

Collect Results: After the test duration, gather all logs and metrics data.

Analyze:

Compare results against your KPIs.

Identify bottlenecks: Is it CPU, memory, I/O, metadata fetching, Pub/Sub publishing limits, MQTT broker limits?

Look for error patterns.

Iterate: Make code or configuration adjustments to ingestionservice, then re-run tests.

Gradually Increase Load: Once stable at a certain level, increase the load to find the breaking point or desired capacity.

First Practical Steps to Implement This Strategy:
Develop the MQTT Load Generator:

Start with a simple Go script using paho.mqtt.golang.

Parameterize: number of devices, message rate, duration, broker URL, topic structure.

Ensure it can embed timestamps and unique IDs in payloads.

Develop the Pub/Sub Verifier:

A Go application that subscribes to the output topics.

Calculates and logs/prints basic latency stats (min, max, average, count).

Instrument ingestionservice:

Add detailed structured logging with timings for critical path operations.

Setup Initial Test Environment:

Run Mosquitto in Docker locally.

Deploy ingestionservice locally.

Create dedicated "load-test-enriched" and "load-test-unidentified" topics in your GCP test project.

Populate Firestore with test data for a subset of simulated device EUIs.

Conduct a Small-Scale Run:

Simulate a low number of devices (e.g., 10-50) with a low message rate (e.g., 1 msg/sec/device).

Verify the end-to-end flow and data collection mechanisms.

Check Cloud Monitoring for Pub/Sub metrics.

This systematic approach will provide valuable insights into your ingestionservice's capabilities and help you prepare for production deployment.