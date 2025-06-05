You've hit on a very important point for robust data pipelines\! Losing data just because metadata is missing (which could be a temporary data entry issue) is definitely not ideal, especially when auditability is a concern.

Here's a strategy to handle messages from devices not yet registered in Firestore, ensuring their data is captured:

**Strategy: "Unidentified Device" Data Routing**

The core idea is to route messages for which metadata cannot be found to a separate Pub/Sub topic. This acts as a holding area or a "dead-letter queue" specifically for these types of messages.

1. **Detection in EnrichMQTTData:**
    * Your GoogleDeviceMetadataFetcher.Fetch method already correctly returns ErrMetadataNotFound when a device EUI isn't found in Firestore.
    * The EnrichMQTTData function receives this error.
2. **Modified Logic in IngestionService.processSingleMessage:**
    * Currently, if EnrichMQTTData returns an error (including ErrMetadataNotFound), processSingleMessage logs it, sends it to ErrorChan, and then stops processing that message (it's not published to the main "enriched messages" topic).
    * **New Logic:** When processSingleMessage catches an ErrMetadataNotFound specifically:
        * It will still log the event and potentially send info to ErrorChan for monitoring.
        * **Crucially**, instead of discarding the message, it will construct a new message. This message should contain:
            * The original DeviceEUI.
            * The RawPayload from the MQTT message.
            * The OriginalMQTTTime and LoRaWANReceivedAt timestamps.
            * An IngestionTimestamp.
            * Optionally, a field indicating the reason it's here (e.g., "error": "ErrMetadataNotFound").
        * This new message will then be published to a **separate, dedicated Google Cloud Pub/Sub topic** (e.g., unidentified-device-messages or device-metadata-missing-topic).
3. **New Pub/Sub Topic for Unidentified Devices:**
    * You'll need to define a new Pub/Sub topic in your Google Cloud project for these messages.
    * The IngestionService will need to be configured with the ID of this new topic (via an environment variable, similar to how the main enriched messages topic is configured).
4. **Publisher for the New Topic:**
    * The IngestionService will need a mechanism to publish to this new topic. You have a few options:
        * **Option A (Separate Publisher Instance):** Add a second MessagePublisher instance to your IngestionService struct (e.g., s.unidentifiedDevicePublisher), configured for this new topic. This is clean and keeps concerns separate.
        * **Option B (Modify MessagePublisher Interface \- More Complex):** Modify the MessagePublisher interface to accept a topic ID or a message type that dictates the destination. This adds more complexity to the publisher implementations. For an MVP, Option A is likely simpler.

**Benefits of this Strategy:**

* **No Data Loss:** The raw data from the device is captured and stored in the "unidentified device" Pub/Sub topic, even if it can't be immediately enriched with metadata.
* **Auditability:** You maintain a complete record of all messages received, including those from unknown or new devices.
* **Operational Alerts & Action:**
    * You can set up a separate subscription to this "unidentified device" topic.
    * A monitoring service or alerting mechanism can listen to this subscription to notify administrators when new, unrecognized devices are sending data. This prompts them to add the necessary metadata to Firestore.
* **Potential for Reprocessing:** Once the metadata for a device is added to Firestore, you could potentially have a separate utility or service that reads messages from the "unidentified device" topic and re-submits them through the enrichment process (or a modified version of it).
* **Keeps Main Pipeline Clean:** Your primary "enriched messages" topic remains clean, containing only data that has been successfully processed and associated with known devices and their metadata.

**Implementation Considerations:**

* **Configuration:** Add new environment variables for the "unidentified device" topic ID.
* **Service Modification:**
    * Update IngestionService to initialize and hold the publisher for the new topic.
    * Modify processSingleMessage to implement the routing logic: if ErrMetadataNotFound, publish to the "unidentified device" topic; otherwise, publish to the main enriched topic.
* **Error Handling:** Ensure that failures to publish to the "unidentified device" topic are also logged and handled appropriately (e.g., sent to ErrorChan).

This strategy effectively decouples the handling of known devices from unknown devices, ensuring data capture while maintaining the integrity of your main data pipeline.