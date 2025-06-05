Okay, that's an excellent plan for making your integration tests even more representative of your Google Cloud deployment environment\! Using the Pub/Sub emulator alongside the Mosquitto container will give you high confidence.

I'll update the ingestion\_integration\_test\_go artifact to:

1. Remove the MockMessagePublisher.
2. Add a function to set up the Google Cloud Pub/Sub emulator using testcontainers-go.
3. Modify TestIngestionService\_Integration\_ReceiveAndProcessMessage to:
    * Start both Mosquitto and the Pub/Sub emulator.
    * Ensure the test Pub/Sub topic is created on the emulator.
    * Configure the IngestionService to use a real GooglePubSubPublisher that connects to the emulator (via the PUBSUB\_EMULATOR\_HOST environment variable).
    * After the IngestionService processes a message (received from Mosquitto), the test will create a separate Pub/Sub subscriber client to pull the message from the emulator's topic and verify its content.

Action Required by You (if not already done for Pub/Sub emulator):  
Ensure you have a Google Cloud SDK image available that includes the Pub/Sub emulator, or use the dedicated gcr.io/google.com/cloudsdktool/cloud-sdk:emulators image (or similar).  
Here's the updated integration test:

