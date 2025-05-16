Okay, I will update the integration test in the Canvas to incorporate the GoogleDeviceMetadataFetcher and use the Firestore emulator.

This will involve adding a setup function for the Firestore emulator using testcontainers-go, seeding test data into the emulator, and then configuring the IngestionService to use the GoogleDeviceMetadataFetcher connected to this emulator. The rest of the test flow (Mosquitto for MQTT input, Pub/Sub emulator for output) will remain.

Here's the updated integration test: