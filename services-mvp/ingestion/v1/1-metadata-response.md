Okay, that's a great progression for your MVP\! Using Firestore for metadata in the MVP is a practical choice, especially with its emulator for testing.

I'll first update the ingestion\_service\_with\_paho\_go artifact to include the new GoogleDeviceMetadataFetcher and its configuration. This will involve:

1. Defining a FirestoreFetcherConfig and a function to load it from environment variables.
2. Creating the GoogleDeviceMetadataFetcher struct and its New... constructor.
3. Implementing the fetcher function logic to query Firestore.

Then, we'll update the integration test to use this real fetcher with the Firestore emulator.

Here's the updated service code with the Firestore fetcher:

**Key Changes and Additions to the Service Code:**

1. **FirestoreFetcherConfig & LoadFirestoreFetcherConfigFromEnv**:
    * Manages configuration for connecting to Firestore (Project ID, target collection name).
    * Loads from environment variables: GCP\_PROJECT\_ID, FIRESTORE\_COLLECTION\_DEVICES, and optionally GCP\_FIRESTORE\_CREDENTIALS\_FILE.
2. **GoogleDeviceMetadataFetcher**:
    * Holds a Firestore client and the target collection name.
    * NewGoogleDeviceMetadataFetcher: Initializes the Firestore client. It will automatically use the Firestore emulator if the FIRESTORE\_EMULATOR\_HOST environment variable is set.
    * Fetch(deviceEUI string) (string, string, string, error): This is the method that implements the DeviceMetadataFetcher function signature.
        * It queries the specified Firestore collection for a document whose ID matches the deviceEUI.
        * It maps the document data to an internal struct to extract clientID, locationID, and deviceCategory.
        * It handles cases where the document is not found (returning ErrMetadataNotFound) or if data mapping fails.
        * It also includes a basic check for incomplete metadata.
    * Close(): To close the Firestore client when the service stops (though IngestionService doesn't directly manage the fetcher's lifecycle; the creator of the fetcher instance would be responsible for closing it).
3. **DeviceMetadataFetcher Function Type**:
    * The IngestionService and EnrichMQTTData function already expect a DeviceMetadataFetcher *function*.
    * When you use GoogleDeviceMetadataFetcher, you will pass its Fetch method as the DeviceMetadataFetcher argument to NewIngestionService. Example:

\<\!-- end list \--\>Go  
// In your main/setup:  
firestoreFetcher, err := NewGoogleDeviceMetadataFetcher(ctx, firestoreCfg, logger)  
// ... handle err ...  
// defer firestoreFetcher.Close() // Important\!

// Pass the Fetch method as the DeviceMetadataFetcher function  
ingestionSvc := NewIngestionService(firestoreFetcher.Fetch, publisher, logger, ...)

**Next Steps:**

1. **Add Firestore Library**: go get cloud.google.com/go/firestore
2. **Update ingestion\_integration\_test\_go**:
    * Add a setupFirestoreEmulator function using testcontainers-go.
    * In TestIngestionService\_Integration\_WithEmulator\_FullFlow:
        * Start the Firestore emulator.
        * Set FIRESTORE\_EMULATOR\_HOST and FIRESTORE\_COLLECTION\_DEVICES environment variables.
        * Before starting the IngestionService, use a temporary Firestore client (connected to the emulator) to **pre-populate test data** for the testDeviceEUIForEmulatorTest.
        * Instantiate GoogleDeviceMetadataFetcher, configure it to use the emulator, and pass its Fetch method to NewIngestionService instead of the mock fetcher.
        * The assertions will now verify that the metadata in the EnrichedMessage (pulled from Pub/Sub emulator) matches what was seeded into the Firestore emulator.

I'll proceed with updating the ingestion\_integration\_test\_go artifact in the next step to incorporate these changes.