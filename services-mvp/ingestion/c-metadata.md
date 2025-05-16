## Beef out retrieving device metadata

we've just been using a very simple struct for retrieving metadata for the devices

````aiprompt
we've been using a simple DeviceMetadataFetcher in our tests.

For our MVP we'll get these values from GoogleCloud's database Firestore (available for GoogleCloud projects).

In production we'll use a fast cache like Redis but for the MVP we'll connect directly to Firestore 
Firestore also gives us a simple web interface which makes it nice for adding low volumes of devices initially.

Create a GoogleDeviceMetadataFetcher based off GooglePubSubPublisher and using Firestore and add that into integration tests

We already have an emulator for GoogleCloud so we can use that in the integration test
````

as usual we [record](1-metadata-response.md) the AI's response

now we ask it to update the integraion test and again note its [response](2-metadata-response.md)
(which is short this time)