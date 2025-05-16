we're going to move on to a second microservice which consumes data generated from here and added to pubsub:

## This our second microservice

It takes the devices raw data and stores it.
The storage does not need to be quickly accessible: Raw storage is only used for audit and recovery.
Give some options for how to achieve this.
For actual implementation stay within google cloud's offerings but again abstract things so we can move if necessary 


We'll decide on a storage mechanism first, unit test it and mock it

Then we'll follow the same pattern as our ingestion service: getting messages off pubsub etc...

### Response
we're just going to list responses from now on

[1](1.md)

that's given us a few options but let's just explore things a bit.

### Follow up 

That's interesting - I was planing to us BigQuery for the decoded device messages 
but it does seem overkill for this.

For retrieval we'd probably be interested in specific locations rather than individual devices.

How does GCS coldline deal with append type operations? 
i.e we'd want to retrieve all messages for a specific location on a given day in a single blob.

[2](2.md)

### Choice

OK let's go with strategy 1 with simple batching 

you suggested the naming strategy:

gs://<your-bucket>/raw-audit-logs/daily-aggregates/<locationID>/<year>/<month>/<day>/

but maybe we should adopt

gs://<your-bucket>/raw-audit-logs/daily-aggregates/<year>/<month>/<day>/<locationID>/

That allows us to get *all* messages in a date range and separate by locationID if necessary

create initial code with unit tests and mocking to validate this

[3](3.md)

### Dead letter

````go
// Archive adds data to an in-memory batch. If the batch reaches the threshold,
// it's flushed to GCS.
func (a *GCSDataArchiver) Archive(ctx context.Context, data RawDeviceData) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Determine batch key: year/month/day/locationID
	// Assuming data.MessageTimestamp is the primary timestamp for batching by date.
	// If LocationID is not present in RawDeviceData, this strategy needs adjustment
	// or LocationID needs to be added to RawDeviceData.
	// For now, let's assume RawDeviceData might not have LocationID, so we use DeviceEUI as a proxy
	// or a default location if not available. This needs clarification based on RawDeviceData content.
	// For this example, we'll use DeviceEUI if LocationID is missing. A real implementation
	// would need a clear way to get LocationID.
	// Let's assume RawDeviceData has a LocationID field for this naming convention.
	// If not, the key generation needs to change.
	// For now, let's assume RawDeviceData has a LocationID field, or we use a placeholder.
	
	// For the naming convention: <year>/<month>/<day>/<locationID>
	// We need LocationID in RawDeviceData or passed somehow.
	// Let's add LocationID to RawDeviceData for this to work as requested.
	// If RawDeviceData struct cannot be changed, then the batch key or object path needs
	// a different source for location.

	// Assuming RawDeviceData now has a LocationID field (as per naming strategy discussion)
	// If LocationID is not available, this will be problematic.
	// The user query implies LocationID is part of the path.
	// Let's assume it's available in `data.LocationID` (needs to be added to struct if not there)

	// Let's assume `data` has a field `LocationID string`
	// If not, this part of the key is problematic.
	// For the sake of this example, let's say `RawDeviceData` is extended:
	// type RawDeviceData struct {
	//     ...
	//     LocationID         string    `json:"location_id"`
	//     ...
	// }
	// If LocationID is not directly in RawDeviceData, the caller of Archive might need to supply it,
	// or it's derived differently. For now, we'll proceed as if it's there.
	// The user's naming implies locationID is known at archival time.

	// Let's assume a LocationID field exists in RawDeviceData for the path.
	// If not, we'll use a placeholder or require it to be added.
	// For this implementation, I'll assume `data.DeviceEUI` can act as a stand-in if LocationID is missing,
	// or that `RawDeviceData` will be updated to include `LocationID`.
	// The path requires locationID.
	// Let's use data.DeviceEUI as locationID if no explicit locationID is in RawDeviceData.
	// This is a placeholder decision.
	
	// Let's assume RawDeviceData has been updated to include LocationID.
	// If not, the key must be formed differently.
	// For the example, I will use a placeholder "unknown_location" if data.LocationID is empty.
	// This should be clarified with the actual RawDeviceData struct.
	
	// For the path: <year>/<month>/<day>/<locationID>
	// The user's query implies locationID is available.
	// Let's assume `data.LocationID` is a field in `RawDeviceData`.
	// If not, this needs to be passed or derived.
	
	// For the sake of the example, let's assume RawDeviceData has LocationID.
	// If not, the object path generation will fail or be incorrect.
	// The user's naming convention is: gs://<your-bucket>/raw-audit-logs/daily-aggregates/<year>/<month>/<day>/<locationID>/
	// This means LocationID must be available when Archive is called.
````


the response code has a large section on the batch key, 
actually this shows our choice of path leaving locationID to the end was good:

we can use a default deadletter path <year>/<month>/<day>/deadletter for all messages missing their metadata 
(see the ingestion service) so use that strategy for dealing with the exceptions

### correction
ah, sorry I've just realized the RawDeviceData you've been thinking about isn't the EnrichedMessage 
that our previous microservice was sending to pubsub. 

let's fix that and we can improve it to accept UnidentifiedDeviceMessage as well

Looking at EnrichedMessage and UnidentifiedDeviceMessage lets deserialize a type that could be either -
if it has a ProcessingError and/or no MetaData then we send it to deadletter

### testing
I asked it for tests on the gcsdataarchiver

I need to prompt again though

````aiprompt
Ok this is one giant test and it fails at require.Len(t, flushedUnparseableStopBatch, 1)

can you make it more go like with a table test with multiple test cases?
````

### rushing?
the interactions with the ingestion service worked really well

perhaps because of that I proceeded more confidently here

result? we got a lot of errors 

let's think a bit before going on

### the service

ok now create a new go file for the service.

the service will connect to two subscriptions - both from ingestion service topics
one passes EnrichedMessage and the other UnidentifiedDeviceMessage

our DataArchiver can handle both message type so we can pass either to it.

again we're using google pubsub but want to abstract to enable Kafka use in future

write the code and test it first with mocks