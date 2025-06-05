### pass messages along

OK, in our next microservice we'll store data. 

But first we need to send data from our service. 
For our MVP we're using GoogleCloud so we want to use the PubSub service provided by Google.
we'll want to take messages from RawMessagesChan
and send them to a Topic that other microservices can subscribe to.
To keep things similar to what we have already do Topic configuration from an environment variable

We might use Kafka instead in the future so how hard is it to abstract the calls to allow us to choose?

### Response
[initial](1-pubsub-response.md)

### Test

our tests our out of date now (note we've not been doing TDD here... 
well maybe we'll get back to it but lets continue for now)

````aipromt
ok gemini, now we need to first update our tests, using the MessagePublisher

how about we mock MessagePublisher, using a chan, 
then we can reuse the test code from before along with the adjustments we put in to deal with shutdown draining 
the EnrichedMessagesChan and ErrorChan
````

The response is in
[tests](2-pubsub-response.md)

This give us a MockMessagePublisher but trys to push it into our integration test.
We wanted the Mock to work in our standard service_test and we'll do the integration test later

````aiprompt
OK i'll use the MockPublisher, but not in the Integration test

We do non-integration testing of the service first and we'll use the MockPublisher there.

Google provides a PubSub emulator so we want to use that for the integration test. 
With the Emulator and Mosquitto our integration test should be very close to our actual deployment target.

````

and its [response](3-pubsub-response.md)

