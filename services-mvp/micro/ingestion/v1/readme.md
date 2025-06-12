## Initial prompt for the service

We'll explain what we're trying to achieve to the AI and look at its response

## The ingestion service

Lets start with some basics.

````aiprompt
We will use GoLang as our microservice language.
Initially, we'll to GoogleCloud and using their pubsub service.
We're using CloudRun to deploy our microservice.

We do something like test-driven-development but using Gemini to generate our tests.

Let's explain our process for this.
Instead of going straight ahead and connecting to an MQTT broker we'll break things down first.

The MQTT service sends us JSON with LoRA data fields in addition to the device payload which is our primary interest.
The JSON also has a device identification structure.

If you know of a good GoLang library which accesses MQTT through GoLang channels use this as a framework for building
the test harness.

So to kick off our TDD create a sample sample struct for interpreting messages from an imaginary mqtt service.
Then advise on some first steps for our Test Driven Development cycle.
````

### AI Response

we've added the [response](response.md) 

we've used [Paho](https://github.com/eclipse-paho) before so it was nice to see Gemini suggest that at the offset

### Follow up

our next prompt didn't really do much:

````aiprompt
## Using PAHO

ok we've used paho before so let's go with that.

but for testing we want to keep the actual Paho implementation out of things for the moment.

So in the next tests we want a struct with a channel Paho (or something else) can write to and then a func that reacts when messages arrive at the channel.

We'll keep track of the messages arriving at least during initial testing to ensure all messages are handled.

So our ProcessMessage tests should ensure valid incoming messages are correctly enhanced and invalid messages rejected and logged

we'll use zerolog for our logging operation (though in general we like using core packages if they are sufficient)

````

so we clarified what we wanted. 

we'll start adding to the service breaking it down into each section

1) [paho](a-paho.md)
2) [pubsub](b-pubsub.md)
3) [metadata](c-metadata.md)

we've now integrated all the major parts, with normal tests and an integration test covering the service

That's pretty good going I think.

Again we can come back and do a more thorough review but we're on our MVP route and now lets
go and first create a store for these enriched messages.

## But first
I've identified a problem with the approach so far and I want to check it with the AI.
I have a solution in mind but want to see what the AI suggests too.

````aipromt
OK, everything is working now but I've just realized a flaw in our thinking

We want to have the ability to record device data for audit but at the moment if we don't have a device in 
DeviceMetadataFetcher we create an error, now that error may be just a data input error on our part, 
so we want to make sure these messages are not lost.

suggest a strategy for dealing with these
````

the [answer](1-problem-response.md) is in line with what I was thinking 
so lets ask it to go ahead with option A for the MVP

there was a small issue I didn't like so I asked it to change the MessagePublisher:

````ai response
Okay, I understand. You'd prefer the MessagePublisher interface to have more specific methods for publishing EnrichedMessage and UnidentifiedDeviceMessage types directly, rather than a generic method that takes []byte. This makes the interface more type-safe and clearly defines the intent.

I will update the MessagePublisher interface and its GooglePubSubPublisher implementation in the Canvas accordingly. The IngestionService.processSingleMessage method will also be updated to use these new, more specific publishing methods.
````




