## Initial prompt for the service

We'll explain what we're trying to achieve to the AI and look at its response

## The ingestion service

Lets start with some basics.

We will use GoLang as our microservice language.
We will be deploying initially to GoogleCloud and using their pubsub service.
We'll use CloudRun to deploy our microservice.

We're going to do something like test-driven-development but using Gemini to generate create our tests.

Let's explain our process for this.
Instead of going straight ahead and connecting to an MQTT broker we'll break things down first.

The MQTT service sends us JSON with LoRA data fields in addition to the device payload which is our primary interest.
The JSON also has a device identification structure.

If you know of a good GoLang library which accesses MQTT through GoLang channels use this as a framework for building
the test harness.

So to kick off our TDD create a sample sample struct for interpreting messages from an imaginary mqtt service.
Then advise on some first steps for our Test Driven Development cycle.

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

so we clarified what we wanted. Let's follow the next section in [paho](paho.md)


