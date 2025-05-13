## The ingestion service

Lets start with some basics.

We will use GoLang as our microservice language.
We will be deploying initially to GoogleCloud's and using their pubsub service.
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


