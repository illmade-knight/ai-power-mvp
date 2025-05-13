## Using PAHO
ok we've used paho before so let's go with that.

but for testing we want to keep the actual Paho implementation out of things for the moment.

So in the next tests we want a struct with a channel Paho (or something else) can write to and
then a func that reacts when messages arrive at the channel.

We'll keep track of the messages arriving at least during initial testing to ensure all messages are handled.

So our ProcessMessage tests should ensure valid incoming messages are correctly enhanced and invalid messages rejected and logged

we'll use zerolog for our logging operation (though in general we like using core packages if they are sufficient)

### Intermission

That didn't really do anything so I added:

no I think you misunderstood. I now want to start testing an implementation of the service. 
So we'll have a struct IngestionService which has a chan for messages from the databroker. In future we'll add to this using Paho but until we wire that in we want to test IngesstionService ourselves by adding directly to the chan