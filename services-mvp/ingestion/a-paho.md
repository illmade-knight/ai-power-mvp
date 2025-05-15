## Using PAHO

no I think you misunderstood. I now want to start testing an implementation of the service. 
So we'll have a struct IngestionService which has a chan for messages from the databroker. 
In future we'll add to this using Paho but until we wire that in we want to test IngesstionService 
ourselves by adding directly to the chan.

the [response](paho-response.md) does mostly what we want - it starts a little weirdly,
saying it was sorry for jumping ahead to a paho implementation, even though it hadn't done anything with paho, 
just added some errMessages to the original files.

from the response we've started building up the service

our service tests now let us start doing things like

````go
rawMsgBytes := makeSampleRawMQTTMsgBytes("DEV002", "TestData2", time.Now())
service.RawMessagesChan <- rawMsgBytes
````

now when we add in paho its receiveMessage can just add to the chan in a similar way to our existing tests

### Actually use paho

let's prompt it to use a real Paho client here

````aiprompt
ok, let's add in creating a real paho client.

we want to get credentials securely from environment variables so we can connect securely over TLS

when we have a paho client that works we'd like to think about an integration test 
to see if things are going down the right path in a more 'real world' environment.
````

and the response goes into [paho-impl](paho-impl-response.md)

this is nice, the change to the service struct is minimal

````go
mqttClientConfig *MQTTClientConfig
pahoClient       mqtt.Client
````

we get a Config and a Client, the channels stay the same for now so the existing tests still run

personally I might have separated the paho stuff but the presented solution is fine and keeps things looking the same.

### Remember this is a MVP
I'm deliberately not analysing things too much - we want our MVP out the door.

We'll do a break down of what we like and don't like afterwards.

### Integration

Ok this looks good - there are minimal changes to the existing service.
We've got a docker image of eclipse mosquito

recommend how we structure the running of an integration test using mosquito to create the messages

