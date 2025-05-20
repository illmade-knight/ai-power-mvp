## A New Microservice

lets create a new microservice in a package called xdevice

## Specific Decoding

Now we want to decode the raw payload delivered by a Device.

Different devices have different raw payloads so each device will have its own associated microservice:

Here we want to create a microservice for a specific device - let's call the device 'Xdevice' for the moment.

## Getting messages
Our ingestion service publishes identified messages to a Topic,

We can either add device type in our metadata stage our have a dedicated topic for each device.

Whatever option is used the subscription should accept the correct messages for this decoder service.

### Start by testing a decoder

Incoming messages will be of the type EnrichedMessage (after decoding) and the field we want to decode is RawPayload.

Lets create a PayloadDecoder func which should decode to the following

IoT device payloads are often raw bytes - the structure must be known in advance

e.g

20 bytes organized as follows:
the first 4 bytes are a BigEndian encoded UID string
the next 4 bytes are a float for the Reading
the next 4 bytes are a float for the AverageCurrent
the next 4 bytes are a float for the MaxCurrent
the next 4 bytes are a float for the MaxVoltage
the next 4 bytes are a float for the AverageVoltage

decode this to a go struct

### refine

just a slight adjustment the UID is a string encoded to 4 bytes

so decode back to the string representation

### we'll drop non-critical responses

these prompts are working OK and from now on we'll only highlight interesting
or problematic responses - In general the responses are now represented by the actual code.

### Output

lets move on to what we do with the decoded data in [output]

