## Lets MVP this

Thanks Gemini, we're pretty familiar with microservices so for us it's more simple to start with them 
than transition to microservices later.

### The device
We've picked our initial device which operates over LoRA. 
We register our devices with a data broker and get a MQTT stream from the data broker.

### The ingestion service
We want an ingestion service that takes data securely from the MQTT while identifying the source device.
Initially we just want to take the raw payload from the device and attach device meta data to it

The meta data includes
1) the client the device belongs to 
2) the location of the device
3) a category for the device (HVAC, lighting etc)

The raw payload and device metadata are then sent to a topic to be consumed by a decoder.

This breakup into very small working units is typical of our approach. In future we can add services that look at the LoRA
data to analyse connection, up time, signal strength etc without touching our decoder, storage services.

Sanity check this approach for us and then we'll move on to implement the service