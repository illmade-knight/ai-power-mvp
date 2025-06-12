## Orchestrate an end to end test of load

Our system has an ingestion microservice which takes the mqtt messages, 

checks the devices to see if they exist in our firestore 
(actually the microservice only needs an interface that allows the Device retrieval)  
and passes messages from recognized devices to one Topic (enriched with the device metadata) 
amd unrecognized devices to a separate Topic

for our load test we have a simple verify app running which checks messages arrive at subscriptions on 
these topics.

I'll include some files from the ingestion/connectors package in the ingestion microservice to show how the
mqtt messages are processed and the basic verify.go app

We want to run an end to end load test using our messanger package we just looked at 
to generate the mqtt messages.

Advise on improving verify.go with local tests and then on how to orchestrate a full end to end test

