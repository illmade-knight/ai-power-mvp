## End to end test

before running a load test we want to see if the parts work end to end

1) generate messages with our messenger code

2) using the microservice to process the messages

3) use the verifier to check the messages from the microservice

### use the existing connectors integration test

we'd like to run the microservice in process like you suggested

can we just reuse the logic in the integration test I showed you

### setup and tear down

we want to use the firestore emulator and a pubsub emulator

with the firestore emulator we'd like to add test device that can be used both 
by the generator and the ingestion microservice

we'd like to send some messages from unrecognised devices to test the other topic path