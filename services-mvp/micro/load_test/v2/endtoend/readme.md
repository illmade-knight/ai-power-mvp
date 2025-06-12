## new service

originally we had a end to end test for a microservice accepting mqtt messages and passing them 
to pubsub topics

during testing we identified potential scaling problems with the mqtt client and adding device metadata
in the same step - so we have split our ingestion service into 2 separate microservices 

the first microservice passes messages from the mqtt client with minimal processing to a pubsub topic

the second microservice reads from the topic and adds metadata, if available, passing the original message to 
another topic. Unidentified messages are passed to a separate topic.

our end to end test uses a message generator to simulate messages and a verifier to read from 
the 2nd microservices topics

### existing test
I've provided the original end to end test and the 2 microservice entry files - can you adapt this test to the
new microservice architecture - replacing the original microservice with the 2 new services that replace it