## Join our services

We have 3 interconnected microservices with unit and integration tests.

Our next steps will be to deploy the services to a test version of our eventual production environment.

We'll run this in an exact copy of the production environment but in a separate Google Cloud project.

### Microservice manager

One of the common pitfalls of microservices is managing them - ensuring topics and subscriptions are
correctly configured for each service and that the messages passed between them conform to the same schemas.

So before we deploy everything we want to create a separate service that will manage these connections.

Initially we want it to work from a single configuration file that keeps track of the services and allows us
to create all the topics and subscriptions, google bigquery tables and storage buckets.
For the test environment we also want it to be able to tear down those resources 
(for production we either want this to have more security constraints and not be easy to do).

What do you think of this idea and how would you begin to implement it?

[ressponse](response.md)

this turned out to be one of the more challenging uses of Gemini - 
I thought I had a solution for the problems encountered in the xdevice Integration test but 
the adjustments to how I approached the context window didn't work and instead uncovered more 
problems with how to 'talk' to the agent and build understanding