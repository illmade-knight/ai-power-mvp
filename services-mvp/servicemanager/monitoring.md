## Accessing and monitoring

We want to monitor and track how the services and used and the quantity of messages passing between our microservices.

Eventually we'll want to be able to set alarms if message numbers exceed given limits: either lower or higher than
we expect.

Lets start simple though. At the moment we have a yaml configuration - 
this is pretty static but it will do for the moment
When one of our services starts we want it to ensure that it is accessing the correct topics, subscriptions 
and other services.

Our services will all run inside a single, secure Google Cloud Project so we can run a REST service from here without 
worrying about AIM for serving this information

Analyse this approach and suggest first steps for building code within our servicemanager package

`````airesponse
Okay, that's a very logical and valuable next step for your servicemanager! Moving from just provisioning resources to also providing a way for services to verify their configurations and lay the groundwork for monitoring is a smart move.

Your approach of:

Using the existing YAML configuration as the source of truth.
Having services query a REST endpoint (provided by servicemanager) at startup to fetch their expected resource configurations (topics, subscriptions, buckets, etc.).
Running this REST service within your secure Google Cloud Project.
is sound. It centralizes configuration knowledge, allows services to self-validate, and can indeed be the foundation for more advanced monitoring and alerting later on.

I'll prepare a document outlining an analysis of this approach and the first steps for implementing this new "Configuration Access Service" within your existing servicemanager package. This will include how to leverage your current TopLevelConfig and the new producer_service, consumer_service, and accessing_services fields.
Service Manager: Configuration Access Service Plan
May 23, 9:11 AM

This plan provides a solid starting point for building the configuration access service within your servicemanager. The key is to leverage the existing configuration and extend the servicemanager's responsibilities.
`````