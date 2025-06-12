## microservice lib

This is a monolithic lib for microservices

### Maturing

As a microservice in our services-mvp matures we extract elements that
we want to reuse.

### [Service Manager](servicemanager)

We've moved the core service manager code here - 
this enables easy local setup of gcloud resources prior to microservice deployment

### Helpers

Tools such as [loadgen](helpers/loadgen) for generating test load for microservices go here.

### Device Specific

[extractors](extractors) currently has device specific code

Any device specific code will eventually be purged from pkg 