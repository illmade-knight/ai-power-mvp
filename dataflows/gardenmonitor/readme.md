## Microservices

Our dataflows combine a number of microservices.

Each microservice is a thin wrapper around reusable code now stored in our
[go-iot](https://github.com/illmade-knight/go-iot) repository refactored from our AI experiments.

[Deployment](https://github.com/illmade-knight/ai-power-mvp/tree/main/dataflows/gardenmonitor/gcpdeploy) 
is configured using [servicemanager](https://github.com/illmade-knight/go-iot/tree/main/pkg/servicemanager)
which creates google cloud resources, pubsub, bigquery, buckets and also generates deployment scripts for
placing services in cloud run with the necessary environment variables