## Dataflows

create dataflows for specific devices with storage, analysis etc.


### Demonstration Dataflow
[garden monitor](gardenmonitor) demonstrates the use of data from an mqtt broker

you can see how the dataflows are put together in the [end to end tests](gardenmonitor/teste2e)

2 dataflows are demonstrated - to google bigQuery and to longer term storage

A 'real' dataflow can combine both: BigQuery and GCS can be configured to access separate subscriptions
running off the same topic published to by the mqqt ingestion server.