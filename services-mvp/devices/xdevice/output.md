### bigquery

we now want to send data from our decoded data to bigquery

our Xdevice provides its own set of attributes

we may have devices in future that do a similar job but provide more or less data.

so can we publish all this data to bigquery but have optional fields 

if we can provide the necessary code in new files.
again we want to start with unit testing and then moving onto an integration test later 
using our docker image for emulating google services

if not provide some options.

### OK I want the response again...

OK straight away I have a question I want the response from, not just the code

````aiprompt
That's good so far but add in the metadata we've passed from the previous microservice, 
we'll be using that in our queries to BigQuery later. 
Before with BigQuery I add to create a Scheme to transfer data is this no longer necessary?
````

and I'll place the response [here](1.md)

I've used the bigquery API before and I like that control 

we'll run with the given response for the moment and come back to it if things change

### More response

Again as I look at this - the AI has tied it to BigQuery very directly
This makes mocking hard and also splits from how we've done stuff before.

Lets
````aiprompt
OK - we can abstract this and make it better not more complex - for our types we don't need BigQuery in the name, 
there's nothiing that ties them to BigQuery rows - 
so lets call them something more generic - 
MeterReading or similar 
we can then have something similar to the other microservice where we have a mockable Insert func 
which we'll tie to BigQuery only in the implementation
````

### A few more iterations
we did a few small iterations to clean things up a bit - all relatively painless

## The Service

let's prompt now to put things together

````aiprompt
before integration we want to create a service in a new file to consume the pubsub messages 
and store them using our DecodedDataInserter. 
This should all look the same as in our previous microservice that stored the raw messages
````