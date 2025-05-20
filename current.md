## Current State

At present we have 3 microservices
1) ingestion from devices
2) storage of raw device messages in slow recall format for audit
3) storage of decoded device messages in google's BigQuery for analysis

### Ingestion from devices
This is where we started to get a feel for our interactions with the agent.

We prime the agent with our aims and get down to seeing it generate code for us.

This time we fully document all our prompts and store the responses sent by Gemini

We step through the problem, gradually setting out the problem and our preferred approaches 
while asking Gemini to sanity check our choices.

The aim was not to over prompt as we wanted to see what Gemini would suggest by itself and
the results were promising with Gemini making a lot of choices that fitted in with our preferred
library and implementation options.

### Long term store
Buoyed up by the responses in ingestion we attempted to do more at once in our
prompts to Gemini - maybe I should return to this style later just to see if it's 
viable for quick and dirty solutions - the results were underwhelming and I returned to 
a more structured approach.

The long-term storage options were interesting even the ones I didn't chose to proceed with.
As I had planned to use BigQuery for analysis the 90 day automatic transition to long term storage is 
intriguing - we could store analysis by quarter or something similar - 
we'll be creating data aggregations anyway so this will mean we can reduce costs without overthinking what to 
do with older data.

### Storing the device data for analysis

Getting pretty used to the Gemini responses at this stage and I can quickly follow up prompts 
that haven't quiet gone to my liking and correct the code without too much effort.

with this up and running we can actually start doing some analysis on data from the devices

Bigquery allows us to import data in something as simple as a googlesheet for a quick look, 
looker studio for some deeper analysis and anything we learn can then easily be turned into it's own
microservice so we can feed data to custom frontends.