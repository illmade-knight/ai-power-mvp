During cloud end-to-end testing we found a problem with device lookup in the enrich
service (this may be due partially to not running everything within the google project)

Our long term goal was to use redis in any case so let's implement it now to get rid of that bottleneck

## Faster device lookup

Our device lookup from firestore is too slow

We want to add a redis cache layer

Create a struct which has a func implementing DeviceMetadataFetcher
that utilizes a Redis store. 
This struct should have access to the original firestore implementation 
so that if a device is not found in redis the cache will check firestore 
and add the device to redis.

