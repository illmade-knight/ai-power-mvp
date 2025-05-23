## Refactor structure

we'll refactor the code now we're adding the apiserver

within our top level package servicemanager we've moved 

bigquery.go, config.go, gcs.go and pubsub.go into a directory called /initialization

we're keeping our api_types.go in a separate file

