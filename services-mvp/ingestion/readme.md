## End to end

following our first end to end test we found a bottleneck around processing mqtt and enriching it

the device lookup seemed to be the only real piece of work being done that would affect things so gemini
advised handling mqtt in a simpler - dump to pubsub - microservice

the original combined [service](v1/connectors/service.go) is in the [v1](v1) directory

we're looking at the split into 2 services in [v2](v2)