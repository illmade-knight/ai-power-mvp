## v1

our first versions of load testing

our [cloudtest](endtoendcloud) uncovered some potential problems 
with mqtt processing losing messages while looking up device meta data

because of this we did not move to fully testing the orchestrate package 
which would have looked at using all our microservices together

instead we'll try v2 where we break the mqtt and meta data adding into 2 different services
(on the advise of Gemini...)