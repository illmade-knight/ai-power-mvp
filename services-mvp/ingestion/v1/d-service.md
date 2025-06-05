## Lets get round to deploying our services

if at our ingestionservice we do not yet have the service main 
that we'll run when we deploy it.

for google cloud run it is useful to run a http server from main 
so we can respond to health checks 

we also want it to connect to the servicemanager and confirm it's using the correct resources so we'll need a
new environment variable for the servicemanager connection.

for now we'll let it confirm it's own mqtt settings as those are external 
but in future we may add them to the central service configuration. 

we prefer having main and the actual server in separate files 

evaluate this and create the service file if it is OK