## Refactor

OK gemini seemed to run into a brick wall refactoring this to something usable 

Lets rewrite it, keeping the *core* functions but hopefully by breaking it down
we can get Gemini to continue the development

Let's reprompt first, with a clean Gemini session

### Restart

We have an mqtt microservice setup we want to load test so we're
writing a simple app to generate temporary devices and send mqtt messages from them

The devices we use are stored in a google firestore database.

For the load test - we can either use a real firestore instance or if we run locally 
we use a firestore emulator running in docker and seed the emulator with devices
(the devices need to be stored as they are used later by other microservices running in the system)

For the mqtt broker we use mosquitto running in a docker image

The load test runs from a golang main and it takes a default yaml config from the local system,
we can override config options with command line arguments.

The main creates an app context and a test context and duration. 
For each device we send messages with a given interval to a publisher and record how many messages are sent.

Evaluate the code given and then create initial unit test which we'll back up afterwards with an integration test