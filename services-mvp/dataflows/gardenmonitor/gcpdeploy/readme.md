## Create resources Deploy services 

### **services.yaml**

This file is the single source of truth for all services and resources. It now defines both a test and a production environment.

````yaml
# The single source of truth for all services and resources in the system.
# Used by the 'provision.go' script to manage cloud infrastructure.

defaultProjectID: "your-prod-gcp-project-id" # CHANGE THIS
defaultLocation: "US"

environments:
  test:
    projectID: "your-test-gcp-project-id" # CHANGE THIS
  production:
    projectID: "your-prod-gcp-project-id" # CHANGE THIS

services:
  - name: "ingestion-service"
    description: "Receives data from MQTT and publishes to Pub/Sub."
  - name: "analysis-service"
    description: "Consumes from Pub/Sub and inserts into BigQuery."

dataflows:
  - name: "test-mqtt-to-bigquery"
    description: "Dataflow for the test environment."
    services:
      - "ingestion-service"
      - "analysis-service"
    # The 'ephemeral' lifecycle could be used here if you want to automatically
    # clean up test resources, similar to the integration test.
    # For now, they are permanent.

  - name: "prod-mqtt-to-bigquery"
    description: "The main production dataflow pipeline."
    services:
      - "ingestion-service"
      - "analysis-service"
    # No 'lifecycle' section means production resources are permanent.

resources:
  # --- Test Resources ---
  pubSubTopics:
    - name: "test-device-data"
      producerService: "ingestion-service"
  pubSubSubscriptions:
    - name: "test-analysis-service-sub"
      topic: "test-device-data"
      consumerService: "analysis-service"
  bigQueryDatasets:
    - name: "test_device_analytics"
      description: "Dataset for storing test telemetry."
  bigQueryTables:
    - name: "test_monitor_payloads"
      dataset: "test_device_analytics"
      accessingServices: ["analysis-service"]
      schemaSourceType: "go_struct"
      schemaSourceIdentifier: "github.com/illmade-knight/ai-power-mpv/pkg/types.GardenMonitorPayload"
````

````yaml
  # --- Production Resources ---
  pubSubTopics:
    - name: "prod-device-data"
      producerService: "ingestion-service"
  pubSubSubscriptions:
    - name: "prod-analysis-service-sub"
      topic: "prod-device-data"
      consumerService: "analysis-service"
  bigQueryDatasets:
    - name: "prod_device_analytics"
      description: "Dataset for storing processed device telemetry."
  bigQueryTables:
    - name: "prod_monitor_payloads"
      dataset: "prod_device_analytics"
      accessingServices: ["analysis-service"]
      schemaSourceType: "go_struct"
      schemaSourceIdentifier: "github.com/illmade-knight/ai-power-mpv/pkg/types.GardenMonitorPayload"

````

### **Further Steps: Staged Deployment (Test then Production)**

Here are the next steps to get your services running in Google Cloud, starting with the test environment.

#### **Prerequisites**

1. **Google Cloud Projects**: Have two GCP projects created: one for test and one for production.
2. **Authentication**: Ensure your local environment is authenticated to GCP: gcloud auth application-default login.
3. **Enable APIs**: Make sure the "Pub/Sub", "BigQuery", and "Cloud Build" APIs are enabled for **both** projects.
4. **Go Environment**: Have a working Go environment.

### **Step 1: Deploy to the Test Environment**

First, provision and deploy everything to your test project.

1. **Update services.yaml**: Open services.yaml and set the correct project IDs for your test and production environments.
2. **Provision Test Infrastructure**: Run the provision.go script, specifying the test environment and dataflow.  
   go run deploy/provision.go \\  
   \-services-def=services.yaml \\  
   \-env=test \\  
   \-dataflow=test-mqtt-to-bigquery

   This will create the resources prefixed with test- in your test project.
3. **Configure Services for Test**:
    * In cmd/ingestion/config.yaml, update the project\_id and publisher.topic\_id to point to the **test** project and test-device-data topic.
    * In cmd/bigquery/config.yaml, update the project\_id and all resource IDs (subscription\_id, dataset\_id, table\_id) to their **test** versions (e.g., test-analysis-service-sub).
4. Deploy Services to Test Project:  
   Use gcloud run deploy to deploy each service from its source directory. Make sure your gcloud is configured to point to your test project (gcloud config set project your-test-gcp-project-id).
    * **Deploy Ingestion Service to Test**:  
      \# From the cmd/ingestion directory  
      gcloud run deploy ingestion-service-test \\  
      \--source . \--platform managed \--region your-region \--allow-unauthenticated

    * **Deploy Analysis Service to Test**:  
      \# From the cmd/bigquery directory  
      gcloud run deploy analysis-service-test \\  
      \--source . \--platform managed \--region your-region \--allow-unauthenticated

At this point, you can thoroughly test the entire pipeline in your isolated test environment.

### **Step 2: Deploy to Production**

Once you have verified that everything works correctly in the test environment, you can proceed to deploy to production.

1. **Provision Production Infrastructure**: Run the provision.go script again, but this time targeting the production environment.  
   go run deploy/provision.go \\  
   \-services-def=services.yaml \\  
   \-env=production \\  
   \-dataflow=prod-mqtt-to-bigquery

2. **Configure Services for Production**:
    * Change the config.yaml files for both services to use the **production** project ID and resource names (e.g., prod-device-data).
3. Deploy Services to Production Project:  
   Change your gcloud configuration to point to your production project (gcloud config set project your-prod-gcp-project-id) and run the deployment commands again.
    * **Deploy Ingestion Service to Production**:  
      \# From the cmd/ingestion directory  
      gcloud run deploy ingestion-service-prod \\  
      \--source . \--platform managed \--region your-region \--allow-unauthenticated

    * **Deploy Analysis Service to Production**:  
      \# From the cmd/bigquery directory  
      gcloud run deploy analysis-service-prod \\  
      \--source . \--platform managed \--region your-region \--allow-unauthenticated

Your full data pipeline is now running in production. This staged approach ensures a much safer and more reliable deployment process.