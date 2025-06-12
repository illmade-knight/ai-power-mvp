### **services.yaml**

This file is the single source of truth for all services and resources. It defines environments, service-specific metadata (like health checks and MQTT configuration), and source code paths.

````yaml
# The single source of truth for all services and resources in the system.
# This version uses snake_case to match the Go struct tags in types.go.

default_project_id: "gemini-power-test"
default_location: "EU" # For multi-region resources like GCS/BigQuery
default_region: "europe-west1" # Specific region for Cloud Run services

environments:
   test:
      project_id: "gemini-power-test"
      default_region: "europe-west1" # Can override the top-level default
   production:
      project_id: "your-prod-gcp-project-id" # CHANGE THIS
      default_region: "us-central1"

services:
   - name: "ingestion-service"
     description: "Receives data from MQTT and publishes to Pub/Sub."
     service_account: "ingestion-sa@your-gcp-project.iam.gserviceaccount.com"
     source_path: "cmd/ingestion" # Path from project root
     health_check:
        port: 8080
        path: "/healthz"
     metadata:
        # Service-specific configuration that will become environment variables
        broker_url: "tcp://broker.emqx.io:1883"
        topic: "garden_monitor"
        client_id_prefix: "garden_broker"

   - name: "analysis-service"
     description: "Consumes from Pub/Sub and inserts into BigQuery."
     service_account: "analysis-sa@your-gcp-project.iam.gserviceaccount.com"
     source_path: "cmd/bigquery" # Path from project root
     health_check:
        port: 8080
        path: "/healthz"

dataflows:
   - name: "test-mqtt-to-bigquery"
     description: "Dataflow for the test environment."
     services:
        - "ingestion-service"
        - "analysis-service"
     lifecycle:
        strategy: "permanent"

   - name: "prod-mqtt-to-bigquery"
     description: "The main production dataflow pipeline."
     services:
        - "ingestion-service"
        - "analysis-service"

resources:
   # --- Test Resources ---
   pubsub_topics:
      - name: "test-device-data"
        producer_service: "ingestion-service"
   pubsub_subscriptions:
      - name: "test-analysis-service-sub"
        topic: "test-device-data"
        consumer_service: "analysis-service"
   bigquery_datasets:
      - name: "test_device_analytics"
        description: "Dataset for storing test telemetry."
   bigquery_tables:
      - name: "test_monitor_payloads"
        dataset: "test_device_analytics"
        accessing_services: ["analysis-service"]
        schema_source_type: "go_struct"
        schema_source_identifier: "github.com/illmade-knight/ai-power-mpv/pkg/types.GardenMonitorPayload"

   # gcs_buckets: [] # GCS section is optional

````

### **Further Steps: Staged Deployment and Management**

This guide outlines the full lifecycle for your pipeline: provisioning infrastructure, deploying services, and tearing it all down when it's no longer needed.

#### **Prerequisites**

1. **Google Cloud Projects**: Have two GCP projects created: one for test and one for production.
2. **Authentication**: Ensure your local environment is authenticated to GCP: gcloud auth application-default login.
3. **Enable APIs**: Make sure the "Pub/Sub", "BigQuery", and "Cloud Build" APIs are enabled for **both** projects.
4. **Go Environment**: Have a working Go environment.

### **Step 1: Provision Infrastructure & Generate Scripts**

This step is run locally. It connects to GCP to create resources and then generates deployment scripts tailored for a specific environment.

1. **Update services.yaml**: Open the services.yaml file and set the correct project IDs and any other environment-specific values (like the default\_region or mqtt\_broker\_url).
2. Run the Provisioning Script:
   At the moment vendoring is required as there are replace mod directives - a vendor flag can turn off vendoring if needed (will be false by default in future). From your terminal, run the provision.go script from within the deploy directory. Use the \-env flag to specify your target environment.
   * **For the Test Environment:**  
     \# This will save the scripts in your project's root directory  
     go run provision.go \-services-def=../services.yaml \-env=test \-output-dir=../

   After the script finishes, you will find two new files in your project's root directory:

   * deploy\_ingestion.sh (or .bat on Windows)
   * deploy\_analysis.sh (or .bat on Windows)

### **Step 2: Deploy the Services**

Now, use the generated shell scripts to deploy your applications to Cloud Run.

1. **Review the Scripts**: Open the generated script files to see the gcloud commands that will be run. They should be fully configured.
2. **Make Scripts Executable** (Linux/macOS):  
   chmod \+x deploy\_ingestion.sh  
   chmod \+x deploy\_analysis.sh

3. Run the Deployment Scripts:  
   Execute the scripts to deploy the services. Make sure your gcloud CLI is logged in to the correct project (gcloud config set project your-target-project-id).  
   ./deploy\_ingestion.sh  
   ./deploy\_analysis.sh

### **Step 3: Tear Down an Environment (Optional)**

When you no longer need an environment (e.g., after testing is complete), you can tear down all associated resources.

1. Run the Provisioning Script in Teardown Mode:  
   Run the same provision.go script, but add the \-teardown flag.  
   go run provision.go \-services-def=../services.yaml \-env=test \-teardown

2. **Confirm the Action**: The script will prompt you for confirmation. This is a critical safeguard. Type the name of the environment (test in this case) to proceed. The script will then delete the BigQuery tables, Pub/Sub subscriptions, and topics.
3. **Delete the Cloud Run Services**: The teardown process also generates service deletion scripts (teardown\_ingestion.sh, etc.). Run these to remove the Cloud Run services themselves.  
   \# (On Linux/macOS)  
   chmod \+x teardown\_\*.sh  
   ./teardown\_ingestion.sh  
   ./teardown\_analysis.sh

This completes the full lifecycle management of your serverless data pipeline.