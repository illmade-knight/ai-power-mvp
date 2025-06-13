@echo off
:: Auto-generated deployment script for the ingestion-service

:: --- Configuration ---
set GCP_PROJECT_ID=gemini-power-test
set SERVICE_NAME=ingestion-service-test
set REGION=europe-west1
set SOURCE_PATH=ingestion



:: --- Deployment ---
echo Deploying %SERVICE_NAME% from %SOURCE_PATH% to project %GCP_PROJECT_ID% in region %REGION%...

gcloud run deploy "%SERVICE_NAME%" ^
  --source "%SOURCE_PATH%" ^
  --platform managed ^
  --project "%GCP_PROJECT_ID%" ^
  --region "%REGION%" ^
  --no-allow-unauthenticated ^
  --set-env-vars="APP_PROJECT_ID=gemini-power-test,APP_PUBLISHER_TOPIC_ID=test-device-data,APP_TOPIC=garden_monitor/861275073104248,APP_CLIENT_ID_PREFIX=garden_broker,APP_BROKER_URL=tcp://broker.emqx.io:1883" ^
  --liveness-probe=httpGet.path=/healthz ^
  --min-instances=1



echo.
echo ✅ Deployment of %SERVICE_NAME% complete.
