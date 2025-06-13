@echo off
:: Auto-generated deployment script for the analysis-service

:: --- Configuration ---
set GCP_PROJECT_ID=gemini-power-test
set SERVICE_NAME=analysis-service-test
set REGION=europe-west1
set SOURCE_PATH=bigquery



:: --- Deployment ---
echo Deploying %SERVICE_NAME% from %SOURCE_PATH% to project %GCP_PROJECT_ID% in region %REGION%...

gcloud run deploy "%SERVICE_NAME%" ^
  --source "%SOURCE_PATH%" ^
  --platform managed ^
  --project "%GCP_PROJECT_ID%" ^
  --region "%REGION%" ^
  --no-allow-unauthenticated ^
  --set-env-vars="APP_PROJECT_ID=gemini-power-test,APP_CONSUMER_SUBSCRIPTION_ID=test-analysis-service-sub,APP_BIGQUERY_DATASET_ID=test_device_analytics,APP_BIGQUERY_TABLE_ID=test_monitor_payloads" ^
  --liveness-probe=httpGet.path=/healthz ^
  --min-instances=1



echo.
echo ✅ Deployment of %SERVICE_NAME% complete.
