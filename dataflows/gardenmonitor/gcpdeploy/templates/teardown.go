package templates

// --- NEW: Teardown Script Templates ---

// --- Teardown Script Templates ---

// --- Teardown Script Templates ---

const TeardownServiceTemplateSh = `#!/bin/bash
# Auto-generated teardown script for the {{.ServiceName}} service

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
GCP_PROJECT_ID="{{.ProjectID}}"
SERVICE_NAME="{{.ServiceName}}-{{.Env}}"
REGION="{{.Region}}"

# --- Teardown ---
echo "Deleting Cloud Run service ${SERVICE_NAME} from project ${GCP_PROJECT_ID} in region ${REGION}..."

gcloud run services delete "${SERVICE_NAME}" \
  --project "${GCP_PROJECT_ID}" \
  --region "${REGION}" \
  --quiet

echo "✅ Deletion of ${SERVICE_NAME} complete."
`

const TeardownServiceTemplateBat = `@echo off
:: Auto-generated teardown script for the {{.ServiceName}} service

:: --- Configuration ---
set GCP_PROJECT_ID={{.ProjectID}}
set SERVICE_NAME={{.ServiceName}}-{{.Env}}
set REGION={{.Region}}

:: --- Teardown ---
echo Deleting Cloud Run service %SERVICE_NAME% from project %GCP_PROJECT_ID% in region %REGION%...

gcloud run services delete "%SERVICE_NAME%" ^
  --project "%GCP_PROJECT_ID%" ^
  --region "%REGION%" ^
  --quiet

echo.
echo ✅ Deletion of %SERVICE_NAME% complete.
`
