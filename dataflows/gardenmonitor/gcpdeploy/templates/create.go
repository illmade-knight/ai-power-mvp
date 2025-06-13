package templates

// This file contains the Go string constants for the deployment scripts.

// --- Bash Script Templates ---

const IngestionServiceTemplateSh = `#!/bin/bash
# Auto-generated deployment script for the ingestion-service

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
GCP_PROJECT_ID="{{.ProjectID}}"
SERVICE_NAME="ingestion-service-{{.Env}}"
REGION="{{.Region}}"
SOURCE_PATH="{{.SourcePath}}"

{{if .Vendor}}
# --- Pre-deployment Steps (Vendoring) ---
echo "Vendoring dependencies for ingestion-service..."
# Use a subshell to run go mod vendor in the correct directory
(cd "${SOURCE_PATH}" && go mod vendor)

# Setup a trap to clean up the vendor directory on exit (success or failure)
trap 'echo "Cleaning up vendor directory..."; (cd "${SOURCE_PATH}" && rm -rf vendor)' EXIT
{{end}}

# --- Deployment ---
echo "Deploying ${SERVICE_NAME} from ${SOURCE_PATH} to project ${GCP_PROJECT_ID} in region ${REGION}..."

gcloud run deploy "${SERVICE_NAME}" \
  --source "${SOURCE_PATH}" \
  --platform managed \
  --project "${GCP_PROJECT_ID}" \
  --region "${REGION}" \
  {{.AllowUnauthenticatedFlag}} \
  --set-env-vars="{{.AllEnvVars}}" \
  {{if .HealthCheckPath}}--liveness-probe=httpGet.path={{.HealthCheckPath}}{{end}} \
  {{if gt .MinInstances 0}}--min-instances={{.MinInstances}}{{end}}

echo "✅ Deployment of ${SERVICE_NAME} complete."
`

const AnalysisServiceTemplateSh = `#!/bin/bash
# Auto-generated deployment script for the analysis-service

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
GCP_PROJECT_ID="{{.ProjectID}}"
SERVICE_NAME="analysis-service-{{.Env}}"
REGION="{{.Region}}"
SOURCE_PATH="{{.SourcePath}}"

{{if .Vendor}}
# --- Pre-deployment Steps (Vendoring) ---
echo "Vendoring dependencies for analysis-service..."
# Use a subshell to run go mod vendor in the correct directory
(cd "${SOURCE_PATH}" && go mod vendor)

# Setup a trap to clean up the vendor directory on exit (success or failure)
trap 'echo "Cleaning up vendor directory..."; (cd "${SOURCE_PATH}" && rm -rf vendor)' EXIT
{{end}}

# --- Deployment ---
echo "Deploying ${SERVICE_NAME} from ${SOURCE_PATH} to project ${GCP_PROJECT_ID} in region ${REGION}..."

gcloud run deploy "${SERVICE_NAME}" \
  --source "${SOURCE_PATH}" \
  --platform managed \
  --project "${GCP_PROJECT_ID}" \
  --region "${REGION}" \
  {{.AllowUnauthenticatedFlag}} \
  --set-env-vars="{{.AllEnvVars}}" \
  {{if .HealthCheckPath}}--liveness-probe=httpGet.path={{.HealthCheckPath}}{{end}} \
  {{if gt .MinInstances 0}}--min-instances={{.MinInstances}}{{end}}

echo "✅ Deployment of ${SERVICE_NAME} complete."
`

// --- Windows Batch Script Templates ---

const IngestionServiceTemplateBat = `@echo off
:: Auto-generated deployment script for the ingestion-service

:: --- Configuration ---
set GCP_PROJECT_ID={{.ProjectID}}
set SERVICE_NAME=ingestion-service-{{.Env}}
set REGION={{.Region}}
set SOURCE_PATH={{.SourcePath}}

{{if .Vendor}}
:: --- Pre-deployment Steps (Vendoring) ---
echo Vendoring dependencies for ingestion-service...
pushd %SOURCE_PATH%
go mod vendor
popd
{{end}}

:: --- Deployment ---
echo Deploying %SERVICE_NAME% from %SOURCE_PATH% to project %GCP_PROJECT_ID% in region %REGION%...

gcloud run deploy "%SERVICE_NAME%" ^
  --source "%SOURCE_PATH%" ^
  --platform managed ^
  --project "%GCP_PROJECT_ID%" ^
  --region "%REGION%" ^
  {{.AllowUnauthenticatedFlag}} ^
  --set-env-vars="{{.AllEnvVars}}" ^
  {{if .HealthCheckPath}}--liveness-probe=httpGet.path={{.HealthCheckPath}} ^{{end}}
  {{if gt .MinInstances 0}}--min-instances={{.MinInstances}}{{end}}

{{if .Vendor}}
:: --- Post-deployment Cleanup ---
echo Cleaning up vendor directory...
if exist "%SOURCE_PATH%\\vendor" (
  rmdir /s /q "%SOURCE_PATH%\\vendor"
)
{{end}}

echo.
echo ✅ Deployment of %SERVICE_NAME% complete.
`

const AnalysisServiceTemplateBat = `@echo off
:: Auto-generated deployment script for the analysis-service

:: --- Configuration ---
set GCP_PROJECT_ID={{.ProjectID}}
set SERVICE_NAME=analysis-service-{{.Env}}
set REGION={{.Region}}
set SOURCE_PATH={{.SourcePath}}

{{if .Vendor}}
:: --- Pre-deployment Steps (Vendoring) ---
echo Vendoring dependencies for analysis-service...
pushd %SOURCE_PATH%
go mod vendor
popd
{{end}}

:: --- Deployment ---
echo Deploying %SERVICE_NAME% from %SOURCE_PATH% to project %GCP_PROJECT_ID% in region %REGION%...

gcloud run deploy "%SERVICE_NAME%" ^
  --source "%SOURCE_PATH%" ^
  --platform managed ^
  --project "%GCP_PROJECT_ID%" ^
  --region "%REGION%" ^
  {{.AllowUnauthenticatedFlag}} ^
  --set-env-vars="{{.AllEnvVars}}" ^
  {{if .HealthCheckPath}}--liveness-probe=httpGet.path={{.HealthCheckPath}} ^{{end}}
  {{if gt .MinInstances 0}}--min-instances={{.MinInstances}}{{end}}

{{if .Vendor}}
:: --- Post-deployment Cleanup ---
echo Cleaning up vendor directory...
if exist "%SOURCE_PATH%\\vendor" (
  rmdir /s /q "%SOURCE_PATH%\\vendor"
)
{{end}}

echo.
echo ✅ Deployment of %SERVICE_NAME% complete.
`
