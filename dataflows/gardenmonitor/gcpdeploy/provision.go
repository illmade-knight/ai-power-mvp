package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"gcpdeploy/templates"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"
	"time"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type DeploymentConfig struct {
	Env                      string
	ProjectID                string
	SourcePath               string
	Region                   string
	AllEnvVars               string
	MinInstances             int
	HealthCheckPath          string
	HealthCheckPort          int
	AllowUnauthenticatedFlag string
	Vendor                   bool
}

type TeardownConfig struct {
	Env         string
	ProjectID   string
	Region      string
	ServiceName string
}

func main() {
	// --- 1. Flags and Logger Setup ---
	servicesDefPath := flag.String("services-def", "services.yaml", "Path to the services definition YAML file.")
	env := flag.String("env", "test", "The environment to provision (e.g., 'test', 'production').")
	dataflowName := flag.String("dataflow", "", "The specific dataflow to provision. If empty, it's inferred from the env.")
	outputDir := flag.String("output-dir", "..", "Directory to save the generated deployment scripts.")
	teardown := flag.Bool("teardown", false, "If true, tear down the resources for the specified environment instead of provisioning.")
	allowUnauthenticated := flag.Bool("allow-unauthenticated", false, "If true, the generated deploy scripts will include the --allow-unauthenticated flag.")
	vendor := flag.Bool("vendor", true, "If true, run 'go mod vendor' before deploying.")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	if *dataflowName == "" {
		*dataflowName = fmt.Sprintf("%s-mqtt-to-bigquery", *env)
	}

	// --- 2. Initialize ServiceManager ---
	ctx := context.Background()
	servicesDef, err := servicemanager.NewYAMLServicesDefinition(*servicesDefPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load services definition")
	}

	fullConfig, _ := servicesDef.GetTopLevelConfig()

	envSpec := fullConfig.Environments[*env]

	var region string
	// Prioritize the environment-specific region, then fall back to the global default.
	if envSpec.DefaultRegion != "" {
		region = envSpec.DefaultRegion
	} else {
		region = fullConfig.DefaultRegion
	}
	if region == "" {
		log.Fatal().Msg("Deployment region could not be determined. Please set a `default_region` in services.yaml either at the top level or for the specific environment.")
	}

	schemaRegistry := make(map[string]interface{})
	schemaRegistry["github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings"] = types.GardenMonitorReadings{}
	manager, err := servicemanager.NewServiceManager(ctx, servicesDef, *env, schemaRegistry, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create service manager.")
	}

	// --- 3. Execute Setup or Teardown Logic ---
	if *teardown {
		handleTeardown(ctx, manager, servicesDef, *env, *dataflowName, region, *outputDir)
	} else {
		handleProvisioning(ctx, manager, servicesDef, *env, *dataflowName, region, *outputDir, *vendor, *allowUnauthenticated)
	}
}

func handleProvisioning(ctx context.Context, manager *servicemanager.ServiceManager, servicesDef servicemanager.ServicesDefinition, env, dataflowName, region, outputDir string, vendor, allowUnauthenticated bool) {
	log.Info().Msgf("Starting provisioning for environment '%s' in region '%s'", env, region)

	projectID, err := servicesDef.GetProjectID(env)
	if err != nil {
		log.Fatal().Err(err).Str("environment", env).Msg("Failed to get project ID for environment")
	}

	log.Info().Str("dataflow", dataflowName).Msg("Provisioning resources for dataflow...")
	provisioned, err := manager.SetupDataflow(ctx, env, dataflowName)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to setup dataflow")
	}

	log.Info().Msg("Generating deployment scripts...")
	fullConfig, _ := servicesDef.GetTopLevelConfig()
	if err := generateDeploymentScripts(provisioned, fullConfig, projectID, env, region, outputDir, vendor, allowUnauthenticated); err != nil {
		log.Fatal().Err(err).Msg("Failed to generate deployment scripts")
	}

	absOutputDir, _ := filepath.Abs(outputDir)
	log.Info().Msg("--- Provisioning Complete ---")
	fmt.Println("✅ Infrastructure provisioning completed successfully.")
	fmt.Printf("✅ Deployment scripts generated in: %s\n", absOutputDir)
	fmt.Println("Next steps: Run the generated scripts to deploy your services.")
}

func handleTeardown(ctx context.Context, manager *servicemanager.ServiceManager, servicesDef servicemanager.ServicesDefinition, env, dataflowName, region, outputDir string) {
	log.Warn().Msgf("--- TEARDOWN MODE ---")
	fmt.Printf("You are about to delete the infrastructure for the '%s' environment.\nThis action is irreversible.\nTo confirm, please type the environment name ('%s'): ", env, env)

	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	if strings.TrimSpace(input) != env {
		fmt.Println("Confirmation failed. Aborting teardown.")
		os.Exit(1)
	}

	fmt.Println("Confirmation accepted. Proceeding with teardown...")
	log.Info().Str("dataflow", dataflowName).Msg("Tearing down resources for dataflow...")
	if err := manager.TeardownDataflow(ctx, env, dataflowName); err != nil {
		log.Fatal().Err(err).Msg("Failed to teardown dataflow")
	}

	log.Info().Msg("Generating service deletion scripts...")
	projectID, _ := servicesDef.GetProjectID(env)
	if err := generateTeardownScripts(projectID, env, region, outputDir); err != nil {
		log.Fatal().Err(err).Msg("Failed to generate teardown scripts")
	}

	absOutputDir, _ := filepath.Abs(outputDir)
	log.Info().Msg("--- Teardown Complete ---")
	fmt.Println("✅ Infrastructure teardown completed successfully.")
	fmt.Printf("✅ Service deletion scripts generated in: %s\n", absOutputDir)
	fmt.Println("Next step: Run the generated scripts to delete the Cloud Run services.")
}

func findServiceSpec(name string, specs []servicemanager.ServiceSpec) (*servicemanager.ServiceSpec, error) {
	for _, spec := range specs {
		if spec.Name == name {
			return &spec, nil
		}
	}
	return nil, errors.New("service spec not found: " + name)
}

func buildEnvVarString(vars ...string) string {
	var nonEmptyVars []string
	for _, v := range vars {
		if v != "" {
			nonEmptyVars = append(nonEmptyVars, v)
		}
	}
	return strings.Join(nonEmptyVars, ",")
}

func buildMetadataEnvVars(metadata map[string]interface{}) string {
	var envVars []string
	for key, val := range metadata {
		envVarName := "APP_" + strings.ToUpper(key)
		envVars = append(envVars, fmt.Sprintf("%s=%v", envVarName, val))
	}
	return strings.Join(envVars, ",")
}

func generateDeploymentScripts(prov *servicemanager.ProvisionedResources, cfg *servicemanager.TopLevelConfig, projectID, env, region, outputDir string, vendor, allowUnauthenticated bool) error {
	isWindows := runtime.GOOS == "windows"
	var ingestionScriptTmpl, analysisScriptTmpl, scriptExt string
	ingestionFilename := "deploy_ingestion"
	analysisFilename := "deploy_analysis"

	if isWindows {
		ingestionScriptTmpl, analysisScriptTmpl = templates.IngestionServiceTemplateBat, templates.AnalysisServiceTemplateBat
		scriptExt = ".bat"
	} else {
		ingestionScriptTmpl, analysisScriptTmpl = templates.IngestionServiceTemplateSh, templates.AnalysisServiceTemplateSh
		scriptExt = ".sh"
	}

	authFlag := "--no-allow-unauthenticated"
	if allowUnauthenticated {
		authFlag = "--allow-unauthenticated"
	}

	ingestionSpec, _ := findServiceSpec("ingestion-service", cfg.Services)
	ingestionStdVars := fmt.Sprintf("APP_PROJECT_ID=%s,APP_PUBLISHER_TOPIC_ID=%s", projectID, prov.PubSubTopics[0].Name)
	ingestionMetaVars := buildMetadataEnvVars(ingestionSpec.Metadata)
	ingestionConfig := DeploymentConfig{
		Env:                      env,
		ProjectID:                projectID,
		Region:                   region,
		SourcePath:               ingestionSpec.SourcePath,
		MinInstances:             ingestionSpec.MinInstances,
		AllEnvVars:               buildEnvVarString(ingestionStdVars, ingestionMetaVars),
		AllowUnauthenticatedFlag: authFlag,
		Vendor:                   vendor,
	}
	if ingestionSpec.HealthCheck != nil {
		ingestionConfig.HealthCheckPort = ingestionSpec.HealthCheck.Port
		ingestionConfig.HealthCheckPath = ingestionSpec.HealthCheck.Path
	}
	if err := renderTemplate(filepath.Join(outputDir, ingestionFilename+scriptExt), ingestionScriptTmpl, ingestionConfig); err != nil {
		return err
	}

	analysisSpec, _ := findServiceSpec("analysis-service", cfg.Services)
	analysisStdVars := fmt.Sprintf("APP_PROJECT_ID=%s,APP_CONSUMER_SUBSCRIPTION_ID=%s,APP_BIGQUERY_DATASET_ID=%s,APP_BIGQUERY_TABLE_ID=%s",
		projectID, prov.PubSubSubscriptions[0].Name, prov.BigQueryTables[0].Dataset, prov.BigQueryTables[0].Name)
	analysisMetaVars := buildMetadataEnvVars(analysisSpec.Metadata)
	analysisConfig := DeploymentConfig{
		Env:                      env,
		ProjectID:                projectID,
		Region:                   region,
		SourcePath:               analysisSpec.SourcePath,
		MinInstances:             ingestionSpec.MinInstances,
		AllEnvVars:               buildEnvVarString(analysisStdVars, analysisMetaVars),
		AllowUnauthenticatedFlag: authFlag,
		Vendor:                   vendor,
	}
	if analysisSpec.HealthCheck != nil {
		analysisConfig.HealthCheckPort = analysisSpec.HealthCheck.Port
		analysisConfig.HealthCheckPath = analysisSpec.HealthCheck.Path
	}
	return renderTemplate(filepath.Join(outputDir, analysisFilename+scriptExt), analysisScriptTmpl, analysisConfig)
}

func generateTeardownScripts(projectID, env, region, outputDir string) error {
	isWindows := runtime.GOOS == "windows"
	var teardownTmpl, scriptExt, filename string

	if isWindows {
		teardownTmpl, scriptExt = templates.TeardownServiceTemplateBat, ".bat"
	} else {
		teardownTmpl, scriptExt = templates.TeardownServiceTemplateSh, ".sh"
	}

	services := []string{"ingestion-service", "analysis-service"}
	for _, serviceName := range services {
		config := TeardownConfig{
			Env:         env,
			ProjectID:   projectID,
			Region:      region,
			ServiceName: serviceName,
		}
		filename = "teardown_" + strings.ReplaceAll(serviceName, "-service", "") + scriptExt
		if err := renderTemplate(filepath.Join(outputDir, filename), teardownTmpl, config); err != nil {
			return fmt.Errorf("failed to generate teardown script for %s: %w", serviceName, err)
		}
	}
	return nil
}

func renderTemplate(filePath, tmplStr string, data interface{}) error {
	tmpl, err := template.New(filepath.Base(filePath)).Parse(tmplStr)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return err
	}
	return os.WriteFile(filePath, buf.Bytes(), 0755)
}
