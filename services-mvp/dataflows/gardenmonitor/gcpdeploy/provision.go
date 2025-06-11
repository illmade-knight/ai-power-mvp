package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/servicemanager"
	"github.com/illmade-knight/ai-power-mpv/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// --- 1. Flags and Logger Setup ---
	servicesDefPath := flag.String("services-def", "services.yaml", "Path to the services definition YAML file.")
	env := flag.String("env", "test", "The environment to provision (e.g., 'test').")
	dataflowName := flag.String("dataflow", "test-mqtt-to-bigquery", "The specific dataflow to provision.")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	log.Info().Msgf("Starting provisioning for environment '%s' using definition '%s'", *env, *servicesDefPath)

	// --- 2. Initialize ServiceManager ---
	ctx := context.Background()

	servicesDef, err := servicemanager.NewYAMLServicesDefinition(*servicesDefPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load services definition")
	}

	// The schema registry is needed to create BigQuery tables from Go structs.
	schemaRegistry := make(map[string]interface{})
	schemaRegistry["github.com/illmade-knight/ai-power-mpv/pkg/types.GardenMonitorPayload"] = types.GardenMonitorPayload{}

	manager, err := servicemanager.NewServiceManager(ctx, servicesDef, *env, schemaRegistry, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create service manager")
	}

	// --- 3. Provision Infrastructure ---
	log.Info().Str("dataflow", *dataflowName).Msg("Provisioning resources for dataflow...")
	provisioned, err := manager.SetupDataflow(ctx, *env, *dataflowName)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to setup dataflow")
	}

	// --- 4. Log Results ---
	log.Info().Msg("--- Provisioning Complete ---")
	for _, topic := range provisioned.PubSubTopics {
		log.Info().Str("topic_name", topic.Name).Msg("Provisioned Pub/Sub Topic")
	}
	for _, sub := range provisioned.PubSubSubscriptions {
		log.Info().Str("subscription_name", sub.Name).Str("topic", sub.Topic).Msg("Provisioned Pub/Sub Subscription")
	}
	for _, dataset := range provisioned.BigQueryDatasets {
		log.Info().Str("dataset_name", dataset.Name).Msg("Provisioned BigQuery Dataset")
	}
	for _, table := range provisioned.BigQueryTables {
		log.Info().Str("table_name", table.Name).Str("dataset", table.Dataset).Msg("Provisioned BigQuery Table")
	}
	log.Info().Msg("-----------------------------")
	fmt.Println("✅ Infrastructure provisioning completed successfully.")
}
