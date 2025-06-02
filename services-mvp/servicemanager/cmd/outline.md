Service Manager: Local CLI Tool Design with Cobra
This document outlines a plan for creating a local Command Line Interface (CLI) tool for the servicemanager using the Cobra library. This CLI is intended for local development, testing, and CI/CD operations, separate from the main deployed apiserver.

The primary servicemanager/main.go will remain the entry point for the deployed Cloud Run service, directly starting the apiserver.

1. Rationale for Cobra (for the Local CLI)
   Using Cobra for the local CLI provides:

A structured, command-based interface (tool command subcommand --flag).

Robust flag parsing.

Automatic help generation.

Extensibility for future local operations.

2. Recommended Directory Structure for the CLI
   your_module_path/servicemanager/cli/main.go: The main package and entry point for building the local CLI executable (e.g., smctl).

your_module_path/servicemanager/cmd/: Contains all Cobra command definitions.

root.go: Defines the root CLI command (e.g., smctl).

apply.go: Defines the smctl apply command.

destroy.go: Defines the smctl destroy command.

validate.go: Defines the smctl validate command.

serve.go: (Optional) Defines a smctl serve command to run the apiserver locally for testing, distinct from the main Cloud Run deployment.

3. CLI Entry Point (cli/main.go)
   This file is very simple; it initializes and executes the Cobra root command.

Example Snippet (cli/main.go):

package main

import (
// Assumes your cmd package is 'your_module_path/servicemanager/cmd'
"your_module_path/servicemanager/cmd"
)

func main() {
cmd.Execute() // cmd.Execute() is defined in your servicemanager/cmd/root.go
}

You would build this CLI tool using a command like: go build -o smctl ./servicemanager/cli/main.go

4. Root Command (cmd/root.go)
   This defines the base command for your local CLI (e.g., smctl).

Purpose: Entry point for all CLI subcommands. Defines global/persistent flags applicable to the CLI operations.

Key Persistent Flags:

--config or -c: Path to the master YAML configuration file (required for apply, destroy, validate, serve).

--env or -e: Target environment (e.g., "test", "prod") (required for apply, destroy, serve).

--project or -p: Optional GCP Project ID override for provisioning commands.

--log-level: Global log level for CLI operations.

The Execute() function will be defined here, to be called by cli/main.go.

The PersistentPreRunE can set up the logger based on --log-level.

(The content of cmd/root.go would be similar to what was previously outlined for the combined CLI/server, but now it's exclusively for the local tool).

5. CLI Subcommands (cmd/*.go)
   These commands are for local/CI/CD interaction with your service manager's provisioning and validation capabilities.

A. apply Command (cmd/apply.go)

Use: smctl apply --config <path> --env <env_name> [--project <id>]

Purpose: Creates or updates resources in GCP based on the YAML config.

Logic:

Validates required flags (--config, --env).

Loads initialization.TopLevelConfig from the specified config file.

Determines the target GCP Project ID.

Instantiates managers from the initialization package (PubSubManager, BigQueryManager, StorageManager).

This command will create real GCP clients (e.g., *pubsub.Client, *storage.Client) using appropriate credentials (ADC by default, or a service account key if a flag for it is added) and pass them (or adapters) to the manager constructors.

Calls Setup() methods on each manager.

B. destroy Command (cmd/destroy.go)

Use: smctl destroy --config <path> --env <env_name> [--project <id>] [--force]

Purpose: Deletes resources from GCP.

Flags: Adds a local --force flag to bypass teardown protection defined in the config.

Logic: Similar to apply, but calls Teardown() methods. Respects TeardownProtection unless --force is used.

C. validate Command (cmd/validate.go)

Use: smctl validate --config <path>

Purpose: Validates the YAML configuration file's syntax and structure.

Logic: Calls initialization.LoadAndValidateConfig().

D. serve Command (cmd/serve.go) - For Local API Server Testing

Use: smctl serve --config <path> --env <env_name> [--port <port_num>]

Purpose: Runs the apiserver (the /config HTTP endpoint) locally. This is useful for testing the API server itself or for local development of services that depend on it, without needing a full Cloud Run deployment of the servicemanager.

Logic:

Validates required flags.

Loads initialization.TopLevelConfig.

Instantiates apiserver.NewConfigServer().

Sets up HTTP routes and starts http.ListenAndServe(). This logic is similar to what's in your current servicemanager/main.go but invoked via the CLI.

6. Interaction with initialization and apiserver Packages
   The CLI commands (apply, destroy, validate, serve) will import and use functions/types from your servicemanager/initialization and servicemanager/apiserver packages.

For example, cmd/apply.go will use initialization.LoadAndValidateConfig, initialization.NewPubSubManager, initialization.NewBigQueryManager, initialization.NewStorageManager, etc.

cmd/serve.go will use initialization.LoadAndValidateConfig and apiserver.NewConfigServer.

This revised structure provides a clear separation:

servicemanager/main.go: The lean entry point for your deployed Cloud Run API service.

servicemanager/cli/main.go + servicemanager/cmd/: The entry point and implementation for your powerful local CLI tool (smctl).

This allows you to evolve the CLI tool with more local commands (e.g., status checks, diffs against live infrastructure) without impacting the deployed API service's main entry point.