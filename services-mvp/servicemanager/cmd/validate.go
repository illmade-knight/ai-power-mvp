package cmd

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	// Replace "your_module_path/servicemanager/initialization" with the actual import path
	// to your initialization package, where LoadAndValidateConfig is defined.
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/initialization"
)

// validateCmd represents the validate command
var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate the service manager's YAML configuration file",
	Long: `The validate command parses the specified YAML configuration file
and checks for basic structural integrity and required fields.

It ensures that the configuration can be successfully loaded by the servicemanager.`,
	Example: `  smctl validate --config ./service.yaml
  smctl validate -c path/to/your/config.yaml`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// cfgFile is the persistent flag from root.go
		if cfgFile == "" {
			return fmt.Errorf("--config flag (or -c) must be set to specify the configuration file")
		}
		// You could add more checks here, e.g., if the file actually exists,
		// though LoadAndValidateConfig will also handle that.
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Info().Str("config_file", cfgFile).Msg("Attempting to validate configuration file...")

		// LoadAndValidateConfig is assumed to be in your initialization package.
		// It should handle reading the file and performing all necessary validations.
		_, err := initialization.LoadAndValidateConfig(cfgFile)
		if err != nil {
			log.Error().Err(err).Str("config_file", cfgFile).Msg("Configuration validation failed.")
			// It's often good practice for CLI commands to return the error
			// so Cobra can handle printing it (unless SilenceErrors is true on rootCmd).
			return fmt.Errorf("validation error for '%s': %w", cfgFile, err)
		}

		log.Info().Str("config_file", cfgFile).Msg("Configuration file is valid.")
		fmt.Printf("Configuration file '%s' validated successfully.\n", cfgFile)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)

	// Here you can define any local flags specific to the validate command.
	// For example:
	// validateCmd.Flags().BoolP("verbose", "v", false, "Enable verbose validation output")
	// Since validation primarily relies on the global --config flag,
	// it might not need many local flags.
}
