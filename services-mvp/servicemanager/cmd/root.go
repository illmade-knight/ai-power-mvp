package cmd

import (
	"os"
	"time" // Added for TimeFormat in logger

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log" // Using zerolog's global logger
	"github.com/spf13/cobra"
)

var (
	// cfgFile will be used by subcommands to load the main YAML configuration.
	// It's a persistent flag, so it's available to all commands.
	cfgFile string

	// environment specifies the target environment (e.g., "test", "prod").
	// Also a persistent flag.
	environment string

	// projectID allows overriding the GCP project MessageID specified in the config or environment.
	// Persistent flag.
	projectID string

	// logLevel controls the verbosity of the CLI's logging.
	// Persistent flag.
	logLevel string
)

// rootCmd represents the base command when called without any subcommands.
// This will be the entry point for your local CLI tool (e.g., 'smctl').
var rootCmd = &cobra.Command{
	Use:   "smctl", // Or servicemanager-cli, or your preferred CLI name
	Short: "A CLI tool to manage and inspect microservice infrastructure configurations.",
	Long: `smctl (Service Manager Control) is a command-line interface
for interacting with the microservice infrastructure manager.

It allows you to:
  - Apply configurations to provision or update cloud resources.
  - Destroy configured resources in non-production environments.
  - Validate configuration files.
  - (Locally) Serve the configuration access API for testing.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Setup global logger based on the --log-level flag.
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs // Or time.RFC3339Nano etc.
		// Use a console writer for more human-readable local CLI output.
		consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		log.Logger = zerolog.New(consoleWriter).With().Timestamp().Logger()

		// Parse and set the global log level.
		level, err := zerolog.ParseLevel(logLevel)
		if err != nil {
			// Log a warning using the default (InfoLevel) logger before it's potentially changed.
			log.Warn().Str("provided_level", logLevel).Msg("Invalid log level provided. Defaulting to 'info'.")
			zerolog.SetGlobalLevel(zerolog.InfoLevel) // Default to InfoLevel if parsing fails
		} else {
			zerolog.SetGlobalLevel(level)
		}
		log.Debug().Msg("Logger initialized.") // This will only show if logLevel is debug or trace
		return nil
	},
	// SilenceUsage is an option to silence usage printing on error.
	// Useful if you're handling errors and don't want Cobra's default usage message.
	// SilenceUsage: true,
	// SilenceErrors: true, // If you want to handle all error printing yourself.
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by cli/main.go. It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		// Cobra prints the error by default. If SilenceErrors is true, you'd print it here.
		// fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func init() {
	// Define persistent flags for the root command. These are global.
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Path to the master YAML configuration file (e.g., ./service.yaml)")
	rootCmd.PersistentFlags().StringVarP(&environment, "env", "e", "", "Target environment (e.g., 'test', 'prod') defined in the config file")
	rootCmd.PersistentFlags().StringVarP(&projectID, "project", "p", "", "GCP Project MessageID (overrides project MessageID from config for the specified environment)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Set the logging level (trace, debug, info, warn, error, fatal, panic)")

	// You can mark flags as required if they are always needed,
	// or check for them in individual command's PreRunE or RunE hooks.
	// Example:
	// if err := rootCmd.MarkPersistentFlagRequired("config"); err != nil {
	// 	 log.Fatal().Err(err).Msg("Failed to mark 'config' flag as required")
	// }
	// It's often better to mark them required on subcommands that actually need them,
	// as the root command itself might not require any flags if it just shows help.
}
