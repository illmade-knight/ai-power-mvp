package main

import (
	// Replace "your_module_path/servicemanager/cmd" with the actual import path
	// to your cmd package where rootCmd and Execute() are defined.
	"github.com/illmade-knight/ai-power-mvp/services-mvp/servicemanager/cmd"
)

func main() {
	// cmd.Execute() is defined in your servicemanager/cmd/root.go file.
	// It initializes and executes the Cobra root command, which handles
	// parsing arguments, flags, and dispatching to the appropriate subcommands.
	cmd.Execute()
}
