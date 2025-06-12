package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/illmade-knight/ai-power-mpv/pkg/mqttconverter"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// 1. Setup logger
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// 2. Parse command-line flags
	numMessages := flag.Int("n", 10, "Number of messages to capture before exiting.")
	outputFile := flag.String("o", "mqtt_samples.json", "Output file to save the captured messages.")
	configFile := flag.String("c", "helpers/sampler/test.json", "Configuration file to use")

	flag.Parse()

	log.Info().Msg("Starting MQTT Sampler Tool...")

	// 3. Load configuration from environment
	mqttCfg, err := mqttconverter.LoadMQTTClientConfigFromFile(*configFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load MQTT configuration")
	}
	log.Info().Str("broker", mqttCfg.BrokerURL).Str("topic", mqttCfg.Topic).Str("output", *outputFile).
		Int("sampling", *numMessages).
		Msg("Configuration loaded")

	// 4. Create and run the sampler
	sampler := mqttconverter.NewSampler(*mqttCfg, log.Logger, *numMessages)
	if err := sampler.Run(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("Sampler execution failed")
	}

	// 5. Get results and write to file
	messages := sampler.Messages()
	if len(messages) == 0 {
		log.Warn().Msg("No messages were captured. The output file will not be created.")
		return
	}

	log.Info().Msg("Shutdown complete. Writing captured messages to file.")
	if err := writeMessagesToFile(*outputFile, messages); err != nil {
		log.Error().Err(err).Str("file", *outputFile).Msg("Failed to write messages to file")
	} else {
		log.Info().Str("file", *outputFile).Int("message_count", len(messages)).Msg("Successfully saved captured messages.")
	}
}

// writeMessagesToFile saves the slice of captured messages to a file as a JSON array.
func writeMessagesToFile(filename string, messages []mqttconverter.CapturedMessage) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}
	defer file.Close()

	// Use an encoder to write formatted JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty-print the final JSON file

	if err := encoder.Encode(messages); err != nil {
		return fmt.Errorf("could not encode messages to JSON: %w", err)
	}
	return nil
}
