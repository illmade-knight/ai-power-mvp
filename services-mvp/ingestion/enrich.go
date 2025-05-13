package ingestion

import (
	"github.com/rs/zerolog"
	"time"
)

// DeviceMetadataFetcher is a function type for fetching device-specific metadata.
type DeviceMetadataFetcher func(deviceEUI string) (clientID, locationID, category string, err error)

// EnrichMQTTData processes an MQTTMessage, validates it, fetches metadata,
// and creates an EnrichedMessage.
func EnrichMQTTData(
	mqttMsg *MQTTMessage,
	fetcher DeviceMetadataFetcher,
	logger zerolog.Logger,
) (*EnrichedMessage, error) {
	if mqttMsg == nil {
		logger.Error().Msg("Input MQTTMessage is nil for enrichment")
		return nil, ErrInvalidMessage
	}
	if mqttMsg.DeviceInfo.DeviceEUI == "" {
		logger.Error().Msg("MQTTMessage is missing DeviceEUI for enrichment")
		return nil, ErrInvalidMessage
	}
	// Potentially add more validation for mqttMsg.RawPayload if it's critical for enrichment itself

	clientID, locationID, category, err := fetcher(mqttMsg.DeviceInfo.DeviceEUI)
	if err != nil {
		logger.Error().
			Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).
			Err(err).
			Msg("Failed to fetch device metadata during enrichment")
		return nil, err
	}

	enrichedMsg := &EnrichedMessage{
		RawPayload:         mqttMsg.RawPayload,
		DeviceEUI:          mqttMsg.DeviceInfo.DeviceEUI,
		OriginalMQTTTime:   mqttMsg.MessageTimestamp,
		LoRaWANReceivedAt:  mqttMsg.LoRaWAN.ReceivedAt,
		ClientID:           clientID,
		LocationID:         locationID,
		DeviceCategory:     category,
		IngestionTimestamp: time.Now().UTC(),
	}

	logger.Debug().
		Str("device_eui", enrichedMsg.DeviceEUI).
		Msg("Successfully enriched MQTT message")
	return enrichedMsg, nil
}
