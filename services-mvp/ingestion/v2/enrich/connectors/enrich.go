package connectors

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"time"
)

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
	// AI: Potentially add more validation for mqttMsg.RawPayload if it's critical for enrichment itself
	// Human Response: this should never be the case - we're only enriching with MetaData at this stage
	clientID, locationID, category, err := fetcher(mqttMsg.DeviceInfo.DeviceEUI)
	if err != nil {
		logger.Error().
			Str("device_eui", mqttMsg.DeviceInfo.DeviceEUI).
			Err(err).
			Msg("Failed to fetch device metadata during enrichment")

		log.Error().Str("deviceID", mqttMsg.DeviceInfo.DeviceEUI).Err(err).Msg("get device failed")
		return nil, err
	}

	// Human note - we'll come back to look at the real metadata we want later but this is a good start
	enrichedMsg := &EnrichedMessage{
		RawPayload:              mqttMsg.RawPayload,
		DeviceEUI:               mqttMsg.DeviceInfo.DeviceEUI,
		OriginalMQTTTime:        mqttMsg.MessageTimestamp,
		LoRaWANReceivedAt:       mqttMsg.LoRaWAN.ReceivedAt,
		ClientID:                clientID,
		LocationID:              locationID,
		DeviceCategory:          category,
		IngestionTimestamp:      time.Now().UTC(),
		OriginalClientTimestamp: mqttMsg.MessageTimestamp,
	}

	return enrichedMsg, nil
}
