package connectors

import (
	"github.com/rs/zerolog"
	"reflect"
	"testing"
	"time"
)

func TestEnrichMQTTData(t *testing.T) {
	fetcher := mockMetadataFetcherFunc("DEV001", "DEV001", "ClientA", "Loc1", "Sensor", nil)
	now := time.Now().UTC().Truncate(1 * time.Second)

	type args struct {
		mqttMsg *MQTTMessage
		fetcher DeviceMetadataFetcher
		logger  zerolog.Logger
	}
	tests := []struct {
		name    string
		args    args
		want    *EnrichedMessage
		wantErr bool
	}{
		{
			name: "single message",
			args: args{
				fetcher: fetcher,
				mqttMsg: &MQTTMessage{
					DeviceInfo:       DeviceInfo{DeviceEUI: "DEV001"},
					RawPayload:       "payload",
					MessageTimestamp: now,
					LoRaWAN: LoRaWANData{ // Add some LoRaWAN data for completeness
						RSSI:       -70,
						SNR:        7.5,
						ReceivedAt: now.Add(-2 * time.Second), // Slightly before message timestamp
					},
				},
			},
			want: &EnrichedMessage{
				RawPayload:         "payload",
				DeviceEUI:          "DEV001",
				OriginalMQTTTime:   now,
				LoRaWANReceivedAt:  now.Add(-2 * time.Second),
				ClientID:           "ClientA",
				LocationID:         "Loc1",
				DeviceCategory:     "Sensor",
				IngestionTimestamp: now,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EnrichMQTTData(tt.args.mqttMsg, tt.args.fetcher, tt.args.logger)
			// we can't judge the got.IngestionTimestamp so just check it's not crazy
			if got.IngestionTimestamp.Truncate(time.Second) != tt.want.IngestionTimestamp {
				t.Errorf("EnrichMQTTData() = %v, want %v", got.IngestionTimestamp, tt.want.IngestionTimestamp)
			} else {
				// good enough so just set so we can do a deep equal next
				tt.want.IngestionTimestamp = got.IngestionTimestamp
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("EnrichMQTTData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EnrichMQTTData() got = %v, want %v", got, tt.want)
			}
		})
	}
}
