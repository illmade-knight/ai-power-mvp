package mqttconverter

import (
	"encoding/json"
	"time"
)

type InMessage struct {
	Payload   []byte    `json:"payload"`
	Topic     string    `json:"topic"`
	MessageID string    `json:"message_id"`
	Timestamp time.Time `json:"timestamp"`
	Duplicate bool      `json:"duplicate"`
}

type GardenMonitorMessage struct {
	Topic     string                `json:"topic"`
	MessageID string                `json:"message_id"`
	Timestamp time.Time             `json:"timestamp"`
	Payload   *GardenMonitorPayload `json:"payload"`
}

type GardenMonitorPayload struct {
	DE           string `json:"DE"`
	SIM          string `json:"SIM"`
	RSSI         string `json:"RS"`
	Version      string `json:"VR"`
	Temperature  int    `json:"TM"`
	Battery      int    `json:"BA"`
	WaterFlow    int    `json:"FL1"`
	SoilMoisture int    `json:"SM1"`
	WaterQuality int    `json:"WQ"`
	Humidity     int    `json:"HM"`
	TankLevel    int    `json:"DL1"`
	AmbientLight int    `json:"AM"`
	Sequence     int    `json:"SQ"`
}

// // ParseMQTTMessage Helper function to parse a JSON string into an MQTTMessage struct.
// // This would be used in your main service logic and in tests.
func ParseMQTTMessage(jsonData []byte) (*GardenMonitorPayload, error) {
	var msg GardenMonitorPayload
	err := json.Unmarshal(jsonData, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
