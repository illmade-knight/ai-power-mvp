package bigquery

import (
	"time"
)

type GardenMonitorMessage struct {
	Topic     string                `json:"topic"`
	MessageID string                `json:"message_id"`
	Timestamp time.Time             `json:"timestamp"`
	Payload   *GardenMonitorPayload `json:"payload"`
}

type GardenMonitorPayload struct {
	UID          string `json:"DE" bigquery:"uid"`
	SIM          string `json:"SIM" bigquery:"sim"`
	RSSI         string `json:"RS" bigquery:"rssi"`
	Version      string `json:"VR" bigquery:"version"`
	Sequence     int    `json:"SQ" bigquery:"sequence"`
	Battery      int    `json:"BA" bigquery:"battery"`
	Temperature  int    `json:"TM" bigquery:"temperature"`
	Humidity     int    `json:"HM" bigquery:"humidity"`
	SoilMoisture int    `json:"SM1" bigquery:"soil_moisture"`
	WaterFlow    int    `json:"FL1" bigquery:"waterflow"`
	WaterQuality int    `json:"WQ" bigquery:"water_quality"`
	TankLevel    int    `json:"DL1" bigquery:"tank_level"`
	AmbientLight int    `json:"AM" bigquery:"ambient_light"`
}
