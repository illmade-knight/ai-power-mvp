package messenger

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
