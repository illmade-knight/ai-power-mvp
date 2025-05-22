package telemetrypb

import (
	"cloud.google.com/go/bigquery"
	"time"
)

// --- Wrapper struct implementing bigquery.ValueSaver
// we get a go struct fom pb but to use it in BigQuery etc we want to adjust it without editing the auto get file
//---

func WrapMeterReadingForBQ(r *MeterReading) *MeterReadingBQWrapper {
	w := MeterReadingBQWrapper{
		Uid:            r.Uid,
		Reading:        r.Reading,
		AverageCurrent: r.AverageCurrent,
		MaxCurrent:     r.MaxCurrent,
		MaxVoltage:     r.MaxVoltage,
		AverageVoltage: r.AverageVoltage,
		DeviceEui:      r.DeviceEui,
		ClientId:       r.ClientId,
		LocationId:     r.LocationId,
		DeviceCategory: r.DeviceCategory,
		DeviceType:     r.DeviceType,
	}

	// Handle protobuf Timestamps, converting to time.Time for BigQuery
	// If s.MeterReading was just initialized to zero, these timestamp fields will be nil,
	// and their corresponding entries in the 'row' map will remain nil (or become time.Time{} if BQ requires non-nil).
	// For schema inference, time.Time{} is fine. For actual data, nil is fine for nullable BQ TIMESTAMPs.
	if r.OriginalMqttTime != nil && r.OriginalMqttTime.IsValid() {
		w.OriginalMqttTime = r.OriginalMqttTime.AsTime()
	}

	if r.UpstreamIngestionTimestamp != nil && r.UpstreamIngestionTimestamp.IsValid() {
		w.UpstreamIngestionTimestamp = r.UpstreamIngestionTimestamp.AsTime()
	}

	if r.ProcessedTimestamp != nil && r.ProcessedTimestamp.IsValid() {
		w.ProcessedTimestamp = r.ProcessedTimestamp.AsTime()
	}

	return &w
}

// MeterReadingBQWrapper wraps the auto-generated telemetrypb.MeterReading
// to provide a Save method for BigQuery, ensuring snake_case column names for schema inference.
type MeterReadingBQWrapper struct {
	Uid                        string    `bigquery:"uid"`             // BigQuery: uid
	Reading                    float32   `bigquery:"reading"`         // BigQuery: reading
	AverageCurrent             float32   `bigquery:"average_current"` // BigQuery: average_current
	MaxCurrent                 float32   `bigquery:"max_current"`     // BigQuery: max_current
	MaxVoltage                 float32   `bigquery:"max_voltage"`     // BigQuery: max_voltage
	AverageVoltage             float32   `bigquery:"average_voltage"` // BigQuery: average_voltage
	DeviceEui                  string    `bigquery:"device_eui"`      // BigQuery: device_eui
	ClientId                   string    `bigquery:"client_id"`
	LocationId                 string    `bigquery:"location_id"`
	DeviceCategory             string    `bigquery:"device_category"`
	OriginalMqttTime           time.Time `bigquery:"original_mqtt_time"`
	UpstreamIngestionTimestamp time.Time `bigquery:"upstream_ingestion_timestamp"`
	ProcessedTimestamp         time.Time `bigquery:"processed_timestamp"`
	DeviceType                 string    `bigquery:"device_type"`
}

// Save implements the bigquery.ValueSaver interface.
// The keys of the returned map define the BigQuery column names.
// The types of the values in the map (when Save is called on a zero instance)
// help bigquery.InferSchema determine the BigQuery column types.
func (s *MeterReadingBQWrapper) Save() (row map[string]bigquery.Value, insertID string, err error) {
	// Populate the map from the (potentially zero-valued if just initialized) embedded struct.
	// The keys are snake_case for BigQuery column names.
	row = map[string]bigquery.Value{
		"uid":                          s.Uid,
		"reading":                      float64(s.Reading),
		"average_current":              float64(s.AverageCurrent),
		"max_current":                  float64(s.MaxCurrent),
		"max_voltage":                  float64(s.MaxVoltage),
		"average_voltage":              float64(s.AverageVoltage),
		"device_eui":                   s.DeviceEui,
		"client_id":                    s.ClientId,
		"location_id":                  s.LocationId,
		"device_category":              s.DeviceCategory,
		"device_type":                  s.DeviceType,
		"original_mqtt_time":           s.OriginalMqttTime,           // Initialize to nil, update below if valid
		"upstream_ingestion_timestamp": s.UpstreamIngestionTimestamp, // Initialize to nil, update below if valid
		"processed_timestamp":          s.ProcessedTimestamp,         // Initialize to nil, update below if valid
	}

	return row, "", nil
}

// Compile-time check to ensure MeterReadingBQWrapper implements bigquery.ValueSaver.
var _ bigquery.ValueSaver = &MeterReadingBQWrapper{}
