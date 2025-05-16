package ingestion

// DeviceMetadataFetcher is a function type for fetching device-specific metadata.
type DeviceMetadataFetcher func(deviceEUI string) (clientID, locationID, category string, err error)
