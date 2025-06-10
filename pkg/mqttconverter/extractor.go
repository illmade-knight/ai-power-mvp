package mqttconverter

// AttributeExtractor inspects a raw message payload and returns a map of
// attributes to be attached to the Pub/Sub message.
// It should return an error if the payload is malformed and attributes
// cannot be extracted.
type AttributeExtractor interface {
	Extract(payload []byte) (attributes map[string]string, err error)
}
