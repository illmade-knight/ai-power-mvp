package icestore

import (
	"fmt"
	"time"

	"github.com/illmade-knight/ai-power-mpv/pkg/types"
)

func ArchivalTransformer(msg types.ConsumedMessage) (*ArchivalData, bool, error) {
	ts := msg.PublishTime
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	batchKey := fmt.Sprintf("%d/%02d/%02d", ts.Year(), ts.Month(), ts.Day())

	//if the msg has been enriched in the pipeline we'll use its location in teh batchKey
	if msg.DeviceInfo != nil {
		batchKey = fmt.Sprintf("%s/%s", batchKey, msg.DeviceInfo.Location)
	}

	return &ArchivalData{
		ID:                    msg.ID,
		BatchKey:              batchKey,
		OriginalPubSubPayload: msg.Payload,
		ArchivedAt:            time.Now().UTC(),
	}, false, nil
}
