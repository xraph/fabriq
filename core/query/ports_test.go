package query_test

import (
	"encoding/json"
	"time"

	"github.com/xraph/fabriq/core/event"
)

func testEnvelope() event.Envelope {
	return event.Envelope{
		ID: event.NewID(), TenantID: "acme", Aggregate: "asset", AggID: "A1",
		Version: 4, Type: "asset.updated", At: time.Now().UTC(),
		PayloadSchemaVersion: 1, Payload: json.RawMessage(`{"id":"A1"}`),
	}
}
