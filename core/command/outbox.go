package command

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
)

// newEnvelope builds the single outbox event for an applied command. The
// payload is the column-keyed row after the change ({} for deletes), so
// projections and subscribers always see the table shape.
func newEnvelope(p *preparedCommand, version int64, vals map[string]any, at time.Time, traceparent string) (event.Envelope, error) {
	payload := json.RawMessage("{}")
	if vals != nil {
		raw, err := json.Marshal(vals)
		if err != nil {
			return event.Envelope{}, fmt.Errorf("fabriq: marshal %s payload: %w", p.entity.Spec.Name, err)
		}
		payload = raw
	}
	return event.Envelope{
		ID:                   event.NewID(),
		TenantID:             p.tenantID,
		Aggregate:            p.entity.Spec.Name,
		AggID:                p.aggID,
		Version:              version,
		Type:                 registry.EventType(p.entity.Spec.Name, p.cmd.Op.Verb()),
		At:                   at.UTC(),
		PayloadSchemaVersion: 1,
		Payload:              payload,
		Traceparent:          traceparent,
	}, nil
}
