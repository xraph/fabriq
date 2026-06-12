package event

import (
	"encoding/json"
	"fmt"
)

// Encode serializes an envelope after validating it.
func Encode(env Envelope) ([]byte, error) {
	if err := env.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(env)
}

// Decode parses and validates an envelope. Callers that own an upcaster
// chain should pass the result through UpcasterChain.Apply before handing
// the payload to appliers.
func Decode(raw []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Envelope{}, fmt.Errorf("fabriq: malformed event envelope: %w", err)
	}
	if err := env.validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func (e Envelope) validate() error {
	switch {
	case e.ID == "":
		return fmt.Errorf("fabriq: event envelope missing id")
	case e.TenantID == "":
		return fmt.Errorf("fabriq: event envelope missing tenant_id")
	case e.Aggregate == "":
		return fmt.Errorf("fabriq: event envelope missing aggregate")
	case e.AggID == "":
		return fmt.Errorf("fabriq: event envelope missing agg_id")
	case e.Version < 1:
		return fmt.Errorf("fabriq: event envelope version %d < 1", e.Version)
	case e.Type == "":
		return fmt.Errorf("fabriq: event envelope missing type")
	case e.PayloadSchemaVersion < 1:
		return fmt.Errorf("fabriq: event envelope payload_schema_version %d < 1", e.PayloadSchemaVersion)
	}
	return nil
}
