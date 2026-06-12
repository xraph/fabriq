package event

import (
	"encoding/json"
	"fmt"
)

// Upcaster migrates one event type's payload from FromVersion to
// FromVersion+1. Upcasters are pure functions; appliers only ever see the
// latest shape.
type Upcaster struct {
	Type        string
	FromVersion int
	Fn          func(json.RawMessage) (json.RawMessage, error)
}

// UpcasterChain holds ordered upcasters per event type and applies them at
// decode until no upcaster matches the current version.
type UpcasterChain struct {
	steps map[string]map[int]Upcaster // type -> fromVersion -> upcaster
}

// NewUpcasterChain returns an empty chain (Apply is then a passthrough).
func NewUpcasterChain() *UpcasterChain {
	return &UpcasterChain{steps: make(map[string]map[int]Upcaster)}
}

// Register adds an upcaster; one per (type, fromVersion).
func (c *UpcasterChain) Register(u Upcaster) error {
	if u.Type == "" || u.FromVersion < 1 {
		return fmt.Errorf("fabriq: upcaster needs a type and FromVersion >= 1, got %q/%d", u.Type, u.FromVersion)
	}
	if u.Fn == nil {
		return fmt.Errorf("fabriq: upcaster %s v%d has nil Fn", u.Type, u.FromVersion)
	}
	byVersion, ok := c.steps[u.Type]
	if !ok {
		byVersion = make(map[int]Upcaster)
		c.steps[u.Type] = byVersion
	}
	if _, dup := byVersion[u.FromVersion]; dup {
		return fmt.Errorf("fabriq: duplicate upcaster for %s v%d", u.Type, u.FromVersion)
	}
	byVersion[u.FromVersion] = u
	return nil
}

// MustRegister is Register that panics; for static wiring.
func (c *UpcasterChain) MustRegister(u Upcaster) {
	if err := c.Register(u); err != nil {
		panic(err)
	}
}

// Apply walks the chain from the envelope's PayloadSchemaVersion upward,
// returning the envelope at the latest known shape.
func (c *UpcasterChain) Apply(env Envelope) (Envelope, error) {
	byVersion := c.steps[env.Type]
	for {
		up, ok := byVersion[env.PayloadSchemaVersion]
		if !ok {
			return env, nil
		}
		migrated, err := up.Fn(env.Payload)
		if err != nil {
			return Envelope{}, fmt.Errorf("fabriq: upcast %s v%d: %w", env.Type, env.PayloadSchemaVersion, err)
		}
		env.Payload = migrated
		env.PayloadSchemaVersion++
	}
}
