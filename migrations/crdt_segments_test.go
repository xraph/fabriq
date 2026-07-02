package migrations_test

import (
	"testing"

	"github.com/xraph/fabriq/migrations"
)

// TestMigration0026Registered proves the segments migration is in the group so
// `fabriq migrate up` creates the table. (Applying against a live DB is covered
// by the CRDT offload integration tests.)
func TestMigration0026Registered(t *testing.T) {
	found := false
	for _, m := range migrations.Group().Migrations() {
		if m.Name == "crdt_segments" {
			found = true
		}
	}
	if !found {
		t.Fatal("crdt_segments migration not registered")
	}
}
