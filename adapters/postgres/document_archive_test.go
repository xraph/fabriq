package postgres

import (
	"testing"

	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/fabriqtest"
)

func boolp(b bool) *bool { return &b }

func TestArchiveEnabledResolution(t *testing.T) {
	d := &DocStore{}
	entDefault := &registry.Entity{Spec: registry.EntitySpec{CRDT: &registry.CRDTSpec{}}}
	entOn := &registry.Entity{Spec: registry.EntitySpec{CRDT: &registry.CRDTSpec{ArchiveHistory: boolp(true)}}}
	entOff := &registry.Entity{Spec: registry.EntitySpec{CRDT: &registry.CRDTSpec{ArchiveHistory: boolp(false)}}}

	// No blob configured → never enabled, regardless of flags.
	if d.archiveEnabled(entOn) {
		t.Fatal("archive must be disabled when no blob store is set")
	}

	// Blob set, global default false.
	d.EnableArchive(fabriqtest.NewFakeBlob(), false)
	if d.archiveEnabled(entDefault) {
		t.Fatal("default entity should inherit global false")
	}
	if !d.archiveEnabled(entOn) {
		t.Fatal("per-entity true override should enable")
	}

	// Global default true.
	d.EnableArchive(fabriqtest.NewFakeBlob(), true)
	if !d.archiveEnabled(entDefault) {
		t.Fatal("default entity should inherit global true")
	}
	if d.archiveEnabled(entOff) {
		t.Fatal("per-entity false override should disable")
	}
}
