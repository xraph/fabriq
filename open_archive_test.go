package fabriq

import (
	"strings"
	"testing"

	"github.com/xraph/fabriq/core/registry"
)

func TestValidateArchiveConfig(t *testing.T) {
	tp := func(b bool) *bool { return &b }

	// Global archive on, no storage → error.
	cfg := Config{}
	cfg.Documents.ArchiveHistory = true
	if err := validateArchiveConfig(cfg, registry.New()); err == nil || !strings.Contains(err.Error(), "storage") {
		t.Fatalf("want storage error, got %v", err)
	}

	// Global archive on, storage configured → ok.
	cfg.Storage.StorageDriver = "file://tmp"
	if err := validateArchiveConfig(cfg, registry.New()); err != nil {
		t.Fatalf("configured storage should pass, got %v", err)
	}

	// Per-entity archive on, no storage → error.
	reg := registry.New()
	reg.MustRegister(registry.EntitySpec{
		Name: "page", Kind: registry.KindDocument, Schema: &registry.DynamicSchema{Table: "pages"},
		CRDT: &registry.CRDTSpec{Engine: "grove-crdt", ArchiveHistory: tp(true)},
	})
	if err := validateArchiveConfig(Config{}, reg); err == nil {
		t.Fatal("per-entity archive without storage must error")
	}

	// Nothing requests archive → ok even without storage.
	if err := validateArchiveConfig(Config{}, registry.New()); err != nil {
		t.Fatalf("no archive requested should pass, got %v", err)
	}
}
