package forgeext_test

import (
	"testing"

	"github.com/xraph/confy"

	"github.com/xraph/fabriq/forgeext"
)

func TestLoadConfig_Storage(t *testing.T) {
	cm := confy.NewTestConfyImplWithData(map[string]any{
		"storage": map[string]any{
			"storageDriver": "file:///tmp/x",
			"defaultBucket": "primary",
		},
	})
	got := forgeext.LoadConfig(cm, "")
	if got.Storage.StorageDriver != "file:///tmp/x" {
		t.Fatalf("storageDriver = %q", got.Storage.StorageDriver)
	}
	if got.Storage.DefaultBucket != "primary" {
		t.Fatalf("defaultBucket = %q", got.Storage.DefaultBucket)
	}
}
