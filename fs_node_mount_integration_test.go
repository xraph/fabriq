//go:build integration

package fabriq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFsMountCreateResolve(t *testing.T) {
	ctx := context.Background()
	f, _, _ := openFsTest(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	parent, err := f.CreateFolder(tctx, "", "mounts")
	if err != nil {
		t.Fatalf("CreateFolder: %v", err)
	}
	cfg := map[string]any{"provider": "blob_source", "sourceId": "src-1", "readOnly": true}

	ref, err := f.CreateMount(tctx, parent.ID, "s3-mount", cfg)
	if err != nil {
		t.Fatalf("CreateMount: %v", err)
	}
	if ref.NodeType != "mount" {
		t.Fatalf("node_type = %q, want mount", ref.NodeType)
	}

	got, err := f.ResolveMount(tctx, ref.ID)
	if err != nil {
		t.Fatalf("ResolveMount: %v", err)
	}
	if got["sourceId"] != "src-1" || got["readOnly"] != true {
		t.Fatalf("mount config wrong: %+v", got)
	}

	// A mount is not a container: creating a child under it is rejected.
	if _, err := f.CreateMount(tctx, ref.ID, "nested", nil); !errors.Is(err, fabriq.ErrNotContainer) {
		t.Fatalf("CreateMount under a non-folder = %v, want ErrNotContainer", err)
	}
}
