// fs_node_path_integration_test.go
//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq"
	fabriqerr "github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFsNodePathAndByPath(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	a, _ := f.CreateFolder(tctx, "", "a")
	b, _ := f.CreateFolder(tctx, a.ID, "b")
	fi, _ := f.CreateFile(tctx, b.ID, "f.txt", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})

	p, err := f.NodePath(tctx, fi.ID)
	if err != nil || p != "/a/b/f.txt" {
		t.Fatalf("NodePath = %q err=%v, want /a/b/f.txt", p, err)
	}

	n, err := f.GetNodeByPath(tctx, "/a/b/f.txt")
	if err != nil || n.ID != fi.ID {
		t.Fatalf("GetNodeByPath id=%q err=%v, want %q", n.ID, err, fi.ID)
	}

	if _, err := f.GetNodeByPath(tctx, "/a/nope"); !errors.Is(err, fabriqerr.ErrNotFound) {
		t.Fatalf("missing path err = %v, want ErrNotFound", err)
	}
	if _, err := f.GetNodeByPath(tctx, "relative/path"); err == nil {
		t.Fatal("relative path should be rejected")
	}
}
