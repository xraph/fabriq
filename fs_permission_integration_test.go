//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq/core/tenant"
)

func TestFsPermissionGrantListRevokeCascade(t *testing.T) {
	ctx := context.Background()
	f, _, _ := openFsTest(t) // Phase-5 helper (Postgres only)
	tctx := tenant.MustWithTenant(ctx, "acme")

	dir, err := f.CreateFolder(tctx, "", "shared")
	if err != nil {
		t.Fatalf("CreateFolder: %v", err)
	}

	pid, err := f.GrantPermission(tctx, dir.ID, "user", "u-1", "write", "admin-1")
	if err != nil {
		t.Fatalf("GrantPermission: %v", err)
	}

	perms, err := f.ListNodePermissions(tctx, dir.ID)
	if err != nil || len(perms) != 1 || perms[0].Permission != "write" || perms[0].PrincipalID != "u-1" {
		t.Fatalf("ListNodePermissions = %+v, err %v", perms, err)
	}
	byPrincipal, err := f.ListPrincipalPermissions(tctx, "user", "u-1")
	if err != nil || len(byPrincipal) != 1 {
		t.Fatalf("ListPrincipalPermissions = %+v, err %v", byPrincipal, err)
	}

	if err := f.RevokePermission(tctx, pid); err != nil {
		t.Fatalf("RevokePermission: %v", err)
	}
	if perms, _ := f.ListNodePermissions(tctx, dir.ID); len(perms) != 0 {
		t.Fatalf("after revoke still %d perms", len(perms))
	}

	// Cascade: permanent-delete of the node removes its permissions.
	dir2, _ := f.CreateFolder(tctx, "", "shared2")
	_, _ = f.GrantPermission(tctx, dir2.ID, "user", "u-2", "read", "admin-1")
	if err := f.PermanentDeleteNode(tctx, dir2.ID); err != nil {
		t.Fatalf("PermanentDeleteNode: %v", err)
	}
	if perms, _ := f.ListNodePermissions(tctx, dir2.ID); len(perms) != 0 {
		t.Fatalf("FK cascade failed: %d perms remain", len(perms))
	}
}
