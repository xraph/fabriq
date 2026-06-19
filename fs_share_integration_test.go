//go:build integration

package fabriq_test

import (
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFsShareDataCRUD(t *testing.T) {
	ctx := context.Background()
	f, _, _ := openFsTest(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	node, _ := f.CreateFolder(tctx, "", "shared")

	id, err := f.CreateShare(tctx, fabriq.CreateShareInput{
		NodeID: node.ID, Token: "tok-abc", Permission: "read", PasswordHash: "$2a$hash", CreatedBy: "u-1",
	})
	if err != nil {
		t.Fatalf("CreateShare: %v", err)
	}

	got, err := f.GetShareByToken(tctx, "tok-abc")
	if err != nil || got.ID != id || got.PasswordHash != "$2a$hash" {
		t.Fatalf("GetShareByToken = %+v, err %v", got, err)
	}

	if err := f.IncrementShareDownload(tctx, id); err != nil {
		t.Fatalf("IncrementShareDownload: %v", err)
	}
	if err := f.IncrementShareDownload(tctx, id); err != nil {
		t.Fatalf("IncrementShareDownload 2: %v", err)
	}
	after, _ := f.GetShareByToken(tctx, "tok-abc")
	if after.DownloadCount != 2 {
		t.Fatalf("download_count = %d, want 2", after.DownloadCount)
	}

	shares, _ := f.ListNodeShares(tctx, node.ID)
	if len(shares) != 1 {
		t.Fatalf("ListNodeShares = %d", len(shares))
	}

	if err := f.DeleteShare(tctx, id); err != nil {
		t.Fatalf("DeleteShare: %v", err)
	}
	if _, err := f.GetShareByToken(tctx, "tok-abc"); err == nil {
		t.Fatal("share still resolvable after delete")
	}
}
