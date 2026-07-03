//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
	"github.com/xraph/fabriq/fabriqtest"
	"github.com/xraph/fabriq/migrations"
)

// openFsTestWithCAS boots Postgres, migrates, and opens fabriq with a
// local file-based CAS — required by CreateFile → PutBlob.
func openFsTestWithCAS(t *testing.T) *fabriq.Fabriq {
	t.Helper()
	ctx := context.Background()
	superDSN := fabriqtest.StartPostgres(t)
	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	owner, err := postgres.Open(ctx, superDSN, reg)
	if err != nil {
		t.Fatalf("postgres.Open (owner): %v", err)
	}
	orch, orchErr := migrations.NewOrchestrator(owner.Driver())
	if orchErr != nil {
		t.Fatal(orchErr)
	}
	if _, err := orch.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = owner.Close()
	fabriqtest.ApplyDDL(t, superDSN, domain.DemoDDL())
	appDSN := fabriqtest.CreateAppRole(t, superDSN)
	f, stores, openErr := fabriq.Open(ctx, reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: appDSN},
		Storage: fabriq.StorageConfig{
			StorageDriver: "file://" + t.TempDir(),
			DefaultBucket: "primary",
			EnableCas:     true,
		},
	})
	if openErr != nil {
		t.Fatalf("fabriq.Open: %v", openErr)
	}
	t.Cleanup(func() { _ = stores.Close() })
	return f
}

func TestFsCreateAndRead(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	root, err := f.CreateFolder(tctx, "", "docs")
	if err != nil {
		t.Fatalf("CreateFolder root: %v", err)
	}
	if root.Path != "/docs" || root.NodeType != "folder" {
		t.Fatalf("root ref = %+v", root)
	}

	sub, err := f.CreateFolder(tctx, root.ID, "reports")
	if err != nil {
		t.Fatalf("CreateFolder sub: %v", err)
	}
	if sub.Path != "/docs/reports" {
		t.Fatalf("sub path = %q", sub.Path)
	}

	file, err := f.CreateFile(tctx, sub.ID, "q3.txt", bytes.NewReader([]byte("hello")), fabriq.CreateFileOpts{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if file.Path != "/docs/reports/q3.txt" || file.NodeType != "file" {
		t.Fatalf("file ref = %+v", file)
	}

	// GetNodeByPath resolves the file; facets are denormalized.
	got, err := f.GetNodeByPath(tctx, "/docs/reports/q3.txt")
	if err != nil {
		t.Fatalf("GetNodeByPath: %v", err)
	}
	if got.Size != 5 || got.ContentType != "text/plain" || got.BlobID == "" || got.Checksum == "" {
		t.Fatalf("denormalized facets wrong: %+v", got)
	}

	// ListChildren of /docs returns reports.
	kids, err := f.ListChildren(tctx, root.ID, 50, 0)
	if err != nil {
		t.Fatalf("ListChildren: %v", err)
	}
	if len(kids) != 1 || kids[0].Name != "reports" {
		t.Fatalf("children = %+v", kids)
	}

	// Sibling-name collision is rejected.
	if _, err := f.CreateFolder(tctx, root.ID, "reports"); !errors.Is(err, fabriq.ErrNodeNameConflict) {
		t.Fatalf("dup sibling = %v, want ErrNodeNameConflict", err)
	}

	// Cannot create under a file.
	if _, err := f.CreateFolder(tctx, file.ID, "nope"); !errors.Is(err, fabriq.ErrNotContainer) {
		t.Fatalf("child-under-file = %v, want ErrNotContainer", err)
	}
}

// TestFsNodeNameValidation verifies the facade rejects unaddressable node
// names (empty, containing "/", "." and "..") against the real command plane:
// creates and renames fail with a structured invalid_input error and nothing
// is written.
func TestFsNodeNameValidation(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	wantInvalid := func(op string, err error) {
		t.Helper()
		var fe *fabriqerr.Error
		if err == nil || !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
			t.Fatalf("%s: got %v, want *fabriqerr.Error with CodeInvalidInput", op, err)
		}
	}

	for _, name := range []string{"", "a/b", ".", ".."} {
		_, err := f.CreateFolder(tctx, "", name)
		wantInvalid("CreateFolder("+name+")", err)
		_, err = f.CreateFile(tctx, "", name, bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})
		wantInvalid("CreateFile("+name+")", err)
		_, err = f.CreateMount(tctx, "", name, nil)
		wantInvalid("CreateMount("+name+")", err)
	}

	// Nothing leaked into the root from the rejected creates.
	kids, err := f.ListChildren(tctx, "", 100, 0)
	if err != nil {
		t.Fatalf("ListChildren: %v", err)
	}
	if len(kids) != 0 {
		t.Fatalf("rejected creates leaked nodes: %+v", kids)
	}

	folder, err := f.CreateFolder(tctx, "", "ok")
	if err != nil {
		t.Fatalf("CreateFolder(ok): %v", err)
	}
	_, err = f.RenameNode(tctx, folder.ID, "a/b")
	wantInvalid("RenameNode(->a/b)", err)

	// The rejected rename left the node addressable by its original path.
	if _, err := f.GetNodeByPath(tctx, "/ok"); err != nil {
		t.Fatalf("GetNodeByPath(/ok) after rejected rename: %v", err)
	}
}
