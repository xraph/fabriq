package fabriq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

func TestValidateNodeName(t *testing.T) {
	valid := []string{"a", "a.txt", ".hidden", "...", "a b", "üñïçødé", "a..b"}
	for _, name := range valid {
		if err := validateNodeName(name); err != nil {
			t.Errorf("validateNodeName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{"", "/", "a/b", "/a", "a/", ".", ".."}
	for _, name := range invalid {
		err := validateNodeName(name)
		if err == nil {
			t.Errorf("validateNodeName(%q) = nil, want invalid_input error", name)
			continue
		}
		var fe *fabriqerr.Error
		if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
			t.Errorf("validateNodeName(%q) = %v, want *fabriqerr.Error with CodeInvalidInput", name, err)
		}
	}
}

// wantInvalidName asserts err is a structured invalid_input error.
func wantInvalidName(t *testing.T, op string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: got nil error, want invalid_input", op)
	}
	var fe *fabriqerr.Error
	if !errors.As(err, &fe) || fe.Code != fabriqerr.CodeInvalidInput {
		t.Fatalf("%s: got %v, want *fabriqerr.Error with CodeInvalidInput", op, err)
	}
}

// TestFsNodeNameValidation exercises every facade entry point that accepts a
// node name against the in-memory fake: bad names must be rejected with a
// structured invalid_input error before any write happens, so no node with an
// unaddressable name (empty, containing "/", "." or "..") can enter the tree.
func TestFsNodeNameValidation(t *testing.T) {
	f := newFakeFabriq(t)
	ctx, err := tenant.WithTenant(context.Background(), "acme")
	if err != nil {
		t.Fatalf("WithTenant: %v", err)
	}

	bad := []string{"", "a/b", ".", ".."}

	for _, name := range bad {
		_, cerr := f.CreateFolder(ctx, "", name)
		wantInvalidName(t, "CreateFolder("+name+")", cerr)

		_, cerr = f.CreateFile(ctx, "", name, strings.NewReader("x"), CreateFileOpts{})
		wantInvalidName(t, "CreateFile("+name+")", cerr)

		_, cerr = f.CreateMount(ctx, "", name, nil)
		wantInvalidName(t, "CreateMount("+name+")", cerr)
	}

	// No node may have been created by the rejected calls.
	kids, err := f.ListChildren(ctx, "", 100, 0)
	if err != nil {
		t.Fatalf("ListChildren: %v", err)
	}
	if len(kids) != 0 {
		t.Fatalf("rejected creates leaked nodes: %+v", kids)
	}

	folder, err := f.CreateFolder(ctx, "", "ok")
	if err != nil {
		t.Fatalf("CreateFolder(ok): %v", err)
	}
	for _, name := range bad {
		_, rerr := f.RenameNode(ctx, folder.ID, name)
		wantInvalidName(t, "RenameNode(->"+name+")", rerr)
	}
	// The node is untouched by the rejected renames.
	n, err := f.GetNode(ctx, folder.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.Name != "ok" {
		t.Fatalf("rejected rename mutated node: name = %q, want %q", n.Name, "ok")
	}

	// MoveNode validates the moved node's (pre-existing) name defensively:
	// a legacy row with a bad name — created here by bypassing the facade —
	// must be renamed before it can be re-parented.
	now := time.Now().UTC()
	legacy := &domain.FsNode{
		ParentID: "", Name: "a/b", NodeType: "folder",
		Metadata: map[string]any{}, MountConfig: map[string]any{}, CreatedAt: now, UpdatedAt: now,
	}
	res, err := f.exec.Exec(ctx, command.Command{Entity: "fs_node", Op: command.OpCreate, Payload: legacy})
	if err != nil {
		t.Fatalf("raw create of legacy bad-named node: %v", err)
	}
	_, merr := f.MoveNode(ctx, res.AggID, folder.ID)
	wantInvalidName(t, "MoveNode(legacy bad name)", merr)

	// Rescue path: RenameNode to a valid name still works on the legacy row,
	// and the node can then be moved.
	if _, err := f.RenameNode(ctx, res.AggID, "fixed"); err != nil {
		t.Fatalf("RenameNode(legacy -> fixed): %v", err)
	}
	if _, err := f.MoveNode(ctx, res.AggID, folder.ID); err != nil {
		t.Fatalf("MoveNode after rescue rename: %v", err)
	}
}
