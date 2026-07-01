package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// blobObjectEntity / fsNodeEntity are the core file-plane entity type names the
// command plane writes through. They are registered minimally (Model only) so
// the demo's command executor knows their shape without pulling in the rest of
// the example domain pack (sites/assets/tags), which would require additional
// search indices and graph nodes the demo does not seed.
const (
	blobObjectEntity = "blob_object"
	fsNodeEntity     = "fs_node"
)

// fileSeedSpecs returns the minimal registry specs for the file plane:
// blob_object (the byte-plane catalog row CreateFile writes) and fs_node (the
// tree node). Models come from fabriq's domain package so the physical columns
// match the fabriq migrations that create the fs_nodes / blob_objects tables.
//
// Unlike domain.RegisterAll, these omit GraphNode/Search/Live specs: the demo
// runs no projection worker for the file plane, so the command-plane shape is
// all that is needed, and omitting the projections keeps the demo from
// requiring an fs_nodes ES index or an FsNode graph label.
func fileSeedSpecs() []registry.EntitySpec {
	return []registry.EntitySpec{
		{
			Name:      blobObjectEntity,
			Kind:      registry.KindAggregate,
			Model:     (*domain.BlobObject)(nil),
			Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		},
		{
			Name:      fsNodeEntity,
			Kind:      registry.KindAggregate,
			Model:     (*domain.FsNode)(nil),
			Subscribe: []registry.Scope{registry.ByID, registry.ByTenant},
		},
	}
}

// seedFileTree idempotently seeds a small file tree for tenant tid: two folders
// ("docs", "images") under root plus a couple of small text files. It is safe
// to re-run on every startup — it first lists the tenant's root children and
// skips entirely when the tree is already present.
//
// Writes go through the file-plane facade (CreateFolder/CreateFile) under a
// tenant-stamped context; bytes land in the configured blob/CAS plane.
func seedFileTree(ctx context.Context, f *fabriq.Fabriq, tid string) (folders, files int, err error) {
	tctx, terr := tenant.WithTenant(ctx, tid)
	if terr != nil {
		return 0, 0, fmt.Errorf("admin-demo: file seed tenant %q: %w", tid, terr)
	}

	// Idempotency: if root already has any of our seed folders, assume seeded.
	roots, lerr := f.ListChildren(tctx, "", 100, 0)
	if lerr != nil {
		return 0, 0, fmt.Errorf("admin-demo: list root for %q: %w", tid, lerr)
	}
	existing := map[string]string{} // name -> id
	for _, n := range roots {
		existing[n.Name] = n.ID
	}
	if _, ok := existing["docs"]; ok {
		return 0, 0, nil // already seeded
	}

	// docs/ with two text files.
	docsRef, cerr := f.CreateFolder(tctx, "", "docs")
	if cerr != nil {
		return 0, 0, fmt.Errorf("admin-demo: create docs for %q: %w", tid, cerr)
	}
	folders++
	if _, cerr := f.CreateFile(tctx, docsRef.ID, "readme.txt",
		strings.NewReader("fabriq admin-demo file plane: this file lives under docs/.\n"),
		fabriq.CreateFileOpts{ContentType: "text/plain"}); cerr != nil {
		return folders, files, fmt.Errorf("admin-demo: create readme.txt for %q: %w", tid, cerr)
	}
	files++
	if _, cerr := f.CreateFile(tctx, docsRef.ID, "notes.md",
		strings.NewReader("# Notes\n\n- seeded by admin-demo\n- editable via the dashboard\n"),
		fabriq.CreateFileOpts{ContentType: "text/markdown"}); cerr != nil {
		return folders, files, fmt.Errorf("admin-demo: create notes.md for %q: %w", tid, cerr)
	}
	files++

	// images/ (empty folder, plus a tiny placeholder text file so the browser
	// shows a file in it too).
	imagesRef, cerr := f.CreateFolder(tctx, "", "images")
	if cerr != nil {
		return folders, files, fmt.Errorf("admin-demo: create images for %q: %w", tid, cerr)
	}
	folders++
	if _, cerr := f.CreateFile(tctx, imagesRef.ID, "placeholder.txt",
		strings.NewReader("drop your images here\n"),
		fabriq.CreateFileOpts{ContentType: "text/plain"}); cerr != nil {
		return folders, files, fmt.Errorf("admin-demo: create placeholder.txt for %q: %w", tid, cerr)
	}
	files++

	return folders, files, nil
}
