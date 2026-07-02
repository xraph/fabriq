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

// baseSeedFile describes one file in the base file tree: the root folder it
// lives under (by name), its file name, content type, and body.
type baseSeedFile struct {
	folder      string
	name        string
	contentType string
	body        string
}

// baseSeedFiles is the base set of files seeded under docs/ and images/.
func baseSeedFiles() []baseSeedFile {
	return []baseSeedFile{
		{"docs", "readme.txt", "text/plain", "fabriq admin-demo file plane: this file lives under docs/.\n"},
		{"docs", "notes.md", "text/markdown", "# Notes\n\n- seeded by admin-demo\n- editable via the dashboard\n"},
		{"images", "placeholder.txt", "text/plain", "drop your images here\n"},
	}
}

// seedFileTree idempotently seeds a small file tree for tenant tid: two folders
// ("docs", "images") under root plus a couple of small text files. It is safe to
// re-run on every startup and is SELF-HEALING: it ensures each seed folder and
// file exists AND that each file's bytes are retrievable. The blob/CAS plane can
// be reset independently of the relational catalog (e.g. an ephemeral file://
// bucket), leaving fs_node rows whose bytes are gone — a state that makes
// download return 500 ("object not found in bucket"). When it finds such a
// dangling file it re-stores the bytes via ReplaceFile, so a stale catalog can
// no longer produce download 500s.
//
// Writes go through the file-plane facade (CreateFolder/CreateFile/ReplaceFile)
// under a tenant-stamped context; bytes land in the configured blob/CAS plane.
func seedFileTree(ctx context.Context, f *fabriq.Fabriq, tid string) (folders, files int, err error) {
	tctx, terr := tenant.WithTenant(ctx, tid)
	if terr != nil {
		return 0, 0, fmt.Errorf("admin-demo: file seed tenant %q: %w", tid, terr)
	}

	// Resolve (or create) the seed root folders by name. No blanket early-return
	// on "docs exists": we must still verify each file's bytes so a catalog left
	// dangling by a blob-store reset gets healed.
	roots, lerr := f.ListChildren(tctx, "", 100, 0)
	if lerr != nil {
		return 0, 0, fmt.Errorf("admin-demo: list root for %q: %w", tid, lerr)
	}
	rootID := map[string]string{} // name -> id
	for _, n := range roots {
		rootID[n.Name] = n.ID
	}
	for _, name := range []string{"docs", "images"} {
		if _, ok := rootID[name]; ok {
			continue
		}
		ref, cerr := f.CreateFolder(tctx, "", name)
		if cerr != nil {
			return folders, files, fmt.Errorf("admin-demo: create %s for %q: %w", name, tid, cerr)
		}
		rootID[name] = ref.ID
		folders++
	}

	// Ensure each base file exists with retrievable bytes. Cache each folder's
	// children (name -> node) so we list once per folder.
	childByFolder := map[string]map[string]domain.FsNode{}
	for _, sf := range baseSeedFiles() {
		kids, ok := childByFolder[sf.folder]
		if !ok {
			kids, err = folderChildren(tctx, f, rootID[sf.folder])
			if err != nil {
				return folders, files, fmt.Errorf("admin-demo: list %s for %q: %w", sf.folder, tid, err)
			}
			childByFolder[sf.folder] = kids
		}
		created, eerr := ensureSeededFile(tctx, f, rootID[sf.folder], kids, sf.name, sf.contentType, sf.body)
		if eerr != nil {
			return folders, files, fmt.Errorf("admin-demo: %s/%s for %q: %w", sf.folder, sf.name, tid, eerr)
		}
		if created {
			files++
		}
	}

	return folders, files, nil
}

// folderChildren lists parentID's children into a name -> node map.
func folderChildren(ctx context.Context, f *fabriq.Fabriq, parentID string) (map[string]domain.FsNode, error) {
	list, err := f.ListChildren(ctx, parentID, 200, 0)
	if err != nil {
		return nil, err
	}
	m := make(map[string]domain.FsNode, len(list))
	for _, n := range list {
		m[n.Name] = n
	}
	return m, nil
}

// ensureSeededFile makes sure a single seed file exists under parentID with
// retrievable bytes. existing maps sibling name -> node (from one ListChildren).
// It creates the file when absent; when the node exists but its blob is missing
// (a catalog left dangling by an independent blob-store reset) it re-stores the
// bytes via ReplaceFile. Returns whether a NEW file was created.
func ensureSeededFile(ctx context.Context, f *fabriq.Fabriq, parentID string, existing map[string]domain.FsNode, name, contentType, body string) (bool, error) {
	if node, ok := existing[name]; ok {
		// Node present — verify its bytes are retrievable; heal if dangling.
		if rc, _, gerr := f.GetBlob(ctx, node.BlobID); gerr == nil {
			_ = rc.Close()
			return false, nil // healthy
		}
		if _, rerr := f.ReplaceFile(ctx, node.ID, strings.NewReader(body),
			fabriq.CreateFileOpts{ContentType: contentType}); rerr != nil {
			return false, fmt.Errorf("heal bytes: %w", rerr)
		}
		return false, nil
	}
	if _, cerr := f.CreateFile(ctx, parentID, name, strings.NewReader(body),
		fabriq.CreateFileOpts{ContentType: contentType}); cerr != nil {
		return false, fmt.Errorf("create: %w", cerr)
	}
	return true, nil
}
