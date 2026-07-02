package main

import (
	"context"
	"fmt"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
	"github.com/xraph/fabriq/domain"
)

// extraFile describes one additional seed file: the folder it lives under (by
// name, resolved under root), its file name, content type, and body. These give
// the file browser real, downloadable content beyond the original readme/notes.
type extraFile struct {
	folder      string
	name        string
	contentType string
	body        string
}

// extraFiles is the per-tenant set of additional files seeded under the existing
// docs/ and images/ folders. Bodies are real text so download returns content.
func extraFiles(tid string) []extraFile {
	return []extraFile{
		{
			folder:      "docs",
			name:        "changelog.md",
			contentType: "text/markdown",
			body: "# Changelog\n\n" +
				"## 0.2.0 — " + tid + "\n" +
				"- Added customer and order entities\n" +
				"- Wired the e-commerce graph (PLACED / CONTAINS / LIVES_IN)\n" +
				"- Indexed customers + orders into search and vector\n\n" +
				"## 0.1.0\n" +
				"- Initial product catalogue + file plane + CRDT docs\n",
		},
		{
			folder:      "docs",
			name:        "api-notes.txt",
			contentType: "text/plain",
			body: "fabriq admin API notes (" + tid + ")\n\n" +
				"GET  /admin/entities/types          list entity types\n" +
				"GET  /admin/entities?type=customer  list customers\n" +
				"GET  /admin/search?type=order&q=..  full-text search orders\n" +
				"POST /admin/graph/traverse          traverse from a node\n" +
				"GET  /admin/crdt/page/about         read a CRDT document\n",
		},
		{
			folder:      "images",
			name:        "diagram.txt",
			contentType: "text/plain",
			body: "ASCII placeholder diagram (" + tid + ")\n\n" +
				"  [Customer] --PLACED--> [Order] --CONTAINS--> [Product]\n" +
				"      |                                            |\n" +
				"   LIVES_IN                                   IN_CATEGORY\n" +
				"      v                                            v\n" +
				"  [Country]                                   [Category]\n",
		},
	}
}

// seedExtraFiles idempotently adds the extraFiles for tenant tid under the
// folders the original seedFileTree created (docs/ and images/). It resolves
// each target folder by name under root, lists that folder's children, and only
// creates a file whose name is not already present — so it is safe to re-run on
// every startup and composes cleanly with the original seed. Returns the number
// of files created this run.
func seedExtraFiles(ctx context.Context, f *fabriq.Fabriq, tid string) (int, error) {
	tctx, err := tenant.WithTenant(ctx, tid)
	if err != nil {
		return 0, fmt.Errorf("admin-demo: extra files tenant %q: %w", tid, err)
	}

	// Resolve root folders by name -> id.
	roots, lerr := f.ListChildren(tctx, "", 100, 0)
	if lerr != nil {
		return 0, fmt.Errorf("admin-demo: list root for extra files %q: %w", tid, lerr)
	}
	folderID := map[string]string{}
	for _, n := range roots {
		folderID[n.Name] = n.ID
	}

	created := 0
	// Cache each folder's existing children (name -> node) so we probe once per
	// folder. Uses the same create-or-heal path as seedFileTree so a blob-store
	// reset that leaves these catalog rows dangling is repaired on restart too.
	childByFolder := map[string]map[string]domain.FsNode{}

	for _, ef := range extraFiles(tid) {
		parentID, ok := folderID[ef.folder]
		if !ok {
			continue // base folder not seeded yet (seedFileTree runs first)
		}
		kids, ok := childByFolder[ef.folder]
		if !ok {
			kids, err = folderChildren(tctx, f, parentID)
			if err != nil {
				return created, fmt.Errorf("admin-demo: list %s for extra files %q: %w", ef.folder, tid, err)
			}
			childByFolder[ef.folder] = kids
		}
		wasCreated, eerr := ensureSeededFile(tctx, f, parentID, kids, ef.name, ef.contentType, ef.body)
		if eerr != nil {
			return created, fmt.Errorf("admin-demo: %s/%s for %q: %w", ef.folder, ef.name, tid, eerr)
		}
		if wasCreated {
			created++
		}
	}
	return created, nil
}
