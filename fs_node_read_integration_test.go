//go:build integration

package fabriq_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/tenant"
)

func TestFsAncestorsDescendantsSearch(t *testing.T) {
	ctx := context.Background()
	f := openFsTestWithCAS(t)
	tctx := tenant.MustWithTenant(ctx, "acme")

	a, _ := f.CreateFolder(tctx, "", "a")
	b, _ := f.CreateFolder(tctx, a.ID, "b")
	c, _ := f.CreateFile(tctx, b.ID, "report.pdf", bytes.NewReader([]byte("x")), fabriq.CreateFileOpts{})

	anc, err := f.Ancestors(tctx, c.ID)
	if err != nil {
		t.Fatalf("Ancestors: %v", err)
	}
	if len(anc) != 2 || anc[0].Name != "a" || anc[1].Name != "b" {
		t.Fatalf("ancestors = %+v, want [a b]", anc)
	}

	desc, err := f.Descendants(tctx, a.ID)
	if err != nil {
		t.Fatalf("Descendants: %v", err)
	}
	if len(desc) != 2 { // b + report.pdf
		t.Fatalf("descendants = %d, want 2", len(desc))
	}

	hits, err := f.SearchNodesByName(tctx, "report", 50)
	if err != nil {
		t.Fatalf("SearchNodesByName: %v", err)
	}
	if len(hits) != 1 || hits[0].Name != "report.pdf" {
		t.Fatalf("search hits = %+v", hits)
	}
}
