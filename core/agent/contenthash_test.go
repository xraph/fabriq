package agent

import "testing"

func TestRollupContentHash_OrderIndependent(t *testing.T) {
	a := RollupContentHash("v1", []string{"h1", "h2", "h3"})
	b := RollupContentHash("v1", []string{"h3", "h1", "h2"})
	if a != b {
		t.Fatal("rollup hash must be independent of child order")
	}
}

func TestRollupContentHash_ChildChangeChangesRoot(t *testing.T) {
	a := RollupContentHash("v1", []string{"h1", "h2"})
	b := RollupContentHash("v1", []string{"h1", "h2x"})
	if a == b {
		t.Fatal("a changed child must change the root hash")
	}
}

func TestContentHash_RecipeVersionSalt(t *testing.T) {
	if RollupContentHash("v1", []string{"h1"}) == RollupContentHash("v2", []string{"h1"}) {
		t.Fatal("recipeVersion must salt the hash")
	}
	if L0ContentHash("v1", "sfh") == L0ContentHash("v2", "sfh") {
		t.Fatal("recipeVersion must salt the L0 hash")
	}
}
