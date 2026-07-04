package forgeext

import "testing"

// Provisioner is nil when the extension is not catalog-backed (no stores /
// no catalog), so adminapi can cheaply detect "not catalog mode".
func TestExtension_Provisioner_NilWithoutCatalog(t *testing.T) {
	e := &Extension{}
	if e.Provisioner() != nil {
		t.Fatal("Provisioner must be nil without opened catalog stores")
	}
}
