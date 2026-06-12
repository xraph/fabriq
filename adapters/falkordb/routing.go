package falkordb

import "github.com/xraph/fabriq/core/registry"

// graphForTenant resolves the engine target for a tenant. modelVersion 0
// means the live graph; a non-zero version names a blue-green rebuild
// target (tenant_{id}_v{N}) that projection_state flips to on cutover.
// Both shapes derive from core/registry — this file only chooses which.
func graphForTenant(tenantID string, modelVersion int) string {
	if modelVersion == 0 {
		return registry.GraphName(tenantID)
	}
	return registry.GraphNameVersioned(tenantID, modelVersion)
}
