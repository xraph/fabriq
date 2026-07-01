package adminapi

import (
	"net/http"

	"github.com/xraph/forge"
)

// migrationItem mirrors one migration's status in camelCase.
type migrationItem struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Group     string `json:"group"`
	Comment   string `json:"comment"`
	Applied   bool   `json:"applied"`
	AppliedAt string `json:"appliedAt,omitempty"`
}

// migrationGroup is one migration group's applied/pending status.
type migrationGroup struct {
	Name    string          `json:"name"`
	Applied []migrationItem `json:"applied"`
	Pending []migrationItem `json:"pending"`
}

// migrationStatusResponse is the payload for GET {BasePath}/migrations.
type migrationStatusResponse struct {
	Groups []migrationGroup `json:"groups"`
}

// registerMigrationRoutes wires the read-only migration status route. This
// endpoint is always available (no WithSchemaAdmin gate) — it only reports
// state, it never executes migrations.
func (c *adminController) registerMigrationRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	opts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.migrations.status"),
		forge.WithSummary("List applied + pending migrations"),
		forge.WithTags("Fabriq", "Admin"),
	}, c.ext.cfg.RouteOptions...)
	return r.GET(base+"/migrations", c.handleMigrationStatus, opts...)
}

// handleMigrationStatus serves GET {BasePath}/migrations. Returns 501 when
// there is no parent forgeext.Extension to resolve a migration target from
// (e.g. the fake-backed test harness) or when the parent itself fails to
// report status (no DSN/grove.DB configured).
func (c *adminController) handleMigrationStatus(ctx forge.Context) error {
	if c.ext.parent == nil {
		return ctx.JSON(http.StatusNotImplemented, map[string]string{
			"error": "migration status unavailable: no parent fabriq extension configured",
		})
	}
	groups, err := c.ext.parent.MigrationStatus(ctx.Request().Context())
	if err != nil {
		// No migration target on the fake/unconfigured backend → 501.
		return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": err.Error()})
	}
	out := migrationStatusResponse{Groups: make([]migrationGroup, 0, len(groups))}
	for _, g := range groups {
		mg := migrationGroup{
			Name:    g.Name,
			Applied: make([]migrationItem, 0, len(g.Applied)),
			Pending: make([]migrationItem, 0, len(g.Pending)),
		}
		for _, m := range g.Applied {
			mg.Applied = append(mg.Applied, migrationInfoTo(m))
		}
		for _, m := range g.Pending {
			mg.Pending = append(mg.Pending, migrationInfoTo(m))
		}
		out.Groups = append(out.Groups, mg)
	}
	return ctx.JSON(http.StatusOK, out)
}

// migrationInfoTo converts a forge.MigrationInfo into the camelCase wire type.
func migrationInfoTo(m *forge.MigrationInfo) migrationItem {
	return migrationItem{
		Name:      m.Name,
		Version:   m.Version,
		Group:     m.Group,
		Comment:   m.Comment,
		Applied:   m.Applied,
		AppliedAt: m.AppliedAt,
	}
}
