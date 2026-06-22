package domain

import "github.com/xraph/grove"

// Page is the KindDocument demo entity: a collaborative page-builder
// document. Its row is written ONLY by document-plane materialization
// (the command plane rejects document kinds); after the quiet window the
// merged CRDT state lands here with version++ and one ordinary
// page.updated event, so projections, search and audit see pages as
// normal entities.
type Page struct {
	grove.BaseModel `grove:"table:pages"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Title    string `grove:"title" json:"title"`
	Body     string `grove:"body" json:"body"`
}

// PagesDDL returns the DDL that creates the demo "pages" materialization target
// (table + tenant index + tenant-isolation RLS). It is deliberately NOT part of
// fabriq's shipped migration chain: materialization targets are application
// entities, and a library must never create a generically-named "pages" table
// in a host database (it would collide with the app's own table). Examples and
// the document-plane integration tests apply this as the table owner before
// provisioning the app role. The statements mirror fabriq's standard
// tenant-isolation pattern (ENABLE+FORCE RLS, USING/WITH CHECK on app.tenant_id).
func PagesDDL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS pages (
			id        TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			version   BIGINT NOT NULL,
			title     TEXT NOT NULL DEFAULT '',
			body      TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS pages_tenant_idx ON pages (tenant_id)`,
		`ALTER TABLE pages ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE pages FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS tenant_isolation ON pages`,
		`CREATE POLICY tenant_isolation ON pages
			USING (tenant_id = current_setting('app.tenant_id', true))
			WITH CHECK (tenant_id = current_setting('app.tenant_id', true))`,
	}
}
