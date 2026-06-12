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
