package domain

import "github.com/xraph/grove"

// Asset is a piece of industrial equipment. It carries both graph edges:
// LOCATED_AT -> Site and CHILD_OF -> Asset (equipment hierarchy).
type Asset struct {
	grove.BaseModel `grove:"table:assets"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Name     string `grove:"name,notnull" json:"name"`
	Kind     string `grove:"kind" json:"kind"` // pump, valve, motor, ...
	Serial   string `grove:"serial" json:"serial"`
	SiteID   string `grove:"site_id" json:"site_id"`
	ParentID string `grove:"parent_id" json:"parent_id"`
}
