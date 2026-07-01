// Package domain is the example domain pack: the only fabriq package with
// domain-specific knowledge. It seeds three entities that exercise every
// fabric capability — site (plain aggregate), asset (graph edges), tag
// (telemetry metadata whose readings bypass per-row events).
package domain

import "github.com/xraph/grove"

// Site is a physical location (plant, facility) assets live at.
type Site struct {
	grove.BaseModel `grove:"table:sites"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Name     string `grove:"name,notnull" json:"name"`
	Code     string `grove:"code" json:"code"`
	Region   string `grove:"region" json:"region"`
}
