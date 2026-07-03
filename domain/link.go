package domain

import "github.com/xraph/grove"

// Link is a generic reified relationship between two assets, used to exercise
// fabriq's reified-edge projection with the demo domain's own vocabulary.
type Link struct {
	grove.BaseModel `grove:"table:fabriq_links"`

	ID       string `grove:"id,pk" json:"id"`
	TenantID string `grove:"tenant_id,notnull" json:"tenant_id"`
	Version  int64  `grove:"version,notnull" json:"version"`
	Kind     string `grove:"kind,notnull" json:"kind"` // relationship type
	SourceID string `grove:"source_id,notnull" json:"source_id"`
	TargetID string `grove:"target_id,notnull" json:"target_id"`
	Note     string `grove:"note" json:"note"`
}
