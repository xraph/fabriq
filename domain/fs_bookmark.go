package domain

import (
	"time"

	"github.com/xraph/grove"
)

// FsBookmark is a user's bookmark of an fs_node (a favourite), with a per-user
// sort order. Unique per (tenant, user, node).
type FsBookmark struct {
	grove.BaseModel `grove:"table:fabriq_fs_bookmarks"`

	ID        string    `grove:"id,pk"             json:"id"`
	TenantID  string    `grove:"tenant_id,notnull" json:"tenantId"`
	ScopeID   string    `grove:"scope_id"          json:"scopeId"`
	Version   int64     `grove:"version,notnull"   json:"version"`
	UserID    string    `grove:"user_id,notnull"   json:"userId"`
	NodeID    string    `grove:"node_id,notnull"   json:"nodeId"`
	SortOrder int       `grove:"sort_order"        json:"sortOrder"`
	CreatedAt time.Time `grove:"created_at"        json:"createdAt"`
}
