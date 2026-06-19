package domain

import (
	"time"

	"github.com/xraph/grove"
)

// FsPermission is an ACL grant on an fs_node. Append-only: grant = insert,
// revoke = delete. Enforcement (effective-permission evaluation) lives in the
// consuming seam, not in fabriq.
type FsPermission struct {
	grove.BaseModel `grove:"table:fs_permissions"`

	ID            string    `grove:"id,pk"             json:"id"`
	TenantID      string    `grove:"tenant_id,notnull" json:"tenantId"`
	ScopeID       string    `grove:"scope_id"          json:"scopeId"`
	Version       int64     `grove:"version,notnull"   json:"version"`
	NodeID        string    `grove:"node_id,notnull"   json:"nodeId"`
	PrincipalType string    `grove:"principal_type"    json:"principalType"` // user|role|team
	PrincipalID   string    `grove:"principal_id"      json:"principalId"`
	Permission    string    `grove:"permission"        json:"permission"` // read|write|delete|admin
	GrantedBy     string    `grove:"granted_by"        json:"grantedBy"`
	CreatedAt     time.Time `grove:"created_at"        json:"createdAt"`
}
