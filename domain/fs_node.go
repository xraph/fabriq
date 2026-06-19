package domain

import (
	"context"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/hook"
)

// FsNode is a filesystem catalog node (folder or file) in the tree over the
// blob plane. parent_id is the adjacency truth; path is a materialized index
// maintained transactionally on move/rename. File nodes reference a
// blob_object (blob_id) with denormalized facets.
//
// Metadata and MountConfig are JSONB NOT NULL columns. Nil maps must be
// normalised before any INSERT or UPDATE to avoid a NOT NULL constraint
// violation. The hooks below fire because the command plane reaches Postgres
// through pgdriver.PgTx.NewInsert / NewUpdate — grove's model-mutation API —
// which calls hook.RunModelBeforeInsert / RunModelBeforeUpdate on the model
// prior to executing the statement (adapters/postgres/tx.go ApplyChange).
type FsNode struct {
	grove.BaseModel `grove:"table:fs_nodes"`

	ID          string         `grove:"id,pk"             json:"id"`
	TenantID    string         `grove:"tenant_id,notnull" json:"tenantId"`
	ScopeID     string         `grove:"scope_id"          json:"scopeId"`
	Version     int64          `grove:"version,notnull"   json:"version"`
	ParentID    string         `grove:"parent_id"         json:"parentId"` // ""=root
	Name        string         `grove:"name,notnull"      json:"name"`
	Path        string         `grove:"path"              json:"path"`
	NodeType    string         `grove:"node_type,notnull" json:"nodeType"` // "folder" | "file"
	BlobID      string         `grove:"blob_id"           json:"blobId"`
	Size        int64          `grove:"size"              json:"size"`
	ContentType string         `grove:"content_type"      json:"contentType"`
	Checksum    string         `grove:"checksum"          json:"checksum"`
	IsLocked    bool           `grove:"is_locked"         json:"isLocked"`
	LockedBy    string         `grove:"locked_by"         json:"lockedBy"`
	Metadata    map[string]any `grove:"metadata"          json:"metadata"`
	MountConfig map[string]any `grove:"mount_config"      json:"mountConfig"`
	DeletedAt   *time.Time     `grove:"deleted_at"        json:"deletedAt"`
	DeletedBy   string         `grove:"deleted_by"        json:"deletedBy"`
	CreatedAt   time.Time      `grove:"created_at"        json:"createdAt"`
	UpdatedAt   time.Time      `grove:"updated_at"        json:"updatedAt"`
}

// normalizeMaps ensures JSONB NOT NULL columns carry a non-nil map so the
// database constraint is never violated by a nil value from a bare struct
// literal (e.g. in tests) that did not go through the facade init path.
func (n *FsNode) normalizeMaps() {
	if n.Metadata == nil {
		n.Metadata = map[string]any{}
	}
	if n.MountConfig == nil {
		n.MountConfig = map[string]any{}
	}
}

// BeforeInsert implements grove/hook.BeforeInsertHook. It fires via
// pgdriver.PgTx.NewInsert (called by adapters/postgres/tx.go ApplyChange)
// before the INSERT executes, normalising nil JSONB maps so the
// mount_config/metadata NOT NULL constraint is never violated.
func (n *FsNode) BeforeInsert(_ context.Context, _ *hook.QueryContext) error {
	n.normalizeMaps()
	return nil
}

// BeforeUpdate implements grove/hook.BeforeUpdateHook for the same reason:
// pgdriver.PgTx.NewUpdate (also in ApplyChange) fires this before the UPDATE.
func (n *FsNode) BeforeUpdate(_ context.Context, _ *hook.QueryContext) error {
	n.normalizeMaps()
	return nil
}
