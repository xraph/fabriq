package domain

import (
	"time"

	"github.com/xraph/grove"
)

// FsNode is a filesystem catalog node (folder or file) in the tree over the
// blob plane. parent_id is the adjacency truth; path is a materialized index
// maintained transactionally on move/rename. File nodes reference a
// blob_object (blob_id) with denormalized facets.
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
	DeletedAt   *time.Time     `grove:"deleted_at"        json:"deletedAt"`
	DeletedBy   string         `grove:"deleted_by"        json:"deletedBy"`
	CreatedAt   time.Time      `grove:"created_at"        json:"createdAt"`
	UpdatedAt   time.Time      `grove:"updated_at"        json:"updatedAt"`
}
