package domain

import "github.com/xraph/grove"

// BlobObject is the catalog row for a stored blob. Many BlobObjects may share a
// Hash (content dedup); the bytes live in the object store, the ref-count in fabriq_blob_cas.
type BlobObject struct {
	grove.BaseModel `grove:"table:fabriq_blob_objects"`

	ID          string `grove:"id,pk"             json:"id"`
	TenantID    string `grove:"tenant_id,notnull" json:"tenantId"`
	ScopeID     string `grove:"scope_id"          json:"scopeId"`
	Version     int64  `grove:"version,notnull"   json:"version"`
	Hash        string `grove:"hash,notnull"      json:"hash"`
	Size        int64  `grove:"size,notnull"      json:"size"`
	ContentType string `grove:"content_type"      json:"contentType"`
}
