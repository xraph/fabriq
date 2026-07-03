package domain

import (
	"time"

	"github.com/xraph/grove"
)

// FsShare is a share-link record for an fs_node. fabriq stores it verbatim; the
// seam generates the token, hashes the password, and enforces expiry/cap/password.
type FsShare struct {
	grove.BaseModel `grove:"table:fabriq_fs_shares"`

	ID            string     `grove:"id,pk"             json:"id"`
	TenantID      string     `grove:"tenant_id,notnull" json:"tenantId"`
	ScopeID       string     `grove:"scope_id"          json:"scopeId"`
	Version       int64      `grove:"version,notnull"   json:"version"`
	NodeID        string     `grove:"node_id,notnull"   json:"nodeId"`
	Token         string     `grove:"token,notnull"     json:"token"`
	Permission    string     `grove:"permission"        json:"permission"`
	ExpiresAt     *time.Time `grove:"expires_at"        json:"expiresAt"`
	MaxDownloads  *int       `grove:"max_downloads"     json:"maxDownloads"`
	DownloadCount int        `grove:"download_count"    json:"downloadCount"`
	PasswordHash  string     `grove:"password_hash"     json:"-"`
	CreatedBy     string     `grove:"created_by"        json:"createdBy"`
	CreatedAt     time.Time  `grove:"created_at"        json:"createdAt"`
}
