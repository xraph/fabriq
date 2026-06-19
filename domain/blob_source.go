package domain

import "github.com/xraph/grove"

// BlobSource is a connection record for an external storage provider. The auth
// credentials are stored ONLY as ciphertext (auth_enc); the plaintext map never
// touches a column.
type BlobSource struct {
	grove.BaseModel `grove:"table:blob_sources"`

	ID          string            `grove:"id,pk"             json:"id"`
	TenantID    string            `grove:"tenant_id,notnull" json:"tenantId"`
	ScopeID     string            `grove:"scope_id"          json:"scopeId"`
	Version     int64             `grove:"version,notnull"   json:"version"`
	ProjectID   string            `grove:"project_id"        json:"projectId"`
	Name        string            `grove:"name,notnull"      json:"name"`
	Provider    string            `grove:"provider"          json:"provider"`
	Endpoint    string            `grove:"endpoint"          json:"endpoint"`
	BasePath    string            `grove:"base_path"         json:"basePath"`
	AuthEnc     []byte            `grove:"auth_enc"          json:"-"`
	WatchConfig map[string]any    `grove:"watch_config"      json:"watchConfig"`
	FileFilter  map[string]any    `grove:"file_filter"       json:"fileFilter"`
	Tags        map[string]string `grove:"tags"              json:"tags"`
	Enabled     bool              `grove:"enabled"           json:"enabled"`
}
