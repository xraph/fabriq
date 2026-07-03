package domain

import "github.com/xraph/grove"

// DigestNode is one node of a tenant's context-distillation Merkle tree.
// Levels: 0 = entity (per source row), 1 = scope/cluster backbone, 2 = tenant
// root. The summary text is content-addressed in the file-plane CAS (SummaryHash);
// ContentHash is the Merkle freshness key; SemHash is the SimHash fingerprint
// (16-hex). The vector lives in the vector plane (entity "digest_node"), not here.
type DigestNode struct {
	grove.BaseModel `grove:"table:fabriq_digest_nodes"`

	ID         string `grove:"id,pk"             json:"id"`
	TenantID   string `grove:"tenant_id,notnull" json:"tenantId"`
	Version    int64  `grove:"version,notnull"   json:"version"`
	Level      int    `grove:"level,notnull"     json:"level"`
	Kind       string `grove:"kind,notnull"      json:"kind"`
	ScopeName  string `grove:"scope_name"        json:"scopeName"`
	ScopeID    string `grove:"scope_id"          json:"scopeId"`
	SourceID   string `grove:"source_id"         json:"sourceId"`
	SourceKind string `grove:"source_kind"       json:"sourceKind"`

	SummaryHash string `grove:"summary_hash" json:"summaryHash"`
	ContentHash string `grove:"content_hash" json:"contentHash"`
	SemHash     string `grove:"sem_hash"     json:"semHash"` // 16-hex

	ChildIDs  []string `grove:"child_ids"  json:"childIds"`  // JSONB
	ParentIDs []string `grove:"parent_ids" json:"parentIds"` // JSONB
	UpdatedAt int64    `grove:"updated_at" json:"updatedAt"` // unix nanos
	Tokens    int64    `grove:"tokens"     json:"tokens"`    // cached summary token count
}
