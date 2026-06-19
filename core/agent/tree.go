package agent

import "fmt"

// DigestEntity is the registry entity name of the distillation tree node.
const DigestEntity = "digest_node"

// Digest node levels.
const (
	LevelEntity = 0 // L0: one per distillable source row
	LevelScope  = 1 // L1: scope/cluster backbone
	LevelTenant = 2 // L2: single tenant root
)

// Digest node kinds.
const (
	KindEntityNode  = "entity"
	KindScopeNode   = "scope"
	KindClusterNode = "cluster"
	KindTenantNode  = "tenant"
)

// L0ID derives the stable id of an entity (L0) digest node.
func L0ID(sourceKind, sourceID string) string {
	return fmt.Sprintf("digest:0:%s:%s", sourceKind, sourceID)
}

// ScopeID derives the stable id of a scope (L1) digest node.
func ScopeID(scopeName, scopeID string) string {
	return fmt.Sprintf("digest:1:scope:%s:%s", scopeName, scopeID)
}

// ClusterID derives the stable id of a cluster (L1) digest node from a SemHash
// prefix. The prefix is the cluster identity — stable across membership drift.
func ClusterID(prefix uint64, p int) string {
	return fmt.Sprintf("digest:1:cluster:%s:%d", FormatSemHash(prefix), p)
}

// TenantRootID is the stable id of the tenant (L2) root digest node.
func TenantRootID() string { return "digest:2:tenant" }

// NoiseFloorMet reports whether a SemHash bucket has enough members to become a
// cluster node (singletons get no digest).
func NoiseFloorMet(members, floor int) bool { return members >= floor }
