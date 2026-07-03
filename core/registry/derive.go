package registry

import "fmt"

// Event verbs. EventType(entity, verb) is the only way event type strings
// are minted, so appliers and consumers can rely on the shape.
const (
	VerbCreated = "created"
	VerbUpdated = "updated"
	VerbDeleted = "deleted"
)

// EventType derives the canonical event type for an entity and verb,
// e.g. "asset.updated".
func EventType(entity, verb string) string {
	return entity + "." + verb
}

// StreamKey is the single Redis stream all domain events are relayed to;
// projections consume it through their own consumer groups.
func StreamKey() string { return "fabriq:events" }

// ChannelName derives a subscription channel. Channels are tenant-prefixed
// and only ever constructed here: changes:{tenant}:{scope}:{id}.
func ChannelName(tenantID string, scope Scope, id string) string {
	return fmt.Sprintf("changes:%s:%s:%s", tenantID, scope.Name, id)
}

// DocChannelName derives the RAW document-sync channel for one document:
// doc:{tenant}:{docID}. Frames on it are never conflated.
func DocChannelName(tenantID, docID string) string {
	return fmt.Sprintf("doc:%s:%s", tenantID, docID)
}

// DocPresenceChannelName derives the RAW awareness channel for one
// document: docpresence:{tenant}:{docID}. Presence frames are ephemeral —
// capped stream, tailed from now, never persisted, no delivery guarantees.
func DocPresenceChannelName(tenantID, docID string) string {
	return fmt.Sprintf("docpresence:%s:%s", tenantID, docID)
}

// GraphName derives the per-tenant graph name (FalkorDB key).
func GraphName(tenantID string) string {
	return "tenant_" + tenantID
}

// GraphNameVersioned derives a blue-green build target for rebuilds; the
// live pointer is tracked in projection_state.
func GraphNameVersioned(tenantID string, version int) string {
	return fmt.Sprintf("tenant_%s_v%d", tenantID, version)
}

// SearchIndexAlias derives the stable per-tenant alias for a search index;
// reads and writes go through the alias, rebuilds swap it atomically.
func SearchIndexAlias(tenantID, base string) string {
	return fmt.Sprintf("fabriq_%s_%s", tenantID, base)
}

// SearchIndexVersioned derives the concrete versioned index behind the alias.
func SearchIndexVersioned(tenantID, base string, version int) string {
	return fmt.Sprintf("fabriq_%s_%s_v%d", tenantID, base, version)
}
