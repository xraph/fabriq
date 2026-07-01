package adminapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/xraph/forge"
)

// defaultUpdateLogLimit is the default page size for the CRDT update-log
// listing.
const defaultUpdateLogLimit = 100

// crdtSnapshotResponse is the payload for GET {BasePath}/crdt/:docId. It
// projects a document.Materialized into a stable camelCase shape the SPA
// renders: the merged field map plus the materialized aggregate version.
type crdtSnapshotResponse struct {
	// DocID is the document id ("<entity>/<id>").
	DocID string `json:"docId"`
	// Version is the aggregate version assigned by the LAST materialization
	// event; 0 until the quiet-window snapshot lands a relational row.
	Version int64 `json:"version"`
	// Snapshot is the merged CRDT state as a column-keyed JSON object.
	Snapshot json.RawMessage `json:"snapshot"`
}

// crdtUpdateItem is one entry in the document's update-log listing. It carries
// the byte size and a short base64 preview of each appended update, derived
// from the public document.Store.Sync payload.
//
// HONEST LIMITATION: the per-row log seq and createdAt timestamp live in the
// fabriq_crdt_updates table and are NOT surfaced by the public document.Store
// port (Sync returns only the opaque update blobs and a final high-water seq).
// The adminapi extension holds no public raw-Postgres handle, so those columns
// cannot be read cleanly from here. Index is therefore the update's ordinal
// position within the Sync page (0-based, oldest first), not the table seq.
type crdtUpdateItem struct {
	// Index is the update's ordinal position in the Sync page (0-based).
	Index int `json:"index"`
	// Size is the byte length of the encoded update blob.
	Size int `json:"size"`
	// Preview is a base64 preview of the first updatePreviewBytes of the blob.
	Preview string `json:"preview"`
}

// crdtUpdateLogResponse is the payload for GET {BasePath}/crdt/:docId/updates.
type crdtUpdateLogResponse struct {
	// DocID is the document id the log belongs to.
	DocID string `json:"docId"`
	// HighWaterSeq is the highest log seq folded into this Sync response (the
	// table-level high-water mark exposed by the Sync wire protocol).
	HighWaterSeq int64 `json:"highWaterSeq"`
	// HasSnapshot reports whether a compacted snapshot preceded the tail
	// updates in the Sync payload (older updates have been folded away).
	HasSnapshot bool `json:"hasSnapshot"`
	// Items lists the tail updates (post-snapshot) returned by Sync.
	Items []crdtUpdateItem `json:"items"`
}

// updatePreviewBytes bounds the base64 preview of each update blob so the
// listing never streams full payloads.
const updatePreviewBytes = 64

// syncWirePayload mirrors the JSON shape DocStore.Sync marshals (postgres
// adapter syncPayload): a high-water seq, an optional compacted snapshot, and
// the tail updates. The fields are decoded through the public Sync byte
// contract; the adapter's struct is unexported, so this local mirror tracks the
// documented wire tags (seq / snapshot / updates).
type syncWirePayload struct {
	Seq      int64             `json:"seq"`
	Snapshot json.RawMessage   `json:"snapshot,omitempty"`
	Updates  []json.RawMessage `json:"updates"`
}

// registerCrdtRoutes wires the CRDT document-plane read routes onto the given
// router. They share the same route options (auth/tenant middleware) as the
// rest of the admin surface so the host controls the security boundary
// uniformly.
//
// Routes:
//
//	GET {base}/crdt/:docId          merged snapshot of a document
//	GET {base}/crdt/:docId/updates  the document's tail update log (metadata)
//
// Both return 501 when the instance has no document plane configured (no
// KindDocument / CRDTSpec entity is registered, or the store reports
// not-configured). docId is "<entity>/<id>"; because forge routers split path
// params on "/", the entity and id are passed as two segments.
func (c *adminController) registerCrdtRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	snapOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.crdt.snapshot"),
		forge.WithSummary("Merged CRDT snapshot for a document (docId = <entity>/<id>)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/crdt/:entity/:id", c.handleCrdtSnapshot, snapOpts...); err != nil {
		return err
	}

	logOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.crdt.updates"),
		forge.WithSummary("Append-only update-log metadata for a document"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.GET(base+"/crdt/:entity/:id/updates", c.handleCrdtUpdates, logOpts...)
}

// docIDFromParams reconstructs the "<entity>/<id>" document id from the two
// path segments, validating both are present.
func docIDFromParams(ctx forge.Context) (string, bool) {
	entity := strings.TrimSpace(ctx.Param("entity"))
	id := strings.TrimSpace(ctx.Param("id"))
	if entity == "" || id == "" {
		return "", false
	}
	return entity + "/" + id, true
}

// handleCrdtSnapshot serves GET {BasePath}/crdt/:entity/:id.
//
// It returns the merged CRDT state and the materialized aggregate version.
// Returns 400 on a malformed docId, 501 when the document plane is not
// configured, and 404 when the docId names a configured-but-unknown document
// ENTITY (a type that is not a registered KindDocument).
//
// HONEST NOTE: a docId whose entity IS a registered document but whose id has
// no updates yet is NOT a 404 — the document.Store port has no "document
// exists" concept distinct from "has updates", so Snapshot returns an empty
// merged field map with NO error. Such a doc reads as 200 with an empty
// snapshot ({}) and version 0. Distinguishing "never written" from "deleted"
// would require a store-level existence check the port does not expose.
func (c *adminController) handleCrdtSnapshot(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.crdtConfigured() {
		return c.crdtNotConfigured(ctx)
	}

	docID, ok := docIDFromParams(ctx)
	if !ok {
		return forge.BadRequest("path must be /crdt/<entity>/<id>")
	}

	mat, snapErr := fab.Document().Snapshot(ctx.Request().Context(), docID)
	if snapErr != nil {
		return mapCrdtError(ctx, c, snapErr)
	}

	snapshot := mat.Snapshot
	if len(snapshot) == 0 {
		snapshot = json.RawMessage("{}")
	}
	return ctx.JSON(http.StatusOK, crdtSnapshotResponse{
		DocID:    docID,
		Version:  mat.Version,
		Snapshot: snapshot,
	})
}

// handleCrdtUpdates serves GET {BasePath}/crdt/:entity/:id/updates.
//
// Optional query params:
//
//	limit  max number of tail updates to return (default 100, capped at maxLimit)
//
// It reads the document's tail updates through the public Sync byte protocol
// (from-scratch state vector) and projects per-update size + preview metadata.
// Returns 400 on a malformed docId, 501 when the document plane is not
// configured, and 404 for an unknown document entity.
func (c *adminController) handleCrdtUpdates(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.crdtConfigured() {
		return c.crdtNotConfigured(ctx)
	}

	docID, ok := docIDFromParams(ctx)
	if !ok {
		return forge.BadRequest("path must be /crdt/<entity>/<id>/updates")
	}

	limit := defaultUpdateLogLimit
	if lStr := ctx.Query("limit"); lStr != "" {
		l, parseErr := strconv.Atoi(lStr)
		if parseErr != nil || l < 1 {
			return forge.BadRequest("query param 'limit' must be a positive integer")
		}
		if l > maxLimit {
			l = maxLimit
		}
		limit = l
	}

	// A nil state vector means "from the beginning": Sync returns the compacted
	// snapshot (when present) plus every tail update. This is the only public,
	// side-effect-free seam onto the update log; the per-row seq/createdAt
	// columns are not exposed by the port (see crdtUpdateItem).
	raw, syncErr := fab.Document().Sync(ctx.Request().Context(), docID, nil)
	if syncErr != nil {
		return mapCrdtError(ctx, c, syncErr)
	}

	var payload syncWirePayload
	if decErr := json.Unmarshal(raw, &payload); decErr != nil {
		return forge.InternalError(decErr)
	}

	items := make([]crdtUpdateItem, 0, len(payload.Updates))
	for i, u := range payload.Updates {
		if i >= limit {
			break
		}
		items = append(items, crdtUpdateItem{
			Index:   i,
			Size:    len(u),
			Preview: previewBase64(u),
		})
	}

	return ctx.JSON(http.StatusOK, crdtUpdateLogResponse{
		DocID:        docID,
		HighWaterSeq: payload.Seq,
		HasSnapshot:  len(payload.Snapshot) > 0,
		Items:        items,
	})
}

// previewBase64 returns a base64 preview of the first updatePreviewBytes of the
// update blob (the whole blob when shorter).
func previewBase64(b []byte) string {
	if len(b) > updatePreviewBytes {
		b = b[:updatePreviewBytes]
	}
	return base64.StdEncoding.EncodeToString(b)
}

// crdtConfigured reports whether the instance has the document (CRDT) plane
// configured. It reuses the registry-derived signal the capabilities endpoint
// uses (registryHasDocumentPlane) so the crdt routes and the capability flag
// agree exactly.
//
// A port probe is deliberately NOT used: the postgres document store is wired
// whenever Postgres is open, so a probe cannot tell "plane present, document
// entity registered" from "plane present, but no KindDocument entity declared"
// — both share the same live store. The registry signal (any KindDocument /
// CRDTSpec entity) is the deterministic, side-effect-free truth, and it matches
// the documented capabilities.go crdt flag.
func (c *adminController) crdtConfigured() bool {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return false
	}
	return registryHasDocumentPlane(reg)
}

// crdtNotConfigured returns the 501 response used when the instance has no
// document (CRDT) plane wired. It mirrors the not-configured shape used across
// the admin surface so the SPA can branch on a stable error payload.
func (c *adminController) crdtNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "document/CRDT plane not configured"})
}

// mapCrdtError translates document.Store errors to forge HTTP errors:
//
//   - a not-configured store (the deferred fake or an unconfigured stub) → 501,
//     matching the instance capability;
//   - an unknown / non-document entity in the docId → 404 (the docId names no
//     servable document);
//   - everything else → 500.
func mapCrdtError(ctx forge.Context, c *adminController, err error) error {
	if notConfigured(err) {
		return c.crdtNotConfigured(ctx)
	}
	if isNotADocumentErr(err) {
		return forge.NotFound("document not found")
	}
	return forge.InternalError(err)
}

// isNotADocumentErr reports whether err is the document store's "not a
// registered document entity" / "must be <entity>/<id>" rejection. The postgres
// DocStore.splitDocID returns these as plain fmt errors (no sentinel), so a
// substring match covers the unknown-entity and malformed-id cases that should
// read as 404 rather than 500.
func isNotADocumentErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "is not a registered document entity") ||
		strings.Contains(msg, "must be <entity>/<id>")
}
