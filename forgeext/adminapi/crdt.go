package adminapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/registry"
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

// crdtEntityInfo describes one registered document (CRDT) entity for the admin
// UI: its identity plus the CRDTSpec knobs. archiveHistory is the per-entity
// override (nil pointer -> null = inherit the global default).
type crdtEntityInfo struct {
	Entity         string `json:"entity"`
	Kind           string `json:"kind"`
	Engine         string `json:"engine"`
	SnapshotEvery  int    `json:"snapshotEvery"`
	QuietWindowMs  int64  `json:"quietWindowMs"`
	ArchiveHistory *bool  `json:"archiveHistory"`
}

// crdtEntitiesResponse is the payload for GET {BasePath}/crdt/entities.
type crdtEntitiesResponse struct {
	Items []crdtEntityInfo `json:"items"`
}

// crdtSegmentItem is one sealed history segment's metadata, as recorded in
// fabriq_crdt_segments and surfaced via document.SegmentLister.
type crdtSegmentItem struct {
	SegSeq      int64  `json:"segSeq"`
	SeqLo       int64  `json:"seqLo"`
	SeqHi       int64  `json:"seqHi"`
	UpdateCount int64  `json:"updateCount"`
	ByteSize    int64  `json:"byteSize"`
	At          string `json:"at"`
}

// crdtSegmentsResponse is the payload for GET {BasePath}/crdt/:entity/:id/segments.
type crdtSegmentsResponse struct {
	DocID string            `json:"docId"`
	Items []crdtSegmentItem `json:"items"`
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
//	GET {base}/crdt/:docId/segments the document's sealed history segments
//
// Both return 501 when the instance has no document plane configured (no
// KindDocument / CRDTSpec entity is registered, or the store reports
// not-configured). docId is "<entity>/<id>"; because forge routers split path
// params on "/", the entity and id are passed as two segments.
func (c *adminController) registerCrdtRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	// Registered before the /crdt/:entity/:id param route so the static
	// "entities" segment is matched first. Verified by
	// TestCrdtEntities_ListsDocumentEntities: forge's router already
	// prioritizes static segments over params, so no additional guard in
	// handleCrdtSnapshot was needed.
	entitiesOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.crdt.entities"),
		forge.WithSummary("List registered CRDT/document entities + their spec"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/crdt/entities", c.handleCrdtEntities, entitiesOpts...); err != nil {
		return err
	}

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
	if err := r.GET(base+"/crdt/:entity/:id/updates", c.handleCrdtUpdates, logOpts...); err != nil {
		return err
	}

	segOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.crdt.segments"),
		forge.WithSummary("Sealed history segments for a document"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.GET(base+"/crdt/:entity/:id/segments", c.handleCrdtSegments, segOpts...)
}

// handleCrdtEntities serves GET {BasePath}/crdt/entities. It is a pure
// registry read (no document store involved): it lists every KindDocument
// entity (plus any entity carrying a CRDTSpec) along with the CRDT knobs the
// admin UI surfaces for document tagging and spec display.
func (c *adminController) handleCrdtEntities(ctx forge.Context) error {
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}
	items := make([]crdtEntityInfo, 0)
	for _, ent := range reg.All() {
		spec := ent.Spec
		if spec.Kind != registry.KindDocument && spec.CRDT == nil {
			continue
		}
		info := crdtEntityInfo{Entity: spec.Name, Kind: spec.Kind.String()}
		if spec.CRDT != nil {
			info.Engine = spec.CRDT.Engine
			info.SnapshotEvery = spec.CRDT.SnapshotEvery
			info.QuietWindowMs = spec.CRDT.QuietWindow.Milliseconds()
			info.ArchiveHistory = spec.CRDT.ArchiveHistory
		}
		items = append(items, info)
	}
	return ctx.JSON(http.StatusOK, crdtEntitiesResponse{Items: items})
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
		return renderError(ctx, decErr)
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

// handleCrdtSegments serves GET {BasePath}/crdt/:entity/:id/segments.
//
// It lists the sealed history segments recorded for a document (when the
// configured document.Store implements document.SegmentLister). Unlike
// handleCrdtSnapshot/handleCrdtUpdates, entity validation goes through the
// REGISTRY rather than a Store.Snapshot probe: the fake document store used in
// tests defers Snapshot (always not-configured), so a Snapshot-based check
// would wrongly report 501 for a registered document entity that simply has
// no segments yet. The registry is the same deterministic signal
// crdtConfigured() already relies on.
//
// Returns 400 on a malformed docId, 501 when the document plane is not
// configured at all, and 404 when :entity does not name a registered document
// (KindDocument or CRDT-tagged) entity. A registered document entity with no
// segments (or a store that doesn't implement SegmentLister) returns 200 with
// an empty items list.
func (c *adminController) handleCrdtSegments(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}
	if !c.crdtConfigured() {
		return c.crdtNotConfigured(ctx)
	}
	docID, ok := docIDFromParams(ctx)
	if !ok {
		return forge.BadRequest("docId must be <entity>/<id>")
	}
	reg, err := c.ext.resolveRegistry()
	if err != nil {
		return forge.InternalError(err)
	}
	ent, ok := reg.Get(ctx.Param("entity"))
	if !ok || (ent.Spec.Kind != registry.KindDocument && ent.Spec.CRDT == nil) {
		return forge.NotFound("not a document entity")
	}
	items := make([]crdtSegmentItem, 0)
	if lister, ok := fab.Document().(document.SegmentLister); ok {
		segs, lerr := lister.ListSegments(ctx.Request().Context(), docID)
		if lerr != nil {
			return renderError(ctx, lerr)
		}
		for _, s := range segs {
			items = append(items, crdtSegmentItem{
				SegSeq: s.SegSeq, SeqLo: s.SeqLo, SeqHi: s.SeqHi,
				UpdateCount: s.UpdateCount, ByteSize: s.ByteSize,
				At: s.At.UTC().Format(time.RFC3339),
			})
		}
	}
	return ctx.JSON(http.StatusOK, crdtSegmentsResponse{DocID: docID, Items: items})
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
	return renderError(ctx, err)
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
