package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// CrdtDocument is a collaborative document's merged (current) state, as
// returned by GET {BasePath}/crdt/:entity/:id. It mirrors adminapi's
// crdtSnapshotResponse JSON exactly: {docId, version, snapshot}. Snapshot's
// shape is document-specific, so it is left as raw JSON.
type CrdtDocument struct {
	// DocID is the document id ("<entity>/<id>").
	DocID string `json:"docId"`
	// Version is the aggregate version assigned by the last materialization
	// event; 0 until the quiet-window snapshot lands a relational row.
	Version int64 `json:"version"`
	// Snapshot is the merged CRDT state as a column-keyed JSON object.
	Snapshot json.RawMessage `json:"snapshot"`
}

// CrdtUpdate is metadata for a single entry in a document's CRDT update log.
// It mirrors adminapi's crdtUpdateItem JSON exactly: {index, size, preview}.
type CrdtUpdate struct {
	// Index is the update's ordinal position in the Sync page (0-based).
	Index int `json:"index"`
	// Size is the byte length of the encoded update blob.
	Size int `json:"size"`
	// Preview is a base64 preview of the first bytes of the blob.
	Preview string `json:"preview"`
}

// CrdtUpdatePage is a page of CRDT update-log metadata, as returned by
// GET {BasePath}/crdt/:entity/:id/updates. It mirrors adminapi's
// crdtUpdateLogResponse JSON exactly:
// {docId, highWaterSeq, hasSnapshot, items}.
type CrdtUpdatePage struct {
	// DocID is the document id the log belongs to.
	DocID string `json:"docId"`
	// HighWaterSeq is the highest log seq folded into this page.
	HighWaterSeq int64 `json:"highWaterSeq"`
	// HasSnapshot reports whether a compacted snapshot preceded the tail
	// updates (older updates have been folded away).
	HasSnapshot bool `json:"hasSnapshot"`
	// Items lists the tail updates (post-snapshot).
	Items []CrdtUpdate `json:"items"`
}

// GetCrdtDocument fetches the merged (current) state of a collaborative
// document. docID is "<entity>/<id>"; each "/"-separated segment is
// percent-escaped individually so a slash inside a segment does not collide
// with the entity/id separator, then the segments are re-joined with "/" -
// mirroring the fabriq-admin TS client's encodeDocId so the server's
// two-path-param route (:entity/:id) sees the same segments either way. It
// calls GET {BasePath}/crdt/<entity>/<id>.
func (c *Client) GetCrdtDocument(ctx context.Context, docID string) (CrdtDocument, error) {
	var out CrdtDocument
	if err := c.do(ctx, http.MethodGet, "/crdt/"+encodeDocID(docID), nil, nil, &out); err != nil {
		return CrdtDocument{}, err
	}
	return out, nil
}

// GetCrdtUpdatesParams are the query parameters for GetCrdtUpdates.
type GetCrdtUpdatesParams struct {
	// DocID is the document id ("<entity>/<id>").
	DocID string
	// Limit caps the number of returned tail updates (server default 100,
	// capped server-side). Zero omits the query param and defers to the
	// server default.
	Limit int
}

// GetCrdtUpdates fetches metadata for a document's update log (size + a
// base64 preview per update; never the full payloads). It calls
// GET {BasePath}/crdt/<entity>/<id>/updates[?limit=<n>].
func (c *Client) GetCrdtUpdates(ctx context.Context, params GetCrdtUpdatesParams) (CrdtUpdatePage, error) {
	q := url.Values{}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}

	var out CrdtUpdatePage
	path := "/crdt/" + encodeDocID(params.DocID) + "/updates"
	if err := c.do(ctx, http.MethodGet, path, q, nil, &out); err != nil {
		return CrdtUpdatePage{}, err
	}
	return out, nil
}

// encodeDocID percent-escapes each "/"-separated segment of a CRDT docID
// individually, then re-joins them with "/", so a slash inside the docID is
// preserved as the real <entity>/<id> path separator rather than being
// escaped to %2F. Mirrors the fabriq-admin TS client's encodeDocId.
func encodeDocID(docID string) string {
	segments := strings.Split(docID, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, "/")
}
