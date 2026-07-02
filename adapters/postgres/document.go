package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/blob"
	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/document"
	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// DocStore is the Postgres document plane: an append-only update log
// (fabriq_crdt_updates) folded through grove's CRDT merge engine, with
// compacted snapshots and quiet-window materialization into ordinary
// entity rows + outbox events.
//
// Update blobs are JSON-encoded []crdt.ChangeRecord (the "grove-crdt"
// engine named by CRDTSpec). Document ids carry their entity:
// "<entity>/<ulid>" — the registry's KindDocument entry binds the
// relational shape materialization writes.
type DocStore struct {
	a     *Adapter
	merge *crdt.MergeEngine

	// blob is the byte-plane handle for offloaded history segments (nil =
	// history stays in Postgres). archiveDefault is the global opt-in;
	// per-entity CRDTSpec.ArchiveHistory overrides it. segCache serves sealed
	// segments hot after first fetch.
	blob           blob.Store
	archiveDefault bool
	segCache       *segmentCache
}

var _ document.Store = (*DocStore)(nil)
var _ document.HistoryReader = (*DocStore)(nil)
var _ document.SegmentLister = (*DocStore)(nil)
var _ document.HistoryPurger = (*DocStore)(nil)

// Documents returns the document-plane store.
func (a *Adapter) Documents() *DocStore {
	return &DocStore{a: a, merge: crdt.NewMergeEngine(), segCache: newSegmentCache(128)}
}

// EnableArchive wires the blob handle and the global archive default. Called
// by Open once the storage adapter exists; safe to call with def=false to set
// only the default. A nil b leaves history in Postgres.
func (d *DocStore) EnableArchive(b blob.Store, def bool) {
	d.blob = b
	d.archiveDefault = def
}

// archiveEnabled reports whether this entity's history should be offloaded:
// requires a blob store, then the per-entity override if set, else the global
// default.
func (d *DocStore) archiveEnabled(ent *registry.Entity) bool {
	if d.blob == nil || ent == nil || ent.Spec.CRDT == nil {
		return false
	}
	if ent.Spec.CRDT.ArchiveHistory != nil {
		return *ent.Spec.CRDT.ArchiveHistory
	}
	return d.archiveDefault
}

// splitDocID parses "<entity>/<id>" and validates the entity is a
// registered KindDocument.
func (d *DocStore) splitDocID(docID string) (*registry.Entity, error) {
	entity, _, ok := strings.Cut(docID, "/")
	if !ok {
		return nil, fmt.Errorf("fabriq: document id %q must be <entity>/<id>", docID)
	}
	ent, found := d.a.reg.Get(entity)
	if !found || ent.Spec.Kind != registry.KindDocument {
		return nil, fmt.Errorf("fabriq: %q is not a registered document entity", entity)
	}
	return ent, nil
}

// ApplyUpdate implements document.Store: append one update to the log and
// touch the doc's activity timestamp (the quiet-window clock).
func (d *DocStore) ApplyUpdate(ctx context.Context, docID string, update []byte) error {
	_, err := d.ApplyUpdateWithSeq(ctx, docID, update)
	return err
}

// ApplyUpdateWithSeq is ApplyUpdate returning the assigned log seq — the
// live fan-out decorator stamps it on the published sync frame so clients
// can detect gaps and fall back to Sync.
func (d *DocStore) ApplyUpdateWithSeq(ctx context.Context, docID string, update []byte) (int64, error) {
	ent, err := d.splitDocID(docID)
	if err != nil {
		return 0, err
	}
	var changes []crdt.ChangeRecord
	if uerr := json.Unmarshal(update, &changes); uerr != nil || len(changes) == 0 {
		return 0, fmt.Errorf("fabriq: update for %s is not a non-empty []ChangeRecord: %w", docID, uerr)
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return 0, err
	}
	// scope stamps the optional secondary scope on the update + bookkeeping
	// rows. NULLIF maps an unscoped write ("") to a true SQL NULL — the shared
	// sentinel the scope-aware RLS predicate treats as visible to every scope.
	scope := tenant.ScopeOrEmpty(ctx)
	var seq int64
	err = d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var seqs []updateRow
		if insErr := tx.NewRaw(
			`INSERT INTO fabriq_crdt_updates (doc_id, tenant_id, update_data, scope_id) VALUES ($1, $2, $3, NULLIF($4, '')) RETURNING seq, update_data`,
			docID, tid, update, scope).Scan(ctx, &seqs); insErr != nil {
			return insErr
		}
		if len(seqs) == 1 {
			seq = seqs[0].Seq
		}
		// The bookkeeping row carries the high-water mark so the
		// worker-plane materializer never has to peek into the RLS'd log.
		// scope_id is stamped on first write (the doc's canonical scope) and
		// left untouched on conflict so later updates can't repartition it.
		_, upErr := tx.NewRaw(`INSERT INTO fabriq_crdt_docs (doc_id, tenant_id, entity, last_seq, scope_id)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''))
			ON CONFLICT (doc_id) DO UPDATE SET updated_at = now(), last_seq = GREATEST(fabriq_crdt_docs.last_seq, EXCLUDED.last_seq)`,
			docID, tid, ent.Spec.Name, seq, scope).Exec(ctx)
		return upErr
	})
	return seq, err
}

type updateRow struct {
	Seq        int64  `grove:"seq"`
	UpdateData []byte `grove:"update_data"`
}

// syncPageLimit bounds one Sync response; clients loop until an empty
// page (the returned Seq advances their vector each round).
const syncPageLimit = 500

// syncPayload is the wire shape Sync exchanges with grove-crdt clients.
type syncPayload struct {
	Seq      int64             `json:"seq"`
	Snapshot json.RawMessage   `json:"snapshot,omitempty"` // crdt.State
	Updates  []json.RawMessage `json:"updates"`
}

// Sync implements document.Store: the state vector is an 8-byte
// big-endian last-seen seq (empty = from the beginning); the reply holds
// the compacted snapshot (when the client is behind it) plus every later
// update, and the new vector seq.
func (d *DocStore) Sync(ctx context.Context, docID string, stateVector []byte) ([]byte, error) {
	if _, err := d.splitDocID(docID); err != nil {
		return nil, err
	}
	since := int64(0)
	if len(stateVector) == 8 {
		since = int64(binary.BigEndian.Uint64(stateVector)) // #nosec G115 -- seqs are bigserial, far below int64 max
	}
	var payload syncPayload
	payload.Seq = since
	err := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var snapSeq int64
		var snapRaw []byte
		var snaps []struct {
			LastSeq  int64  `grove:"last_seq"`
			Snapshot []byte `grove:"snapshot"`
		}
		if err := tx.NewRaw(`SELECT last_seq, snapshot FROM fabriq_crdt_snapshots WHERE doc_id = $1`, docID).
			Scan(ctx, &snaps); err != nil {
			return err
		}
		if len(snaps) == 1 {
			snapSeq, snapRaw = snaps[0].LastSeq, snaps[0].Snapshot
		}
		if since < snapSeq {
			payload.Snapshot = snapRaw
			payload.Seq = snapSeq
			since = snapSeq
		}
		// Pages are bounded: a client behind by more than syncPageLimit
		// updates loops (vector advances each call) until an empty page.
		var rows []updateRow
		if err := tx.NewRaw(
			`SELECT seq, update_data FROM fabriq_crdt_updates WHERE doc_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`,
			docID, since, syncPageLimit).Scan(ctx, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			payload.Updates = append(payload.Updates, json.RawMessage(r.UpdateData))
			payload.Seq = r.Seq
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}

// Snapshot implements document.Store: merged current state (compacted
// snapshot + log tail) and the materialized aggregate version.
func (d *DocStore) Snapshot(ctx context.Context, docID string) (document.Materialized, error) {
	ent, err := d.splitDocID(docID)
	if err != nil {
		return document.Materialized{}, err
	}
	state, _, err := d.mergedState(ctx, docID)
	if err != nil {
		return document.Materialized{}, err
	}
	vals := stateValues(state)
	raw, err := json.Marshal(vals)
	if err != nil {
		return document.Materialized{}, err
	}
	var version int64
	verErr := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var rows []versionRow
		sql := fmt.Sprintf(`SELECT version FROM %s WHERE id = $1`, quoteIdent(ent.Binding.Table))
		if err := tx.NewRaw(sql, docID).Scan(ctx, &rows); err != nil {
			return err
		}
		if len(rows) == 1 {
			version = rows[0].Version
		}
		return nil
	})
	if verErr != nil {
		return document.Materialized{}, verErr
	}
	return document.Materialized{DocID: docID, Snapshot: raw, Version: version}, nil
}

// Compact implements document.Store: fold the log into the snapshot row and
// trim it, one transaction. When archive is enabled for the entity, the
// trimmed updates are first sealed into an immutable blob segment + index row
// (blob Put before the DB commit) so history survives outside the DB. Merge
// results never change — only their storage shape.
func (d *DocStore) Compact(ctx context.Context, docID string) error {
	ent, err := d.splitDocID(docID)
	if err != nil {
		return err
	}
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	state, maxSeq, err := d.mergedState(ctx, docID)
	if err != nil {
		return err
	}
	if maxSeq == 0 {
		return nil // nothing newer than the snapshot
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}

	archive := d.archiveEnabled(ent)

	// When archiving, seal the trimmed range (prevSeq, maxSeq] to a blob BEFORE
	// the DB tx. prevSeq is the pre-compact snapshot watermark; the sealed range
	// tiles contiguously with earlier segments.
	var (
		segKey   string
		segLo    int64
		segCount int64
		segBytes int64
	)
	if archive {
		segKey, segLo, segCount, segBytes, err = d.sealSegment(ctx, docID, maxSeq)
		if err != nil {
			return err
		}
	}

	return d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		// The snapshot inherits the doc's recorded scope (from the bookkeeping
		// row) rather than the caller's ctx scope: compaction may run from an
		// unscoped worker/admin context, and a snapshot stamped NULL there would
		// silently become shared — leaking scoped content to every scope. The
		// subquery pins it to the doc's canonical scope so the compacted state is
		// filtered exactly like the raw update log.
		if _, err := tx.NewRaw(`INSERT INTO fabriq_crdt_snapshots (doc_id, tenant_id, snapshot, last_seq, at, scope_id)
			VALUES ($1, $2, $3, $4, now(), (SELECT scope_id FROM fabriq_crdt_docs WHERE doc_id = $1))
			ON CONFLICT (doc_id) DO UPDATE SET snapshot = EXCLUDED.snapshot, last_seq = EXCLUDED.last_seq, scope_id = EXCLUDED.scope_id, at = now()`,
			docID, tid, raw, maxSeq).Exec(ctx); err != nil {
			return err
		}
		if archive && segCount > 0 {
			if _, err := tx.NewRaw(`INSERT INTO fabriq_crdt_segments
				(doc_id, tenant_id, seq_lo, seq_hi, blob_key, byte_size, update_count, scope_id)
				VALUES ($1, $2, $3, $4, $5, $6, $7, (SELECT scope_id FROM fabriq_crdt_docs WHERE doc_id = $1))`,
				docID, tid, segLo, maxSeq, segKey, segBytes, segCount).Exec(ctx); err != nil {
				return err
			}
		}
		_, err := tx.NewRaw(`DELETE FROM fabriq_crdt_updates WHERE doc_id = $1 AND seq <= $2`, docID, maxSeq).Exec(ctx)
		return err
	})
}

// sealSegment reads the trimmable updates (prevSeq, maxSeq], encodes them into
// a segment, and Puts it to the blob store under a deterministic key. Returns
// the key, the sealed range's low seq, the update count, and the byte size.
// A zero count means there was nothing to seal (returns count 0, no blob written).
func (d *DocStore) sealSegment(ctx context.Context, docID string, maxSeq int64) (key string, seqLo, count, size int64, err error) {
	var prevSeq int64
	var entries []segEntry
	rerr := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var snaps []struct {
			LastSeq int64 `grove:"last_seq"`
		}
		if e := tx.NewRaw(`SELECT last_seq FROM fabriq_crdt_snapshots WHERE doc_id = $1`, docID).Scan(ctx, &snaps); e != nil {
			return e
		}
		if len(snaps) == 1 {
			prevSeq = snaps[0].LastSeq
		}
		var rows []updateRow
		if e := tx.NewRaw(
			`SELECT seq, update_data FROM fabriq_crdt_updates WHERE doc_id = $1 AND seq > $2 AND seq <= $3 ORDER BY seq`,
			docID, prevSeq, maxSeq).Scan(ctx, &rows); e != nil {
			return e
		}
		for _, r := range rows {
			entries = append(entries, segEntry{Seq: r.Seq, Data: r.UpdateData})
		}
		return nil
	})
	if rerr != nil {
		return "", 0, 0, 0, rerr
	}
	if len(entries) == 0 {
		return "", 0, 0, 0, nil
	}
	seqLo = prevSeq + 1
	key = segmentKey(docID, seqLo, maxSeq)
	body := encodeSegment(entries)
	if _, e := d.blob.Put(ctx, key, bytes.NewReader(body), blob.PutOpts{ContentType: "application/octet-stream", Size: int64(len(body))}); e != nil {
		return "", 0, 0, 0, fmt.Errorf("fabriq: seal segment %s: %w", key, e)
	}
	return key, seqLo, int64(len(entries)), int64(len(body)), nil
}

// ReadHistory returns every update with seqLo <= seq <= seqHi in seq order,
// drawn from sealed blob segments (cached after first fetch) and the live
// update log. A referenced segment blob that is missing is a hard error — never
// silently skipped (that would be undetected data loss).
func (d *DocStore) ReadHistory(ctx context.Context, docID string, seqLo, seqHi int64) ([]document.HistoryUpdate, error) {
	if _, err := d.splitDocID(docID); err != nil {
		return nil, err
	}
	if seqLo > seqHi {
		return nil, nil
	}
	out := make([]document.HistoryUpdate, 0, 16)

	// 1) Sealed segments overlapping [seqLo, seqHi].
	type segRow struct {
		SeqLo   int64  `grove:"seq_lo"`
		SeqHi   int64  `grove:"seq_hi"`
		BlobKey string `grove:"blob_key"`
	}
	var segs []segRow
	if err := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(
			`SELECT seq_lo, seq_hi, blob_key FROM fabriq_crdt_segments
			 WHERE doc_id = $1 AND seq_hi >= $2 AND seq_lo <= $3 ORDER BY seq_lo`,
			docID, seqLo, seqHi).Scan(ctx, &segs)
	}); err != nil {
		return nil, err
	}
	for _, s := range segs {
		updates, cerr := d.loadSegment(ctx, s.BlobKey)
		if cerr != nil {
			return nil, cerr
		}
		for _, u := range updates {
			if u.Seq >= seqLo && u.Seq <= seqHi {
				out = append(out, u)
			}
		}
	}

	// 2) Still-in-DB updates in range (hot tail + any not-yet-sealed rows).
	var rows []updateRow
	if err := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(
			`SELECT seq, update_data FROM fabriq_crdt_updates WHERE doc_id = $1 AND seq >= $2 AND seq <= $3 ORDER BY seq`,
			docID, seqLo, seqHi).Scan(ctx, &rows)
	}); err != nil {
		return nil, err
	}
	for _, r := range rows {
		out = append(out, document.HistoryUpdate{Seq: r.Seq, Update: json.RawMessage(r.UpdateData)})
	}

	// A seq lives in exactly one tier (sealing deletes from the DB), so a plain
	// sort yields the ordered, gap-free range.
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	// A seq normally lives in exactly one tier, but de-dup defensively so an
	// overlapping/duplicate segment (e.g. from a concurrent compaction) can't
	// surface the same update twice.
	if len(out) > 1 {
		dedup := out[:1]
		for _, u := range out[1:] {
			if u.Seq != dedup[len(dedup)-1].Seq {
				dedup = append(dedup, u)
			}
		}
		out = dedup
	}
	return out, nil
}

// DeleteHistory purges a document's offloaded history: every segment blob and
// its index row. The GC seam for document deletion. Blob deletes run first
// (best-effort per key); the index rows are dropped last so a partial failure
// leaves recoverable pointers rather than orphaned rows.
func (d *DocStore) DeleteHistory(ctx context.Context, docID string) error {
	if _, err := d.splitDocID(docID); err != nil {
		return err
	}
	type segRow struct {
		BlobKey string `grove:"blob_key"`
	}
	var segs []segRow
	if err := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(`SELECT blob_key FROM fabriq_crdt_segments WHERE doc_id = $1`, docID).Scan(ctx, &segs)
	}); err != nil {
		return err
	}
	if d.blob != nil {
		for _, s := range segs {
			if err := d.blob.Delete(ctx, s.BlobKey); err != nil {
				return fmt.Errorf("fabriq: delete history segment %s: %w", s.BlobKey, err)
			}
			d.segCache.put(s.BlobKey, nil) // drop any cached copy
		}
	}
	return d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		_, err := tx.NewRaw(`DELETE FROM fabriq_crdt_segments WHERE doc_id = $1`, docID).Exec(ctx)
		return err
	})
}

// ListSegments returns the sealed history segments for a document, ordered by
// their seq range. Blob keys are intentionally omitted (internal detail).
func (d *DocStore) ListSegments(ctx context.Context, docID string) ([]document.SegmentInfo, error) {
	if _, err := d.splitDocID(docID); err != nil {
		return nil, err
	}
	type segRow struct {
		SegSeq      int64     `grove:"seg_seq"`
		SeqLo       int64     `grove:"seq_lo"`
		SeqHi       int64     `grove:"seq_hi"`
		UpdateCount int64     `grove:"update_count"`
		ByteSize    int64     `grove:"byte_size"`
		At          time.Time `grove:"at"`
	}
	var rows []segRow
	if err := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		return tx.NewRaw(
			`SELECT seg_seq, seq_lo, seq_hi, update_count, byte_size, at
			 FROM fabriq_crdt_segments WHERE doc_id = $1 ORDER BY seq_lo`,
			docID).Scan(ctx, &rows)
	}); err != nil {
		return nil, err
	}
	out := make([]document.SegmentInfo, len(rows))
	for i, r := range rows {
		out[i] = document.SegmentInfo{
			SegSeq: r.SegSeq, SeqLo: r.SeqLo, SeqHi: r.SeqHi,
			UpdateCount: r.UpdateCount, ByteSize: r.ByteSize, At: r.At,
		}
	}
	return out, nil
}

// loadSegment returns a sealed segment's decoded updates, from the cache when
// present, otherwise fetching the blob, decoding, and caching it.
func (d *DocStore) loadSegment(ctx context.Context, blobKey string) ([]document.HistoryUpdate, error) {
	if v, ok := d.segCache.get(blobKey); ok {
		return v, nil
	}
	rc, _, err := d.blob.Get(ctx, blobKey)
	if err != nil {
		return nil, fmt.Errorf("fabriq: missing history segment %s: %w", blobKey, err)
	}
	defer func() { _ = rc.Close() }()
	body, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	entries, err := decodeSegment(body)
	if err != nil {
		return nil, fmt.Errorf("fabriq: corrupt history segment %s: %w", blobKey, err)
	}
	updates := make([]document.HistoryUpdate, len(entries))
	for i, e := range entries {
		updates[i] = document.HistoryUpdate{Seq: e.Seq, Update: json.RawMessage(e.Data)}
	}
	d.segCache.put(blobKey, updates)
	return updates, nil
}

// mergedState folds snapshot + log tail through grove's merge engine,
// returning the state and the highest log seq folded (0 if none).
func (d *DocStore) mergedState(ctx context.Context, docID string) (*crdt.State, int64, error) {
	state := crdt.NewState("fabriq_docs", docID)
	var maxSeq int64
	err := d.a.inTenantTx(ctx, func(tx *pgdriver.PgTx) error {
		var snaps []struct {
			LastSeq  int64  `grove:"last_seq"`
			Snapshot []byte `grove:"snapshot"`
		}
		if err := tx.NewRaw(`SELECT last_seq, snapshot FROM fabriq_crdt_snapshots WHERE doc_id = $1`, docID).
			Scan(ctx, &snaps); err != nil {
			return err
		}
		since := int64(0)
		if len(snaps) == 1 {
			if err := json.Unmarshal(snaps[0].Snapshot, state); err != nil {
				return fmt.Errorf("fabriq: corrupt snapshot for %s: %w", docID, err)
			}
			since = snaps[0].LastSeq
		}
		var rows []updateRow
		if err := tx.NewRaw(
			`SELECT seq, update_data FROM fabriq_crdt_updates WHERE doc_id = $1 AND seq > $2 ORDER BY seq`,
			docID, since).Scan(ctx, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			var changes []crdt.ChangeRecord
			if err := json.Unmarshal(r.UpdateData, &changes); err != nil {
				return fmt.Errorf("fabriq: corrupt update %d for %s: %w", r.Seq, docID, err)
			}
			for i := range changes {
				if err := d.fold(state, &changes[i]); err != nil {
					return err
				}
			}
			maxSeq = r.Seq
		}
		return nil
	})
	return state, maxSeq, err
}

// fold applies one change record onto the state via grove's canonical op
// application: type-specific payloads (counter deltas, set/list/text ops,
// document path writes, full-state carriers) all merge losslessly.
func (d *DocStore) fold(state *crdt.State, c *crdt.ChangeRecord) error {
	merged, err := crdt.ApplyChange(d.merge, state.Fields[c.Field], c)
	if err != nil {
		return fmt.Errorf("fabriq: apply field %q: %w", c.Field, err)
	}
	state.Fields[c.Field] = merged
	return nil
}

// stateValues projects merged field states onto column-keyed values:
// counters resolve to their total, sets/lists to element arrays, text to
// its visible string, lww/document to the stored value.
func stateValues(state *crdt.State) map[string]any {
	vals := make(map[string]any, len(state.Fields))
	for field, fs := range state.Fields {
		if fs == nil {
			continue
		}
		switch fs.Type {
		case crdt.TypeCounter:
			if fs.CounterState != nil {
				vals[field] = fs.CounterState.Value()
			}
		case crdt.TypeSet:
			if fs.SetState != nil {
				vals[field] = rawElements(fs.SetState.Elements())
			}
		case crdt.TypeList:
			if fs.ListState != nil {
				vals[field] = rawElements(fs.ListState.Elements())
			}
		case crdt.TypeText:
			if fs.TextState != nil {
				vals[field] = fs.TextState.Value()
			}
		default:
			if len(fs.Value) == 0 {
				continue
			}
			var v any
			if err := json.Unmarshal(fs.Value, &v); err == nil {
				vals[field] = v
			}
		}
	}
	return vals
}

// rawElements decodes a JSON-encoded element list to plain values.
func rawElements(elements []json.RawMessage) []any {
	out := make([]any, 0, len(elements))
	for _, el := range elements {
		var v any
		if err := json.Unmarshal(el, &v); err == nil {
			out = append(out, v)
		}
	}
	return out
}

// ValidateFunc is the post-merge validation hook: CRDTs converge but do
// not guarantee business validity. A non-nil error flags the document for
// resolution instead of materializing.
type ValidateFunc func(entity string, vals map[string]any) error

// MaterializeQuiet materializes every unflagged document whose last
// activity is older than its entity's QuietWindow and which has updates
// beyond the last materialization: merged state -> validation -> entity
// row write + ONE <entity>.updated event (version++) through the outbox.
// Returns the number of documents materialized.
func (d *DocStore) MaterializeQuiet(ctx context.Context, validate ValidateFunc) (int, error) {
	type docRow struct {
		DocID    string `grove:"doc_id"`
		TenantID string `grove:"tenant_id"`
		Entity   string `grove:"entity"`
		Scope    string `grove:"scope_id"`
		LastSeq  int64  `grove:"last_seq_materialized"`
	}
	var docs []docRow
	// COALESCE folds the nullable scope_id to the "" sentinel so the doc's scope
	// can be carried onto the materialized row (materializeOne stamps the column).
	rows, err := d.a.pg.Query(ctx, `SELECT doc_id, tenant_id, entity, COALESCE(scope_id, ''), last_seq_materialized
		FROM fabriq_crdt_docs
		WHERE flagged = FALSE AND last_seq > last_seq_materialized`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var r docRow
		if err := rows.Scan(&r.DocID, &r.TenantID, &r.Entity, &r.Scope, &r.LastSeq); err != nil {
			_ = rows.Close()
			return 0, err
		}
		docs = append(docs, r)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	materialized := 0
	for _, doc := range docs {
		ent, ok := d.a.reg.Get(doc.Entity)
		if !ok || ent.Spec.CRDT == nil {
			continue
		}
		quiet, err := d.isQuiet(ctx, doc.DocID, ent.Spec.CRDT.QuietWindow)
		if err != nil || !quiet {
			continue
		}
		wrote, err := d.materializeOne(ctx, doc.TenantID, doc.Scope, doc.DocID, ent, validate)
		if err != nil {
			return materialized, err
		}
		if wrote {
			materialized++
		}
	}
	return materialized, nil
}

func (d *DocStore) isQuiet(ctx context.Context, docID string, window time.Duration) (bool, error) {
	row := d.a.pg.QueryRow(ctx,
		`SELECT updated_at < now() - ($2 || ' milliseconds')::interval FROM fabriq_crdt_docs WHERE doc_id = $1`,
		docID, fmt.Sprintf("%d", window.Milliseconds()))
	var quiet bool
	if err := row.Scan(&quiet); err != nil {
		return false, err
	}
	return quiet, nil
}

func (d *DocStore) materializeOne(ctx context.Context, tenantID, scope, docID string, ent *registry.Entity, validate ValidateFunc) (bool, error) {
	tctx, err := tenant.WithTenant(ctx, tenantID)
	if err != nil {
		return false, err
	}
	state, maxSeq, err := d.mergedState(tctx, docID)
	if err != nil {
		return false, err
	}
	vals := stateValues(state)

	if ent.Spec.Schema == nil || !ent.Spec.Schema.NoTypeCheck {
		if terr := registry.CoerceRow(ent, vals); terr != nil {
			_, ferr := d.a.pg.Exec(ctx,
				`UPDATE fabriq_crdt_docs SET flagged = TRUE, flag_reason = $2 WHERE doc_id = $1`,
				docID, terr.Error())
			if ferr != nil {
				return false, ferr
			}
			return false, nil // type mismatch: flagged for resolution; no event, no row
		}
	}

	if validate != nil {
		if verr := validate(ent.Spec.Name, vals); verr != nil {
			_, ferr := d.a.pg.Exec(ctx,
				`UPDATE fabriq_crdt_docs SET flagged = TRUE, flag_reason = $2 WHERE doc_id = $1`,
				docID, verr.Error())
			if ferr != nil {
				return false, ferr
			}
			return false, nil // flagged for resolution; no event, no row
		}
	}

	// One transactional write: row + ONE versioned event + the
	// materialization watermark, all in the same transaction — a crash
	// can never re-materialize (no duplicate events). storeTx gives the
	// command primitives; the raw watermark update rides the same PgTx
	// (the bookkeeping table has no RLS, so any tx may write it).
	err = d.a.inTenantTx(tctx, func(ptx *pgdriver.PgTx) error {
		var tx command.Tx = &storeTx{ptx: ptx}
		txCtx := tctx
		current, cvErr := tx.CurrentVersion(txCtx, ent, docID)
		if cvErr != nil {
			return cvErr
		}
		next := current + 1
		op := command.OpUpdate
		if current == 0 {
			op = command.OpCreate
		}
		stamped := make(map[string]any, len(vals)+3)
		for k, v := range vals {
			if ent.Binding.HasColumn(k) {
				stamped[k] = v
			}
		}
		stamped[registry.ColumnID] = docID
		stamped[registry.ColumnTenant] = tenantID
		stamped[registry.ColumnVersion] = next
		// Carry the document's scope onto the materialized row so a scope-aware
		// entity table keeps it partitioned. Scope is a soft column-level filter,
		// so stamping the column is sufficient — no need to re-scope the write
		// context. Entities without scope_id (e.g. the demo "page") and unscoped
		// docs (scope == "") are unaffected: the column stays NULL (shared).
		if scope != "" && ent.Binding.HasColumn(registry.ColumnScope) {
			stamped[registry.ColumnScope] = scope
		}
		if acErr := tx.ApplyChange(txCtx, ent, op, docID, next, stamped); acErr != nil {
			return acErr
		}
		payload, mErr := json.Marshal(stamped)
		if mErr != nil {
			return mErr
		}
		if obErr := tx.AppendOutbox(txCtx, event.Envelope{
			ID: event.NewID(), TenantID: tenantID, Aggregate: ent.Spec.Name, AggID: docID,
			Version: next, Type: registry.EventType(ent.Spec.Name, registry.VerbUpdated),
			At: time.Now().UTC(), PayloadSchemaVersion: 1, Payload: payload,
		}); obErr != nil {
			return obErr
		}
		_, wmErr := ptx.NewRaw(
			`UPDATE fabriq_crdt_docs SET last_seq_materialized = $2 WHERE doc_id = $1`, docID, maxSeq).Exec(txCtx)
		return wmErr
	})
	return err == nil, err
}
