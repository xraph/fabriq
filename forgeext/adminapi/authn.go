package adminapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/core/event"
)

// base62Alphabet is used to render random key bytes into a URL-safe,
// unambiguous character set for API keys.
const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// generateKey creates a new random API key of the form "fq_<base62>", along
// with its lookup prefix (the first 7 characters of the key, used for
// non-secret indexed lookup) and its sha256 hex digest (used for storage and
// verification without persisting the raw key).
func generateKey() (key, prefix, hash string, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", "", err
	}

	encoded := make([]byte, len(raw))
	for i, b := range raw {
		encoded[i] = base62Alphabet[int(b)%len(base62Alphabet)]
	}

	key = "fq_" + string(encoded)
	prefix = key[:7]
	hash = hashKey(key)
	return key, prefix, hash, nil
}

// hashKey returns the sha256 hex digest of key, used as the stored/lookup
// representation so raw keys are never persisted.
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// constantTimeEqualHash compares two hash strings in constant time to avoid
// timing side-channels during key verification.
func constantTimeEqualHash(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// KeySpec describes a key to be issued. TenantID == "" issues a multi-tenant
// key (stored as NULL tenant_id, callable against any tenant); a non-empty
// TenantID scopes the key to that single tenant.
type KeySpec struct {
	Label         string
	TenantID      string
	CanManageKeys bool
}

// IssuedKey is returned once, at issue time. Key is the plaintext bearer token
// and is never persisted or recoverable afterwards — only its hash is stored.
type IssuedKey struct {
	ID     string
	Prefix string
	Key    string
}

// KeyRecord is a stored key as seen on the read paths. It deliberately carries
// NO hash or plaintext field: redaction is structural, so List can never leak a
// verifiable secret. TenantID == "" means the key is multi-tenant (NULL in the
// database).
type KeyRecord struct {
	ID            string
	Prefix        string
	TenantID      string
	Label         string
	CanManageKeys bool
	CreatedAt     time.Time
	RevokedAt     *time.Time
}

// KeyStore is the persistence port for hosted-fabriq API keys. It operates on
// the instance-global fabriq_api_key table (created by migration 0027), which
// is deliberately NOT under RLS: keys are resolved by hash before any tenant
// context exists, mirroring the outbox's tenant-less rationale.
type KeyStore interface {
	// Issue mints a new key, persists its hash, and returns the plaintext once.
	Issue(ctx context.Context, spec KeySpec) (IssuedKey, error)
	// Lookup resolves a key by its sha256 hex hash with NO tenant scoping.
	// Revoked keys still return (found=true): revocation is enforced by the
	// middleware, not by hiding the row.
	Lookup(ctx context.Context, keyHash string) (rec KeyRecord, found bool, err error)
	// List returns every key as a redacted KeyRecord (no hash/plaintext).
	List(ctx context.Context) ([]KeyRecord, error)
	// Revoke stamps revoked_at on the key. The row stays visible to Lookup.
	Revoke(ctx context.Context, id string) error
}

// relationalKeyStore is the grove-backed KeyStore. fabriq's relational facade
// exposes only a READ-ONLY raw path (RelationalQuerier.Query), and the command
// plane requires a registered aggregate that stamps tenant/outbox — neither
// fits an instance-global, tenant-nullable auth table. So writes AND reads go
// through the pg driver's raw API directly (mirroring adapters/postgres, which
// runs internal SQL via ptx.NewRaw(...).Exec / pg.Query).
type relationalKeyStore struct {
	pg *pgdriver.PgDB
}

// NewKeyStore builds a KeyStore over a grove.DB. The grove MUST be backed by
// the pg driver (grove + pgdriver), which is fabriq's only supported backend;
// this mirrors postgres.OpenWithGrove's driver assertion.
func NewKeyStore(gdb *grove.DB) KeyStore {
	pg, ok := gdb.Driver().(*pgdriver.PgDB)
	if !ok {
		// fabriq only ever runs on the pg driver; a mismatch is a wiring bug.
		panic(fmt.Sprintf("adminapi: KeyStore needs a grove backed by the pg driver, got %q", gdb.Driver().Name()))
	}
	return &relationalKeyStore{pg: pg}
}

// Issue generates a fresh key, inserts a fully-specified row (all columns from
// Go values — no DB now()/defaults), and returns the plaintext once.
func (s *relationalKeyStore) Issue(ctx context.Context, spec KeySpec) (IssuedKey, error) {
	key, prefix, hash, err := generateKey()
	if err != nil {
		return IssuedKey{}, fmt.Errorf("adminapi: generate key: %w", err)
	}

	id := event.NewID()
	createdAt := time.Now().UTC()

	// tenant_id is nullable: "" -> NULL (multi-tenant).
	var tenantArg any
	if spec.TenantID != "" {
		tenantArg = spec.TenantID
	}

	const insert = `INSERT INTO fabriq_api_key
		(id, prefix, key_hash, tenant_id, label, can_manage_keys, created_at, revoked_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)`
	if _, err := s.pg.NewRaw(insert,
		id, prefix, hash, tenantArg, spec.Label, spec.CanManageKeys, createdAt,
	).Exec(ctx); err != nil {
		return IssuedKey{}, fmt.Errorf("adminapi: insert api key: %w", err)
	}

	return IssuedKey{ID: id, Prefix: prefix, Key: key}, nil
}

// Lookup resolves a key by hash. No tenant scoping — the table is
// instance-global. Returns (zero, false, nil) when no row matches.
func (s *relationalKeyStore) Lookup(ctx context.Context, keyHash string) (KeyRecord, bool, error) {
	const q = `SELECT id, prefix, tenant_id, label, can_manage_keys, created_at, revoked_at
		FROM fabriq_api_key
		WHERE key_hash = $1`
	rows, err := s.pg.Query(ctx, q, keyHash)
	if err != nil {
		return KeyRecord{}, false, fmt.Errorf("adminapi: lookup api key: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return KeyRecord{}, false, fmt.Errorf("adminapi: lookup api key rows: %w", err)
		}
		return KeyRecord{}, false, nil
	}

	rec, err := scanKeyRecord(rows)
	if err != nil {
		return KeyRecord{}, false, err
	}
	return rec, true, nil
}

// List returns every stored key as a redacted KeyRecord, newest first.
func (s *relationalKeyStore) List(ctx context.Context) ([]KeyRecord, error) {
	const q = `SELECT id, prefix, tenant_id, label, can_manage_keys, created_at, revoked_at
		FROM fabriq_api_key
		ORDER BY id DESC`
	rows, err := s.pg.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("adminapi: list api keys: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []KeyRecord
	for rows.Next() {
		rec, err := scanKeyRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminapi: list api keys rows: %w", err)
	}
	return out, nil
}

// Revoke stamps revoked_at (Go time, UTC) on the key. The row stays visible to
// Lookup — revocation is enforced by the middleware, not by hiding the row.
func (s *relationalKeyStore) Revoke(ctx context.Context, id string) error {
	const q = `UPDATE fabriq_api_key SET revoked_at = $2 WHERE id = $1`
	if _, err := s.pg.NewRaw(q, id, time.Now().UTC()).Exec(ctx); err != nil {
		return fmt.Errorf("adminapi: revoke api key: %w", err)
	}
	return nil
}

// scanKeyRecord scans one row of the SELECT column list used by Lookup/List
// into a KeyRecord, mapping the nullable tenant_id/revoked_at columns.
func scanKeyRecord(rows interface{ Scan(...any) error }) (KeyRecord, error) {
	var (
		rec     KeyRecord
		tenant  sql.NullString
		revoked sql.NullTime
	)
	if err := rows.Scan(&rec.ID, &rec.Prefix, &tenant, &rec.Label, &rec.CanManageKeys, &rec.CreatedAt, &revoked); err != nil {
		return KeyRecord{}, fmt.Errorf("adminapi: scan api key: %w", err)
	}
	if tenant.Valid {
		rec.TenantID = tenant.String
	}
	if revoked.Valid {
		t := revoked.Time
		rec.RevokedAt = &t
	}
	return rec, nil
}
