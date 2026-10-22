package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver" //nolint:depguard // the TenantRegistry reads and writes an instance-global table that no fabriq port expresses; it uses the raw pg driver deliberately, mirroring KeyStore and adapters/postgres.

	"github.com/xraph/fabriq/core/tenant"
)

// maxTenantLabelLen caps the optional label on POST /tenants, counted in
// characters rather than bytes.
const maxTenantLabelLen = 200

// TenantRecord is one row of the static-mode tenant registry.
type TenantRecord struct {
	ID        string
	Label     string
	CreatedAt time.Time
}

// TenantRegistry is the persistence port for static-mode tenants. In row-level
// security mode a tenant is only a string on each row, so the registry is the
// one place that records a tenant exists. It operates on the instance-global
// fabriq_tenant table (created by migration 0034), which is deliberately NOT
// under RLS. Catalog mode has its own directory and does not use it.
type TenantRegistry interface {
	// Create records the tenant. An existing id returns the stored record and
	// created == false, with its label and created_at untouched.
	Create(ctx context.Context, id, label string) (rec TenantRecord, created bool, err error)
	// Get returns the tenant, or found == false when the id is unknown.
	Get(ctx context.Context, id string) (rec TenantRecord, found bool, err error)
	// List returns every tenant ordered by id.
	List(ctx context.Context) ([]TenantRecord, error)
}

// relationalTenantRegistry is the grove-backed TenantRegistry. It runs raw SQL
// through the pg driver for the same reason relationalKeyStore does: the table
// is instance-global and fits neither the command plane nor the read-only raw
// query path.
type relationalTenantRegistry struct {
	pg *pgdriver.PgDB
}

// NewTenantRegistry builds a TenantRegistry over a grove.DB. The grove MUST be
// backed by the pg driver, like NewKeyStore.
func NewTenantRegistry(gdb *grove.DB) TenantRegistry {
	pg, ok := gdb.Driver().(*pgdriver.PgDB)
	if !ok {
		// fabriq only ever runs on the pg driver; a mismatch is a wiring bug.
		panic(fmt.Sprintf("adminapi: TenantRegistry needs a grove backed by the pg driver, got %q", gdb.Driver().Name()))
	}
	return &relationalTenantRegistry{pg: pg}
}

// Create inserts the tenant when the id is new. ON CONFLICT DO NOTHING leaves
// an existing row alone, so a retry never rewrites the label or created_at.
func (r *relationalTenantRegistry) Create(ctx context.Context, id, label string) (TenantRecord, bool, error) {
	const q = `INSERT INTO fabriq_tenant (id, label) VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
		RETURNING id, label, created_at`
	rows, err := r.pg.Query(ctx, q, id, label)
	if err != nil {
		return TenantRecord{}, false, fmt.Errorf("adminapi: create tenant: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		rec, serr := scanTenantRecord(rows)
		if serr != nil {
			return TenantRecord{}, false, serr
		}
		return rec, true, nil
	}
	if rerr := rows.Err(); rerr != nil {
		return TenantRecord{}, false, fmt.Errorf("adminapi: create tenant rows: %w", rerr)
	}
	_ = rows.Close()

	// Nothing came back, so the id already existed: return the stored row.
	rec, found, gerr := r.Get(ctx, id)
	if gerr != nil {
		return TenantRecord{}, false, gerr
	}
	if !found {
		return TenantRecord{}, false, fmt.Errorf("adminapi: create tenant %q: conflict reported but no row found", id)
	}
	return rec, false, nil
}

// Get returns one tenant by id.
func (r *relationalTenantRegistry) Get(ctx context.Context, id string) (TenantRecord, bool, error) {
	const q = `SELECT id, label, created_at FROM fabriq_tenant WHERE id = $1`
	rows, err := r.pg.Query(ctx, q, id)
	if err != nil {
		return TenantRecord{}, false, fmt.Errorf("adminapi: get tenant: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if rerr := rows.Err(); rerr != nil {
			return TenantRecord{}, false, fmt.Errorf("adminapi: get tenant rows: %w", rerr)
		}
		return TenantRecord{}, false, nil
	}
	rec, err := scanTenantRecord(rows)
	if err != nil {
		return TenantRecord{}, false, err
	}
	return rec, true, nil
}

// List returns every tenant ordered by id.
func (r *relationalTenantRegistry) List(ctx context.Context) ([]TenantRecord, error) {
	const q = `SELECT id, label, created_at FROM fabriq_tenant ORDER BY id`
	rows, err := r.pg.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("adminapi: list tenants: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []TenantRecord
	for rows.Next() {
		rec, serr := scanTenantRecord(rows)
		if serr != nil {
			return nil, serr
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("adminapi: list tenants rows: %w", err)
	}
	return out, nil
}

func scanTenantRecord(rows interface{ Scan(...any) error }) (TenantRecord, error) {
	var rec TenantRecord
	if err := rows.Scan(&rec.ID, &rec.Label, &rec.CreatedAt); err != nil {
		return TenantRecord{}, fmt.Errorf("adminapi: scan tenant: %w", err)
	}
	rec.CreatedAt = rec.CreatedAt.UTC()
	return rec, nil
}

// staticTenantView is the JSON projection of a TenantRecord.
type staticTenantView struct {
	TenantID  string `json:"tenantId"`
	Label     string `json:"label"`
	State     string `json:"state"`
	Mode      string `json:"mode"`
	CreatedAt string `json:"createdAt"`
}

func toStaticTenantView(r TenantRecord) staticTenantView {
	return staticTenantView{
		TenantID:  r.ID,
		Label:     r.Label,
		State:     "active", // static tenants have no lifecycle: they exist or they do not
		Mode:      "static",
		CreatedAt: r.CreatedAt.UTC().Format(rfc3339Milli),
	}
}

// staticTenantRegistry returns the registry that serves the tenant routes, or
// nil when they belong to catalog mode. Static mode needs a registry and no
// catalog: with a catalog open, the db-per-tenant handlers keep the routes.
func (c *adminController) staticTenantRegistry() TenantRegistry {
	reg := c.ext.cfg.TenantRegistry
	if reg == nil {
		return nil
	}
	if stores := c.ext.resolveStores(); stores != nil && stores.Catalog != nil {
		return nil
	}
	return reg
}

// staticTenantBody is the POST {BasePath}/tenants request body in static mode.
type staticTenantBody struct {
	TenantID string `json:"tenantId"`
	Label    string `json:"label"`
}

// handleStaticTenantCreate serves POST {BasePath}/tenants in static mode:
// 201 for a new tenant, 200 with the same shape when it already existed.
func (c *adminController) handleStaticTenantCreate(ctx forge.Context, reg TenantRegistry) error {
	if err := c.requireCap(ctx, "tenants.admin"); err != nil {
		return err
	}
	var body staticTenantBody
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}
	if body.TenantID == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "tenantId is required"})
	}
	if !tenant.Valid(body.TenantID) {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid tenant id"})
	}
	if utf8.RuneCountInString(body.Label) > maxTenantLabelLen {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("label must be at most %d characters", maxTenantLabelLen)})
	}

	rec, created, err := reg.Create(ctx.Request().Context(), body.TenantID, body.Label)
	if err != nil {
		return renderError(ctx, err)
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	return ctx.JSON(status, map[string]staticTenantView{"tenant": toStaticTenantView(rec)})
}

// handleStaticTenantList serves GET {BasePath}/tenants in static mode.
func (c *adminController) handleStaticTenantList(ctx forge.Context, reg TenantRegistry) error {
	if err := c.requireCap(ctx, "tenants.admin"); err != nil {
		return err
	}
	recs, err := reg.List(ctx.Request().Context())
	if err != nil {
		return renderError(ctx, err)
	}
	out := make([]staticTenantView, 0, len(recs)) // non-nil: an empty list is [], never null
	for _, r := range recs {
		out = append(out, toStaticTenantView(r))
	}
	return ctx.JSON(http.StatusOK, map[string]any{"tenants": out})
}

// handleStaticTenantGet serves GET {BasePath}/tenants/:id in static mode.
func (c *adminController) handleStaticTenantGet(ctx forge.Context, reg TenantRegistry) error {
	if err := c.requireCap(ctx, "tenants.admin"); err != nil {
		return err
	}
	id := ctx.Param("id")
	if !tenant.Valid(id) {
		// A malformed id can never be registered, so the contract is the same
		// as for any unknown tenant: a view or a 404.
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such tenant"})
	}
	rec, found, err := reg.Get(ctx.Request().Context(), id)
	if err != nil {
		return renderError(ctx, err)
	}
	if !found {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such tenant"})
	}
	return ctx.JSON(http.StatusOK, toStaticTenantView(rec))
}
