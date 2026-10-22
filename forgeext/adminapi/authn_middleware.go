package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/tenant"
)

// bearerPrefix is the case-insensitive scheme prefix of the Authorization
// header. RFC 7235 treats the scheme token case-insensitively, so both
// "Bearer " and "bearer " are accepted.
const bearerPrefix = "bearer "

// keyIDCtxKey is the package-private context key under which the resolved
// API key's id is stashed after a successful lookup. Unexported so only this
// package can write it; resolvedKeyID is the read-side accessor for
// downstream handlers (e.g. the future logout handler, which revokes the
// presented session by this id).
type keyIDCtxKey struct{}

// resolvedKeyID returns the id of the API key that authMiddleware resolved
// for this request, if any. Absent for requests that bypassed auth (e.g. the
// /login exemption) or predate the middleware running.
func resolvedKeyID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(keyIDCtxKey{}).(string)
	return id, ok
}

// authMiddleware verifies a per-tenant API key on every admin request and
// resolves the tenant into the request context for downstream handlers.
//
// basePath is the admin API mount point (e.g. "/admin"). Requests under
// basePath+"/keys" additionally require a key with CanManageKeys; requests
// under basePath+"/tenants" require a multi-tenant key with CanManageKeys.
// Paths are compared in routePath form, the form the router resolves.
//
// Those two subtrees, plus exactly GET basePath+"/migrations" (the migration
// status), are the instance routes: they act on the instance, not on one
// tenant's data. On an instance route a multi-tenant key may omit X-Tenant-ID;
// the request then proceeds with no tenant stamped (the key id is still
// stashed). The migration status needs CanManageKeys like /keys does. Every
// other route, including POST basePath+"/migrations/up" and any deeper
// migrations path, is a data route and keeps the 400.
//
// A request that passes every check records the key's last use through the
// store's optional KeyToucher (see touchThrottle): at most once a minute per
// key, off the request path, never failing the request.
//
// The X-Fabriq-Api-Version response header is set on EVERY response (success or
// short-circuit), so clients can always read the server's contract version.
//
// Short-circuits write the status + a small JSON body directly and return nil,
// rather than returning a forge error: forge's route-middleware runner maps ANY
// error returned from middleware to HTTP 500 (it does not inspect the error's
// StatusCode), so returning a status-bearing forge error would leak as a 500.
// This mirrors corsMiddleware's direct-WriteHeader short-circuit.
//
// Return-code matrix:
//
//	missing/malformed Authorization        → 401
//	Lookup miss OR RevokedAt != nil        → 401
//	ExpiresAt != nil and in the past       → 401
//	Lookup error                           → 500
//	tenant-bound key, X-Tenant-ID mismatch → 403
//	multi-tenant key, X-Tenant-ID absent,
//	  on a data route                      → 400
//	.../keys route, !CanManageKeys         → 403
//	.../tenants route, !CanManageKeys or a
//	  tenant-bound key                     → 403
//	GET .../migrations, !CanManageKeys     → 403
//	X-Fabriq-Api-Version present, major ≠  → 426
//	otherwise                              → next(ctx)
func authMiddleware(store KeyStore, basePath string) forge.Middleware {
	return authMiddlewareWithTouch(store, basePath, newTouchThrottle(store))
}

// authMiddlewareWithTouch is authMiddleware with the last-use throttle
// supplied by the caller, so tests can drive it with their own clock and a
// synchronous runner. A nil throttle records no last use.
func authMiddlewareWithTouch(store KeyStore, basePath string, touches *touchThrottle) forge.Middleware {
	// Every route decision below compares routePath forms, so a request the
	// router resolves to a keys or tenants handler can never dodge the gates
	// with extra slashes ("/admin//keys", "//admin/keys").
	loginPath := routePath(basePath + "/login")
	keysPrefix := routePath(basePath + "/keys")
	tenantsPrefix := routePath(basePath + "/tenants")
	migrationsStatusPath := routePath(basePath + "/migrations")

	return func(next forge.Handler) forge.Handler {
		return func(ctx forge.Context) error {
			// 0. The login route is the way in: it must be reachable with no
			// Authorization header at all. This is the ONLY exemption from
			// auth — every other route, including logout, stays gated. Checked
			// before the version header is set / anything else runs, so a
			// POST to basePath+"/login" is a pure pass-through.
			req := ctx.Request()
			path := routePath(req.URL.Path)
			if req.Method == http.MethodPost && path == loginPath {
				return next(ctx)
			}

			// Always advertise the server's contract version, including on the
			// short-circuits below. Set before any WriteHeader so it lands in
			// the response.
			ctx.Response().Header().Set(apiVersionHeader, apiVersionValue())

			// 1. Extract the bearer token (scheme is case-insensitive).
			authz := req.Header.Get("Authorization")
			if len(authz) < len(bearerPrefix) || !strings.EqualFold(authz[:len(bearerPrefix)], bearerPrefix) {
				return deny(ctx, http.StatusUnauthorized, "missing or malformed Authorization header")
			}
			raw := strings.TrimSpace(authz[len(bearerPrefix):])
			if raw == "" {
				return deny(ctx, http.StatusUnauthorized, "missing or malformed Authorization header")
			}

			// 2. Resolve the key by hash. Revoked keys still resolve (found) so
			// revocation is enforced here, not by hiding the row.
			rec, found, err := store.Lookup(req.Context(), hashKey(raw))
			if err != nil {
				return deny(ctx, http.StatusInternalServerError, "key lookup failed")
			}
			if !found || rec.RevokedAt != nil || (rec.ExpiresAt != nil && rec.ExpiresAt.Before(time.Now())) {
				return deny(ctx, http.StatusUnauthorized, "invalid or revoked API key")
			}

			selector := req.Header.Get("X-Tenant-ID")
			keysRoute := isUnderRoute(path, keysPrefix)
			tenantsRoute := isUnderRoute(path, tenantsPrefix)
			migrationsStatus := req.Method == http.MethodGet && path == migrationsStatusPath
			instanceRoute := keysRoute || tenantsRoute || migrationsStatus

			// 3/4. Resolve the effective tenant. tid stays "" only for a
			// multi-tenant key on an instance route with no selector.
			var tid string
			if rec.TenantID != "" {
				// Tenant-bound key: it dictates the tenant. A conflicting
				// selector is a client error (403).
				if selector != "" && selector != rec.TenantID {
					return deny(ctx, http.StatusForbidden, "X-Tenant-ID does not match the key's tenant")
				}
				tid = rec.TenantID
			} else {
				// Multi-tenant key: the selector is required on data routes.
				// Instance routes act on no single tenant, so it may be absent.
				if selector == "" && !instanceRoute {
					return deny(ctx, http.StatusBadRequest, "X-Tenant-ID header is required for a multi-tenant key")
				}
				tid = selector
			}

			// 5. Key- and tenant-management routes require CanManageKeys.
			if !rec.CanManageKeys && keysRoute {
				return deny(ctx, http.StatusForbidden, "this API key may not manage keys")
			}
			// The migration status describes the whole instance, so it is an
			// operator view: it needs the same CanManageKeys as /keys.
			if !rec.CanManageKeys && migrationsStatus {
				return deny(ctx, http.StatusForbidden, "this API key may not read migration status")
			}
			// Tenants are instance-wide, so only a multi-tenant manage key may
			// touch them: a tenant-bound key is refused even with CanManageKeys.
			if tenantsRoute && (!rec.CanManageKeys || rec.TenantID != "") {
				return deny(ctx, http.StatusForbidden, "this API key may not manage tenants")
			}

			// 6. Version negotiation: if the client advertises a version, its
			// major must match. Absent header is tolerated.
			if v := req.Header.Get(apiVersionHeader); v != "" {
				major, perr := strconv.Atoi(strings.SplitN(v, ".", 2)[0])
				if perr != nil || major != APIVersion {
					return deny(ctx, http.StatusUpgradeRequired,
						"unsupported X-Fabriq-Api-Version; server speaks "+apiVersionValue())
				}
			}

			// Stamp the resolved tenant so downstream handlers and the fabric
			// see it, and stash the resolved key id alongside it (the future
			// logout handler reads this via resolvedKeyID to revoke the
			// presented session). WithContext replaces the request's context
			// in place, so both must land in the same call.
			// An instance-route request with no tenant skips the stamp, so
			// anything below that needs a tenant still fails with ErrNoTenant.
			tctx := req.Context()
			if tid != "" {
				tctx, err = tenant.WithTenant(tctx, tid)
				if err != nil {
					return deny(ctx, http.StatusBadRequest, "invalid tenant id: "+err.Error())
				}
			}
			ctx.WithContext(context.WithValue(tctx, keyIDCtxKey{}, rec.ID))

			// Every check passed: record the use. touch never blocks.
			touches.touch(rec.ID)

			return next(ctx)
		}
	}
}

const (
	// touchInterval is the least time between two recorded uses of one key in
	// one process. lastUsedAt is therefore accurate to about this long.
	touchInterval = time.Minute
	// touchTimeout bounds one Touch call. It runs on a context detached from
	// the request, so a slow database can neither stall nor cancel it.
	touchTimeout = 2 * time.Second
)

// touchThrottle records a key's last use through a KeyToucher without ever
// getting in the request's way: the write runs in its own goroutine on a
// context detached from the request with a 2s timeout, and a key is written at
// most once per touchInterval per process. The throttle state is in memory, so
// a restart or a second replica may write a key a little more often; both are
// harmless because the write is a plain UPDATE.
//
// The zero value is not usable; build one with newTouchThrottle. A nil
// *touchThrottle is a valid no-op.
type touchThrottle struct {
	toucher  KeyToucher
	interval time.Duration
	now      func() time.Time
	// spawn runs fn off the request path. Production uses a goroutine; tests
	// replace it with a synchronous call so they can assert without sleeping.
	spawn func(fn func())

	mu   sync.Mutex
	last map[string]time.Time
}

// newTouchThrottle returns a throttle over store's KeyToucher capability, or
// nil when the store does not have one (it then reports no last use).
func newTouchThrottle(store KeyStore) *touchThrottle {
	kt, ok := store.(KeyToucher)
	if !ok {
		return nil
	}
	return &touchThrottle{
		toucher:  kt,
		interval: touchInterval,
		now:      time.Now,
		spawn:    func(fn func()) { go fn() },
		last:     map[string]time.Time{},
	}
}

// touch records a use of the key id unless it was recorded within the
// interval. A failed write is dropped: the next request after the interval
// tries again, and the request itself has long since moved on.
func (t *touchThrottle) touch(id string) {
	if t == nil {
		return
	}
	at := t.now()
	t.mu.Lock()
	if prev, seen := t.last[id]; seen && at.Sub(prev) < t.interval {
		t.mu.Unlock()
		return
	}
	// Entries older than the interval can never throttle anything again, so
	// drop them on every insert: the map then holds only the keys seen in the
	// last interval, not one id per login for the life of the process.
	for k, prev := range t.last {
		if at.Sub(prev) >= t.interval {
			delete(t.last, k)
		}
	}
	t.last[id] = at
	t.mu.Unlock()

	t.spawn(func() {
		ctx, cancel := context.WithTimeout(context.Background(), touchTimeout)
		defer cancel()
		_ = t.toucher.Touch(ctx, id, at)
	})
}

// deny writes a JSON error body with the given status and returns nil. Returning
// nil (rather than an error) is deliberate: forge maps any error returned from
// route middleware to HTTP 500, so a status-bearing short-circuit MUST write the
// response itself. The X-Fabriq-Api-Version header is assumed already set by the
// caller.
func deny(ctx forge.Context, status int, message string) error {
	w := ctx.Response()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":    http.StatusText(status),
		"message": message,
	})
	return nil
}

// isUnderRoute reports whether path targets the subtree rooted at prefix (the
// keys or the tenants subtree). It matches prefix exactly and any sub-path, but
// not sibling paths that merely share the prefix string (e.g.
// "/admin/keyspace"). Both arguments must already be in routePath form.
func isUnderRoute(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// routePath returns p the way forge's default router (forgemux) resolves it:
// empty segments are dropped, so repeated, leading and trailing slashes
// collapse, and the result has exactly one leading slash. Dot segments are
// kept as they are, because the router matches "." and ".." as literal
// segments rather than resolving them. Matching is case-sensitive, like the
// router's static segments. p is the decoded r.URL.Path the router walks, so
// an encoded slash ("%2F") has already become a separator in both.
func routePath(p string) string {
	segs := strings.Split(p, "/")
	kept := segs[:0]
	for _, seg := range segs {
		if seg != "" {
			kept = append(kept, seg)
		}
	}
	return "/" + strings.Join(kept, "/")
}
