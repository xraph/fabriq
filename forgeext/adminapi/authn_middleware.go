package adminapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/tenant"
)

// bearerPrefix is the case-insensitive scheme prefix of the Authorization
// header. RFC 7235 treats the scheme token case-insensitively, so both
// "Bearer " and "bearer " are accepted.
const bearerPrefix = "bearer "

// authMiddleware verifies a per-tenant API key on every admin request and
// resolves the tenant into the request context for downstream handlers.
//
// basePath is the admin API mount point (e.g. "/admin"); requests under
// basePath+"/keys" additionally require a key with CanManageKeys.
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
//	multi-tenant key, X-Tenant-ID absent   → 400
//	.../keys route, !CanManageKeys         → 403
//	X-Fabriq-Api-Version present, major ≠  → 426
//	otherwise                              → next(ctx)
func authMiddleware(store KeyStore, basePath string) forge.Middleware {
	keysPrefix := strings.TrimRight(basePath, "/") + "/keys"

	return func(next forge.Handler) forge.Handler {
		return func(ctx forge.Context) error {
			// Always advertise the server's contract version, including on the
			// short-circuits below. Set before any WriteHeader so it lands in
			// the response.
			ctx.Response().Header().Set(apiVersionHeader, apiVersionValue())

			req := ctx.Request()

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

			// 3/4. Resolve the effective tenant.
			var tid string
			if rec.TenantID != "" {
				// Tenant-bound key: it dictates the tenant. A conflicting
				// selector is a client error (403).
				if selector != "" && selector != rec.TenantID {
					return deny(ctx, http.StatusForbidden, "X-Tenant-ID does not match the key's tenant")
				}
				tid = rec.TenantID
			} else {
				// Multi-tenant key: the selector is required.
				if selector == "" {
					return deny(ctx, http.StatusBadRequest, "X-Tenant-ID header is required for a multi-tenant key")
				}
				tid = selector
			}

			// 5. Key-management routes require CanManageKeys.
			if !rec.CanManageKeys && isKeysRoute(req.URL.Path, keysPrefix) {
				return deny(ctx, http.StatusForbidden, "this API key may not manage keys")
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
			// see it. WithContext replaces the request's context in place.
			tctx, err := tenant.WithTenant(req.Context(), tid)
			if err != nil {
				return deny(ctx, http.StatusBadRequest, "invalid tenant id: "+err.Error())
			}
			ctx.WithContext(tctx)

			return next(ctx)
		}
	}
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

// isKeysRoute reports whether path targets the key-management surface rooted at
// keysPrefix. It matches keysPrefix exactly and any sub-path, but not sibling
// paths that merely share the prefix string (e.g. "/admin/keyspace").
func isKeysRoute(path, keysPrefix string) bool {
	return path == keysPrefix || strings.HasPrefix(path, keysPrefix+"/")
}
