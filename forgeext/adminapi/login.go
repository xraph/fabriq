package adminapi

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"

	"github.com/xraph/forge"
	"golang.org/x/crypto/bcrypt"
)

// sessionTTL is the lifetime of a dashboard-login session token minted by
// handleLogin. A session is just an expiring KeyStore row (see
// KeyStore.IssueSession), validated by the same authMiddleware path as any
// other API key.
const sessionTTL = 12 * time.Hour

// loginRequest is the request body for POST {BasePath}/login.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// loginResponse is the payload for POST {BasePath}/login. Token is the
// plaintext bearer session token, returned once at login time.
type loginResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expiresAt"`
}

// logoutResponse is the payload for POST {BasePath}/logout.
type logoutResponse struct {
	LoggedOut bool `json:"loggedOut"`
}

// registerLoginRoutes wires the dashboard-login routes (POST .../login and
// POST .../logout) onto the given router. It is called from
// adminController.Routes ONLY when the host configured WithAdminLogin
// (cfg.AdminLoginUser != ""); Extension.Start already fail-fast-checks that
// WithAdminLogin requires WithAuth, so cfg.KeyStore is guaranteed non-nil by
// the time these handlers run. The /login route is also the one path
// authMiddleware exempts from bearer-token verification (see
// authn_middleware.go) — it must be reachable with no Authorization header.
func (c *adminController) registerLoginRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	loginOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.login"),
		forge.WithSummary("Log in with username/password (body: {username, password}); mints a session token"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.POST(base+"/login", c.handleLogin, loginOpts...); err != nil {
		return err
	}

	logoutOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.logout"),
		forge.WithSummary("Log out — revokes the session token presented on this request"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/logout", c.handleLogout, logoutOpts...)
}

// handleLogin serves POST {BasePath}/login.
//
// Request body:
//
//	{ "username": "<name>", "password": "<plaintext>" }
//
// The username is compared in constant time and the password is verified
// against the bcrypt hash configured via WithAdminLogin. Any mismatch — wrong
// username OR wrong password — returns a uniform 401 "invalid credentials" so
// the response never discloses which field was wrong. On success it mints a
// session token via KeyStore.IssueSession and returns 201 with {token,
// expiresAt}.
func (c *adminController) handleLogin(ctx forge.Context) error {
	cfg := c.ext.cfg
	if cfg.AdminLoginUser == "" {
		// Defensive: registerLoginRoutes is only wired when AdminLoginUser is
		// set, so this route should not be reachable when it is empty.
		return forge.NotFound("login is not configured")
	}

	var req loginRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}

	userMatch := subtle.ConstantTimeCompare([]byte(req.Username), []byte(cfg.AdminLoginUser)) == 1
	passErr := bcrypt.CompareHashAndPassword([]byte(cfg.AdminLoginHash), []byte(req.Password))
	if !userMatch || passErr != nil {
		return deny(ctx, http.StatusUnauthorized, "invalid credentials")
	}

	reqCtx := ctx.Request().Context()
	issued, err := cfg.KeyStore.IssueSession(reqCtx, sessionTTL)
	if err != nil {
		return renderError(ctx, err)
	}

	expiresAt := time.Now().UTC().Add(sessionTTL).Format(time.RFC3339)
	return ctx.JSON(http.StatusCreated, loginResponse{
		Token:     issued.Key,
		ExpiresAt: expiresAt,
	})
}

// handleLogout serves POST {BasePath}/logout.
//
// It revokes the session token presented on this request (resolved by
// authMiddleware into the request context as the key id). Unlike /login,
// /logout stays behind auth: a caller must present a valid bearer token to
// revoke it. Returns 200 with {loggedOut: true}.
func (c *adminController) handleLogout(ctx forge.Context) error {
	reqCtx := ctx.Request().Context()
	if id, ok := resolvedKeyID(reqCtx); ok {
		if err := c.ext.cfg.KeyStore.Revoke(reqCtx, id); err != nil {
			return renderError(ctx, err)
		}
	}
	return ctx.JSON(http.StatusOK, logoutResponse{LoggedOut: true})
}
