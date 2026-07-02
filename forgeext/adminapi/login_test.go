package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/fabriqtest"
)

// sessionKeyStore extends liveKeyStore (from keys_test.go) with a real
// IssueSession implementation — liveKeyStore's own IssueSession is a stub
// that errors, since the keys tests never exercise it. It mints a fresh key
// exactly like liveKeyStore.Issue, but additionally stamps ExpiresAt, so a
// session both authenticates (Lookup finds it, CanManageKeys true) and
// eventually expires like the real relationalKeyStore.IssueSession.
type sessionKeyStore struct {
	*liveKeyStore
}

func newSessionKeyStore() *sessionKeyStore {
	return &sessionKeyStore{liveKeyStore: newLiveKeyStore()}
}

func (s *sessionKeyStore) IssueSession(_ context.Context, ttl time.Duration) (IssuedKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	id := fmt.Sprintf("session-%d", s.seq)
	key, prefix, hash, err := generateKey()
	if err != nil {
		return IssuedKey{}, err
	}
	createdAt := time.Now().UTC()
	expiresAt := createdAt.Add(ttl)
	rec := &KeyRecord{
		ID:            id,
		Prefix:        prefix,
		Label:         "session",
		CanManageKeys: true,
		CreatedAt:     createdAt,
		ExpiresAt:     &expiresAt,
	}
	s.byHash[hash] = rec
	s.byID[id] = rec
	return IssuedKey{ID: id, Prefix: prefix, Key: key}, nil
}

var _ KeyStore = (*sessionKeyStore)(nil)

// buildLoginExt builds an Extension backed by a fresh test world with auth
// enabled (WithAuth over an in-memory sessionKeyStore, mirroring buildKeysExt
// in keys_test.go) AND WithAdminLogin configured, plus the auth middleware
// installed via WithRouteOptions so /login is reachable unauthenticated and
// every other route (including /logout) is gated end-to-end.
func buildLoginExt(t *testing.T) (*Extension, *sessionKeyStore) {
	t.Helper()
	world := buildTestWorld(t)
	store := newSessionKeyStore()

	e := NewAdminAPI(nil,
		WithAuth(store),
		WithAdminLogin("admin", "s3cret"),
		WithRouteOptions(forge.WithMiddleware(authMiddleware(store, "/admin"))),
	)
	e.fabric = fabriqtest.NewFabric(world)
	e.reg = world.Registry
	return e, store
}

// postJSON issues a POST to srv's /admin/login with the given JSON body.
func postJSON(t *testing.T, srv, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv+"/admin/login", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestLogin_ValidCredentials_IssuesSessionToken(t *testing.T) {
	e, _ := buildLoginExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postJSON(t, srv.URL, `{"username":"admin","password":"s3cret"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201, body = %s", resp.StatusCode, b)
	}

	var got loginResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Token == "" {
		t.Error("token must not be empty")
	}
	if got.ExpiresAt == "" {
		t.Error("expiresAt must not be empty")
	}

	// The issued token must itself authenticate against the admin surface
	// (multi-tenant session key, so X-Tenant-ID is required).
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/admin/meta", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+got.Token)
	req.Header.Set("X-Tenant-ID", testTenantID)
	metaResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer metaResp.Body.Close()
	if metaResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(metaResp.Body)
		t.Fatalf("issued token auth status = %d, want 200, body = %s", metaResp.StatusCode, b)
	}
}

func TestLogin_WrongPassword_401(t *testing.T) {
	e, _ := buildLoginExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postJSON(t, srv.URL, `{"username":"admin","password":"wrong"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 401, body = %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "invalid credentials") {
		t.Errorf("body = %s, want it to contain %q", b, "invalid credentials")
	}
}

func TestLogin_WrongUsername_401(t *testing.T) {
	e, _ := buildLoginExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postJSON(t, srv.URL, `{"username":"nope","password":"s3cret"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 401, body = %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "invalid credentials") {
		t.Errorf("body = %s, want it to contain %q", b, "invalid credentials")
	}
}

// TestLogin_ReachableWithoutBearerToken verifies that POST /login is exempted
// from authMiddleware's bearer-token check — a keyless request must still
// reach the handler (and fail on credentials, not on missing Authorization).
func TestLogin_ReachableWithoutBearerToken(t *testing.T) {
	e, _ := buildLoginExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postJSON(t, srv.URL, `{"username":"admin","password":"s3cret"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201 (login must be reachable with no Authorization header), body = %s", resp.StatusCode, b)
	}
}

func TestLogout_RevokesPresentedToken(t *testing.T) {
	e, store := buildLoginExt(t)
	srv := buildServer(t, e)
	defer srv.Close()

	loginResp := postJSON(t, srv.URL, `{"username":"admin","password":"s3cret"}`)
	defer loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(loginResp.Body)
		t.Fatalf("login status = %d, body = %s", loginResp.StatusCode, b)
	}
	var logged loginResponse
	if err := json.NewDecoder(loginResp.Body).Decode(&logged); err != nil {
		t.Fatalf("decode login: %v", err)
	}

	// Resolve the key id the token maps to, so we can assert it was revoked.
	rec, found, err := store.Lookup(t.Context(), hashKey(logged.Token))
	if err != nil || !found {
		t.Fatalf("lookup issued token: found=%v err=%v", found, err)
	}
	if rec.RevokedAt != nil {
		t.Fatal("token must not be revoked before logout")
	}

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/admin/logout", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+logged.Token)
	req.Header.Set("X-Tenant-ID", testTenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("logout status = %d, want 200, body = %s", resp.StatusCode, b)
	}
	var got logoutResponse
	if derr := json.NewDecoder(resp.Body).Decode(&got); derr != nil {
		t.Fatalf("decode logout: %v", derr)
	}
	if !got.LoggedOut {
		t.Error("loggedOut must be true")
	}

	// The store must now show the key as revoked.
	rec2, found2, err2 := store.Lookup(t.Context(), hashKey(logged.Token))
	if err2 != nil || !found2 {
		t.Fatalf("lookup after logout: found=%v err=%v", found2, err2)
	}
	if rec2.RevokedAt == nil {
		t.Error("token must be revoked after logout")
	}

	// The revoked token must no longer authenticate.
	req2, err := http.NewRequest(http.MethodGet, srv.URL+"/admin/meta", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req2.Header.Set("Authorization", "Bearer "+logged.Token)
	req2.Header.Set("X-Tenant-ID", testTenantID)
	metaResp, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer metaResp.Body.Close()
	if metaResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("post-logout meta status = %d, want 401", metaResp.StatusCode)
	}
}

func TestWithAdminLogin_RequiresWithAuth_FailsFast(t *testing.T) {
	e := NewAdminAPI(nil, WithAdminLogin("admin", "s3cret"))
	if err := e.Start(t.Context()); err == nil {
		t.Fatal("Start must fail when WithAdminLogin is set without WithAuth")
	} else if !strings.Contains(err.Error(), "WithAdminLogin requires WithAuth") {
		t.Errorf("err = %v, want it to mention WithAdminLogin requires WithAuth", err)
	}
}

func TestWithAdminLogin_NotConfigured_RoutesNotRegistered(t *testing.T) {
	// No WithAdminLogin at all: /login must not be registered (404), and the
	// rest of the package stays green (auth-off unaffected).
	world := buildTestWorld(t)
	e := fakeBackedAdminExt(t, world)
	srv := buildServer(t, e)
	defer srv.Close()

	resp := postJSON(t, srv.URL, `{"username":"admin","password":"s3cret"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 404 (route must not be registered), body = %s", resp.StatusCode, b)
	}
}
