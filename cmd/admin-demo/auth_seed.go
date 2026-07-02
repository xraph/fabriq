package main

import (
	"context"
	"fmt"
	"log"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/fabriq/forgeext/adminapi"
)

// setupDemoAuth is the ADMIN_DEMO_AUTH=1 opt-in path: it dials a small
// dedicated grove/pg pool (independent of both the prep fabric's pool, which
// closes right after this runs, and the serving forgeext extension's pool,
// which does not exist yet at this point in startup), builds an
// adminapi.KeyStore over it, seeds one CanManageKeys admin key per tenant
// (idempotent — only when the tenant has no live manage key yet), prints a
// ready-to-paste fabriq:// DSN per tenant, and returns the adminapi.WithAuth
// option to fold into the extension's option list.
//
// The dedicated pool is intentionally never closed: it must outlive this
// function, since the auth middleware installed by WithAuth resolves every
// admin request against this same store for the life of the process (mirrors
// how the demo's other long-lived handles, e.g. stores.Postgres, are never
// explicitly closed either — the process exit reclaims them).
func setupDemoAuth(ctx context.Context, dsn, addr string, tenants []string) (adminapi.Option, error) {
	pg := pgdriver.New()
	if err := pg.Open(ctx, dsn, driver.WithPoolSize(4)); err != nil {
		return nil, fmt.Errorf("admin-demo: auth: open dedicated pg pool: %w", err)
	}
	gdb, err := grove.Open(pg)
	if err != nil {
		_ = pg.Close()
		return nil, fmt.Errorf("admin-demo: auth: grove.Open: %w", err)
	}

	store := adminapi.NewKeyStore(gdb)

	existing, err := store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("admin-demo: auth: list existing keys: %w", err)
	}
	hasLiveManageKey := make(map[string]bool, len(tenants))
	for _, rec := range existing {
		if rec.CanManageKeys && rec.RevokedAt == nil {
			hasLiveManageKey[rec.TenantID] = true
		}
	}

	log.Printf("admin-demo: ADMIN_DEMO_AUTH=1 — auth ENABLED, admin routes require 'Authorization: Bearer <key>'")
	for _, tid := range tenants {
		if hasLiveManageKey[tid] {
			log.Printf("  auth: tenant %q already has an admin key (skipping issue)", tid)
			continue
		}
		issued, err := store.Issue(ctx, adminapi.KeySpec{
			Label:         "admin-demo " + tid,
			TenantID:      tid,
			CanManageKeys: true,
		})
		if err != nil {
			return nil, fmt.Errorf("admin-demo: auth: issue key for tenant %q: %w", tid, err)
		}
		log.Printf("  auth: issued admin key for tenant %q — DSN: %s", tid, demoAuthDSN(issued.Key, addr, tid))
	}

	return adminapi.WithAuth(store), nil
}

// demoAuthDSN formats a ready-to-paste fabriq:// DSN embedding the bearer key,
// the demo's listen address (host defaulted to localhost — ADMIN_DEMO_ADDR is
// typically just a ":port"), and the tenant path segment.
func demoAuthDSN(key, addr, tenant string) string {
	host := addr
	if len(host) > 0 && host[0] == ':' {
		host = "localhost" + host
	}
	return fmt.Sprintf("fabriq://%s@%s/%s?tls=false", key, host, tenant)
}
