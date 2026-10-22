package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/forgeext"
	"github.com/xraph/fabriq/forgeext/adminapi"
)

// serveSettings is everything `fabriq serve` reads from the environment beyond
// the datastore config (which forgeext.LoadConfig binds).
type serveSettings struct {
	HTTPAddr            string        // FABRIQ_HTTP_ADDR, default ":8081"
	ReconcileInterval   time.Duration // FABRIQ_RECONCILE_INTERVAL, default 5m, "0" disables
	DomainPack          string        // FABRIQ_DOMAIN_PACK, "example" (default) or "none"
	DynamicPersist      bool          // FABRIQ_DYNAMIC_PERSIST, default true
	DynamicSyncInterval time.Duration // FABRIQ_DYNAMIC_SYNC_INTERVAL, default 15s
	AdminAPI            bool          // FABRIQ_ADMIN_API, default false
	AdminLoginUser      string        // FABRIQ_ADMIN_LOGIN_USER
	AdminLoginPassword  string        // FABRIQ_ADMIN_LOGIN_PASSWORD
	AllowRLSBypass      bool          // FABRIQ_ALLOW_RLS_BYPASS, default false

	// adminErr holds what is wrong with the admin-only variables
	// (FABRIQ_ADMIN_API, the login pair, FABRIQ_ALLOW_RLS_BYPASS). It fails
	// the server when it starts, not every command: migrate and the other
	// operator commands ignore the admin variables.
	adminErr error
}

// maxLoginPasswordBytes is bcrypt's input limit. A longer password cannot be
// hashed, which would leave the dashboard login answering 401 forever.
const maxLoginPasswordBytes = 72

// loadServeSettings reads the serve settings through getenv. A bad value in a
// variable every command depends on is returned as an error. A bad admin-only
// value is kept in adminErr instead, for registerServe to report at Start.
// Every error names the variable at fault. FABRIQ_ADMIN_KEY is not read here:
// the admin API checks it when it starts.
func loadServeSettings(getenv func(string) string) (serveSettings, error) {
	s := serveSettings{
		HTTPAddr:           getenv("FABRIQ_HTTP_ADDR"),
		DomainPack:         getenv("FABRIQ_DOMAIN_PACK"),
		AdminLoginUser:     getenv("FABRIQ_ADMIN_LOGIN_USER"),
		AdminLoginPassword: getenv("FABRIQ_ADMIN_LOGIN_PASSWORD"),
	}
	if s.HTTPAddr == "" {
		s.HTTPAddr = ":8081"
	}
	if s.DomainPack == "" {
		s.DomainPack = "example"
	}
	if s.DomainPack != "example" && s.DomainPack != "none" {
		return serveSettings{}, fmt.Errorf("unknown FABRIQ_DOMAIN_PACK %q (want example or none)", s.DomainPack)
	}

	var err error
	if s.ReconcileInterval, err = envDuration(getenv, "FABRIQ_RECONCILE_INTERVAL", 5*time.Minute); err != nil {
		return serveSettings{}, err
	}
	if s.DynamicPersist, err = envBool(getenv, "FABRIQ_DYNAMIC_PERSIST", true); err != nil {
		return serveSettings{}, err
	}
	if s.DynamicSyncInterval, err = envDuration(getenv, "FABRIQ_DYNAMIC_SYNC_INTERVAL", 15*time.Second); err != nil {
		return serveSettings{}, err
	}

	var adminErrs []error
	if s.AdminAPI, err = envBool(getenv, "FABRIQ_ADMIN_API", false); err != nil {
		adminErrs = append(adminErrs, err)
	}
	if s.AllowRLSBypass, err = envBool(getenv, "FABRIQ_ALLOW_RLS_BYPASS", false); err != nil {
		adminErrs = append(adminErrs, err)
	}
	if (s.AdminLoginUser == "") != (s.AdminLoginPassword == "") {
		adminErrs = append(adminErrs, errors.New("FABRIQ_ADMIN_LOGIN_USER and FABRIQ_ADMIN_LOGIN_PASSWORD must be set together"))
	}
	if len(s.AdminLoginPassword) > maxLoginPasswordBytes {
		adminErrs = append(adminErrs, fmt.Errorf("FABRIQ_ADMIN_LOGIN_PASSWORD is %d bytes; bcrypt takes at most %d", len(s.AdminLoginPassword), maxLoginPasswordBytes))
	}
	s.adminErr = errors.Join(adminErrs...)
	return s, nil
}

// newServeForgeApp builds the bare forge app `fabriq serve` runs on.
//
// Forge loads the datastore config: it auto-discovers config.yaml (+
// config.local.yaml) from the search paths and overlays FABRIQ_* environment
// variables (env wins). EnvPrefix is pinned to FABRIQ_ explicitly so the
// documented FABRIQ_POSTGRES_DSN-style contract, and the Helm chart that
// injects it, keeps working regardless of how forge derives a default prefix
// from the app name.
func newServeForgeApp(s serveSettings) forge.App {
	return forge.NewApp(forge.AppConfig{
		Name:                      "fabriq",
		Version:                   version,
		HTTPAddress:               s.HTTPAddr,
		EnableConfigAutoDiscovery: true,
		EnableEnvConfig:           true,
		EnvOverridesFile:          true,
		EnvPrefix:                 "FABRIQ_",
		ConfigSearchPaths:         []string{".", "/etc/fabriq"},
	})
}

// buildServeApp builds the `fabriq serve` app over cfg (forgeext.LoadConfig's
// result, or a literal config in tests). It does no I/O: stores are dialled
// and the admin key is checked only when the app starts.
func buildServeApp(s serveSettings, cfg fabriq.Config) (forge.App, error) {
	app := newServeForgeApp(s)
	if err := registerServe(app, s, cfg); err != nil {
		return nil, err
	}
	return app, nil
}

// registerServe registers the worker extension, the admin API when
// s.AdminAPI is on, and the probes on app. It sets cfg.Dynamic from s. When
// s.adminErr is set it also registers a before-start hook that returns it, so
// app.Start fails with that error.
//
// Catalog mode refuses dynamic.persist, so persistence stays off there whatever
// FABRIQ_DYNAMIC_PERSIST says; otherwise its default of true would stop every
// catalog-mode deployment from starting.
func registerServe(app forge.App, s serveSettings, cfg fabriq.Config) error {
	if s.adminErr != nil {
		// Before-start hooks run ahead of every extension's Register and
		// Start, so a bad admin setting stops the server before it dials
		// anything, while setup (and so every other command) still succeeds.
		adminErr := s.adminErr
		if err := app.RegisterHookFn(forge.PhaseBeforeStart, "fabriq-serve-settings", func(context.Context, forge.App) error {
			return adminErr
		}); err != nil {
			return err
		}
	}

	reg := registry.New()
	if err := registerDomainPack(reg, s.DomainPack); err != nil {
		return err
	}
	cfg.Dynamic = fabriq.DynamicConfig{
		Persist:      s.DynamicPersist && !cfg.Catalog.Enabled(),
		SyncInterval: s.DynamicSyncInterval,
	}

	fabricExt := forgeext.New(reg,
		forgeext.WithConfig(cfg),
		forgeext.WithWorker(true),
		forgeext.WithReconcileInterval(s.ReconcileInterval),
		// RLS is the only tenant fence behind the admin API, so a hosted
		// server refuses a role that bypasses it.
		forgeext.WithRequireRLS(s.AdminAPI && !s.AllowRLSBypass),
	)
	if err := app.RegisterExtension(fabricExt); err != nil {
		return err
	}
	checks := []forgeext.ReadyCheck{fabricExt.Ready}

	if s.AdminAPI {
		opts := []adminapi.Option{
			adminapi.WithAuthRequired(),
			adminapi.WithoutDefaultLogin(),
			adminapi.WithTenantsAdmin(),
		}
		if s.AdminLoginUser != "" && s.AdminLoginPassword != "" {
			opts = append(opts, adminapi.WithAdminLogin(s.AdminLoginUser, s.AdminLoginPassword))
		}
		adminExt := adminapi.NewAdminAPI(fabricExt, opts...)
		if err := app.RegisterExtension(adminExt); err != nil {
			return err
		}
		checks = append(checks, adminExt.Ready)
	}

	return forgeext.MountProbes(app.Router(), checks...)
}
