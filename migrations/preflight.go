package migrations

import (
	"context"
	"fmt"
	"strings"

	"github.com/xraph/grove/drivers/pgdriver"
)

// ExtensionStatus is one required extension's state on the target server.
type ExtensionStatus struct {
	Name      string `json:"name"`
	Available bool   `json:"available"`
	Installed string `json:"installed"` // installed version, "" when not yet created
	Preloaded bool   `json:"preloaded"` // timescaledb: in shared_preload_libraries; others: true
	OK        bool   `json:"ok"`
}

// ExtensionReport is the result of CheckExtensions.
type ExtensionReport struct {
	OK         bool              `json:"ok"`
	Extensions []ExtensionStatus `json:"extensions"`
	Problems   []string          `json:"problems"` // e.g. "timescaledb: not in shared_preload_libraries"
}

// DefaultRequiredExtensions are the extensions fabriq's migrations create.
var DefaultRequiredExtensions = []string{"timescaledb", "vector", "postgis"}

// CheckExtensions reports whether each required extension can be created on
// the server behind dsn. An extension is OK when the server ships it
// (pg_available_extensions) and, for timescaledb, when it is loaded through
// shared_preload_libraries, because CREATE EXTENSION timescaledb succeeds
// without the preload but the database then misbehaves. It does not create
// anything. The error is for connection and query failures only; a missing
// extension is reported in the returned report.
func CheckExtensions(ctx context.Context, dsn string, required []string) (ExtensionReport, error) {
	pg := pgdriver.New()
	if err := pg.Open(ctx, dsn); err != nil {
		return ExtensionReport{}, fmt.Errorf("fabriq: preflight: connect to postgres: %w", redactSecrets(err, dsn, ""))
	}
	defer func() { _ = pg.Close() }()

	rows, err := pg.Query(ctx, `SELECT name, COALESCE(installed_version, '') FROM pg_available_extensions`)
	if err != nil {
		return ExtensionReport{}, fmt.Errorf("fabriq: preflight: list available extensions: %w", err)
	}
	available := map[string]string{}
	for rows.Next() {
		var name, installed string
		if err := rows.Scan(&name, &installed); err != nil {
			_ = rows.Close()
			return ExtensionReport{}, fmt.Errorf("fabriq: preflight: scan available extensions: %w", err)
		}
		available[name] = installed
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return ExtensionReport{}, fmt.Errorf("fabriq: preflight: list available extensions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ExtensionReport{}, fmt.Errorf("fabriq: preflight: list available extensions: %w", err)
	}

	var preload string
	if err := pg.QueryRow(ctx, `SELECT current_setting('shared_preload_libraries')`).Scan(&preload); err != nil {
		return ExtensionReport{}, fmt.Errorf("fabriq: preflight: read shared_preload_libraries: %w", err)
	}
	preloaded := preloadedLibraries(preload)

	rep := ExtensionReport{OK: true, Extensions: make([]ExtensionStatus, 0, len(required)), Problems: []string{}}
	for _, name := range required {
		installed, ok := available[name]
		st := ExtensionStatus{Name: name, Available: ok, Installed: installed, Preloaded: true}
		if name == "timescaledb" {
			st.Preloaded = preloaded[name]
		}
		st.OK = st.Available && st.Preloaded
		switch {
		case !st.Available:
			rep.Problems = append(rep.Problems, name+": not available")
		case !st.Preloaded:
			rep.Problems = append(rep.Problems, name+": not in shared_preload_libraries")
		}
		if !st.OK {
			rep.OK = false
		}
		rep.Extensions = append(rep.Extensions, st)
	}
	return rep, nil
}

// preloadedLibraries parses the shared_preload_libraries setting, a comma
// separated list whose items may be quoted and carry a path or a .so suffix.
func preloadedLibraries(setting string) map[string]bool {
	out := map[string]bool{}
	for _, item := range strings.Split(setting, ",") {
		item = strings.Trim(strings.TrimSpace(item), `"'`)
		if i := strings.LastIndex(item, "/"); i >= 0 {
			item = item[i+1:]
		}
		item = strings.TrimSuffix(item, ".so")
		if item != "" {
			out[item] = true
		}
	}
	return out
}
