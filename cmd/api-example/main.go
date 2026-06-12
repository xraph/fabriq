// Command api-example is a minimal TWINOS-style API service on Forge,
// demonstrating fabriq's data plane: commands, queries, and SSE
// fetch-then-subscribe. It represents a Go service behind the Rust
// gateway — it does NOT proxy or terminate anything itself, and its SSE
// responses flush explicitly so they survive reverse proxies.
//
// Auth: the service verifies JWTs ITSELF (HS256 demo) and stamps the
// tenant from validated claims. It never trusts a forwarded header.
//
// Environment:
//
//	FABRIQ_POSTGRES_DSN  (required)
//	FABRIQ_REDIS_ADDR    (required for SSE)
//	FABRIQ_JWT_SECRET    (required)
//	FABRIQ_HTTP_ADDR     (default :8080)
package main

import (
	"context"
	"log"
	"os"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

func main() {
	dsn := os.Getenv("FABRIQ_POSTGRES_DSN")
	secret := os.Getenv("FABRIQ_JWT_SECRET")
	if dsn == "" || secret == "" {
		log.Fatal("api-example: FABRIQ_POSTGRES_DSN and FABRIQ_JWT_SECRET are required")
	}
	addr := os.Getenv("FABRIQ_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		log.Fatalf("api-example: %v", err)
	}

	f, _, err := fabriq.Open(context.Background(), reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: dsn},
		Redis:    fabriq.RedisConfig{Addr: os.Getenv("FABRIQ_REDIS_ADDR")},
	})
	if err != nil {
		log.Fatalf("api-example: open fabriq: %v", err)
	}

	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-api-example",
		Version:     "0.1.0",
		HTTPAddress: addr,
	})

	srv := &server{fabric: f, auth: newAuthenticator([]byte(secret))}
	srv.routes(app.Router())

	runErr := app.Run()
	if cerr := f.Close(); cerr != nil && runErr == nil {
		runErr = cerr
	}
	if runErr != nil {
		log.Fatalf("api-example: %v", runErr)
	}
}
