// Command fabriq-worker runs fabriq's background plane as a Forge app:
//
//   - outbox relay        — leader-elected (advisory lock 1001): exactly
//     one active publisher across any number of replicas
//   - projection consumers — phase 4 (graph), phase 5 (search); the
//     consumer-group primitives exist, the appliers land with the engines
//   - reconciler           — phase 6, leader-elected (lock 1002)
//
// Health and metrics come from Forge: /_/livez, /_/readyz, /_/health,
// /_/metrics. Configuration via environment:
//
//	FABRIQ_POSTGRES_DSN  (required)
//	FABRIQ_REDIS_ADDR    (required)
//	FABRIQ_HTTP_ADDR     (default :8081)
package main

import (
	"log"
	"os"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/domain"
)

func main() {
	dsn := os.Getenv("FABRIQ_POSTGRES_DSN")
	redisAddr := os.Getenv("FABRIQ_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		log.Fatal("fabriq-worker: FABRIQ_POSTGRES_DSN and FABRIQ_REDIS_ADDR are required")
	}
	addr := os.Getenv("FABRIQ_HTTP_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	reg := registry.New()
	if err := domain.RegisterAll(reg); err != nil {
		log.Fatalf("fabriq-worker: register domain: %v", err)
	}

	app := forge.NewApp(forge.AppConfig{
		Name:        "fabriq-worker",
		Version:     "0.1.0",
		HTTPAddress: addr,
	})

	ext := newWorkerExtension(reg, fabriq.Config{
		Postgres: fabriq.PostgresConfig{DSN: dsn},
		Redis:    fabriq.RedisConfig{Addr: redisAddr},
	})
	if err := app.RegisterExtension(ext); err != nil {
		log.Fatalf("fabriq-worker: register extension: %v", err)
	}

	if err := app.Run(); err != nil {
		log.Fatalf("fabriq-worker: %v", err)
	}
}
