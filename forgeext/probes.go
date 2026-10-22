package forgeext

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/xraph/forge"
)

// readyCheckTimeout bounds each readiness check. A probe that hangs is worse
// than one that fails, because the orchestrator keeps the pod in rotation
// while it waits.
const readyCheckTimeout = 2 * time.Second

// ReadyCheck reports whether one dependency is ready to serve. A nil return
// means ready. The context carries the probe's deadline, but a check that
// ignores it still cannot hold the probe past readyCheckTimeout.
type ReadyCheck func(ctx context.Context) error

// MountProbes registers the orchestrator probes on r:
//
//	GET /_/livez   always 200 {"status":"alive"}
//	GET /_/readyz  200 {"status":"ready"} when every check passes, otherwise
//	               503 {"status":"not ready","reason":"<first error>"}
//
// forge registers its own /_/health, /_/health/live and /_/health/ready, but
// those report ready during a startup grace period whatever the state of the
// stores. These two have no grace period: readyz is ready when the checks say
// so. Checks run in order, each under a 2s timeout, and the first failure
// stops the run.
func MountProbes(r forge.Router, checks ...ReadyCheck) error {
	if err := r.GET("/_/livez", func(ctx forge.Context) error {
		return ctx.JSON(http.StatusOK, map[string]string{"status": "alive"})
	}); err != nil {
		return err
	}
	return r.GET("/_/readyz", func(ctx forge.Context) error {
		if err := runReadyChecks(ctx.Request().Context(), checks); err != nil {
			return ctx.JSON(http.StatusServiceUnavailable, map[string]string{
				"status": "not ready",
				"reason": err.Error(),
			})
		}
		return ctx.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})
}

// runReadyChecks runs the checks in order and returns the first failure.
func runReadyChecks(ctx context.Context, checks []ReadyCheck) error {
	for _, check := range checks {
		if err := runReadyCheck(ctx, check); err != nil {
			return err
		}
	}
	return nil
}

// runReadyCheck runs one check under readyCheckTimeout. The check runs in its
// own goroutine so a check that never looks at its context still yields at the
// deadline. The result channel is buffered, so that goroutine ends as soon as
// the check returns and never blocks on a reader that has given up.
func runReadyCheck(ctx context.Context, check ReadyCheck) error {
	cctx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- check(cctx) }()
	select {
	case err := <-done:
		return err
	case <-cctx.Done():
		if ctx.Err() == nil {
			return fmt.Errorf("readiness check timed out after %s", readyCheckTimeout)
		}
		return ctx.Err()
	}
}
