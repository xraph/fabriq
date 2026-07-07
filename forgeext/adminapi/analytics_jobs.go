package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/event"
	"github.com/xraph/fabriq/core/subscribe"
)

// analyticsJob is one async analytics bulk operation (fleet backfill, reproject,
// or reconcile). The synchronous single-tenant forms stay inline; only the
// fleet-wide forms — which can run long enough to exceed an HTTP timeout — are
// offered as a background job with a pollable id.
type analyticsJob struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`  // "backfill" | "reproject" | "reconcile"
	State     string          `json:"state"` // "running" | "done" | "failed"
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	StartedAt time.Time       `json:"startedAt"`
	EndedAt   *time.Time      `json:"endedAt,omitempty"`
}

// analyticsJobs is an in-memory job registry. Unlike migrations (single-flight),
// analytics bulk ops are idempotent and version-gated, so overlapping jobs are
// allowed — each is tracked by id.
type analyticsJobs struct {
	mu   sync.Mutex
	jobs map[string]*analyticsJob
}

func newAnalyticsJobs() *analyticsJobs { return &analyticsJobs{jobs: map[string]*analyticsJob{}} }

// start records a running job and runs it in a detached goroutine so it
// outlives the request. run returns a JSON-marshalable result (the per-tenant
// counts / reports) stored on completion.
func (j *analyticsJobs) start(kind string, run func(context.Context) (any, error)) *analyticsJob {
	job := &analyticsJob{ID: event.NewID(), Kind: kind, State: "running", StartedAt: time.Now()}
	j.mu.Lock()
	j.jobs[job.ID] = job
	j.mu.Unlock()

	go func() {
		res, err := run(context.Background())
		j.mu.Lock()
		defer j.mu.Unlock()
		now := time.Now()
		job.EndedAt = &now
		if err != nil {
			job.State, job.Error = "failed", err.Error()
			return
		}
		job.State = "done"
		if res != nil {
			if b, merr := json.Marshal(res); merr == nil {
				job.Result = b
			}
		}
	}()
	return job
}

func (j *analyticsJobs) get(id string) (*analyticsJob, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	job, ok := j.jobs[id]
	return job, ok
}

// handleAnalyticsJob serves GET {BasePath}/analytics/jobs/:id — poll one job.
func (c *adminController) handleAnalyticsJob(ctx forge.Context) error {
	if !c.ext.cfg.AnalyticsAdmin {
		return forge.Forbidden("analytics admin not enabled (host must opt in via WithAnalyticsAdmin)")
	}
	job, ok := c.analyticsJobs.get(ctx.Param("id"))
	if !ok {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such job"})
	}
	c.analyticsJobs.mu.Lock()
	snap := *job
	c.analyticsJobs.mu.Unlock()
	return ctx.JSON(http.StatusOK, snap)
}

// handleAnalyticsJobStream serves GET {BasePath}/analytics/jobs/:id/stream — an
// SSE stream of the job's state (every ~500ms) until it reaches a terminal
// state (done|failed) or the client disconnects.
func (c *adminController) handleAnalyticsJobStream(ctx forge.Context) error {
	if !c.ext.cfg.AnalyticsAdmin {
		return forge.Forbidden("analytics admin not enabled (host must opt in via WithAnalyticsAdmin)")
	}
	job, ok := c.analyticsJobs.get(ctx.Param("id"))
	if !ok {
		return ctx.JSON(http.StatusNotFound, map[string]string{"error": "no such job"})
	}
	sse, err := subscribe.NewSSEWriter(ctx.Response())
	if err != nil {
		return err
	}
	reqCtx := ctx.Request().Context()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.analyticsJobs.mu.Lock()
		snap := *job
		c.analyticsJobs.mu.Unlock()

		_ = sse.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if werr := sse.WriteEvent(snap.ID, "state", snap); werr != nil {
			return nil // client gone
		}
		if snap.State != "running" {
			return nil
		}
		select {
		case <-reqCtx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
