package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/fabriq/core/query"
)

// defaultSeries is the telemetry table the demo writes readings into. fabriq's
// TSQuerier addresses a series by its physical table name; tag_readings is the
// example-domain telemetry table (migration 0003), which runs as a plain table
// on non-Timescale Postgres (migration 0005 converts it to a hypertable only
// when the timescaledb extension is present).
const defaultSeries = "tag_readings"

// defaultRangeWindow is the look-back applied when a range request omits "from".
const defaultRangeWindow = 24 * time.Hour

// maxRangePoints caps how many points a single range response may carry, so a
// wide window against a dense series cannot return an unbounded payload.
const maxRangePoints = 5000

// tsKeysResponse is the payload for GET {BasePath}/timeseries/keys.
type tsKeysResponse struct {
	Series string   `json:"series"`
	Keys   []string `json:"keys"`
}

// tsRangeRequest is the request body for POST {BasePath}/timeseries/range.
//
// It reads the half-open window [From, To) of one series key. When BucketSeconds
// is > 0 the raw points are downsampled into fixed-width buckets using Agg (the
// adapter's own bucketing is not yet implemented, so bucketing is applied
// in-memory here — see handleTimeseriesRange).
type tsRangeRequest struct {
	// Series is the telemetry table name. Defaults to tag_readings.
	Series string `json:"series"`
	// Key is the series key (tag id) to read. Required.
	Key string `json:"key"`
	// From/To bound the half-open window [from, to), RFC3339. When omitted, To
	// defaults to now and From to now-24h.
	From string `json:"from"`
	To   string `json:"to"`
	// BucketSeconds, when > 0, downsamples raw points into fixed-width buckets.
	BucketSeconds int `json:"bucketSeconds"`
	// Agg selects the bucket aggregation: avg|min|max|last (default avg).
	Agg string `json:"agg"`
}

// tsPoint is one sample in the range response.
type tsPoint struct {
	At      time.Time `json:"at"`
	Value   float64   `json:"value"`
	Quality int       `json:"quality"`
}

// tsRangeResponse is the payload for POST {BasePath}/timeseries/range.
type tsRangeResponse struct {
	Series   string    `json:"series"`
	Key      string    `json:"key"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Bucketed bool      `json:"bucketed"`
	Agg      string    `json:"agg,omitempty"`
	Points   []tsPoint `json:"points"`
}

// registerTimeseriesRoutes wires the telemetry read routes onto the given
// router. They share the same route options (auth/tenant middleware) as the rest
// of the admin surface so the host controls the security boundary uniformly.
func (c *adminController) registerTimeseriesRoutes(r forge.Router) error {
	base := c.ext.cfg.BasePath
	routeOpts := c.ext.cfg.RouteOptions

	keysOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.timeseries.keys"),
		forge.WithSummary("List distinct telemetry series keys (query: ?series=)"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	if err := r.GET(base+"/timeseries/keys", c.handleTimeseriesKeys, keysOpts...); err != nil {
		return err
	}

	rangeOpts := append([]forge.RouteOption{
		forge.WithName("fabriq.admin.timeseries.range"),
		forge.WithSummary("Read a telemetry range (body: {series?, key, from?, to?, bucketSeconds?, agg?})"),
		forge.WithTags("Fabriq", "Admin"),
	}, routeOpts...)
	return r.POST(base+"/timeseries/range", c.handleTimeseriesRange, rangeOpts...)
}

// handleTimeseriesKeys serves GET {BasePath}/timeseries/keys.
//
// It returns the distinct series keys (tag ids) present for the request tenant.
// The telemetry table carries no row-level security (it is the hypertable, whose
// columnstore forbids RLS), so the distinct scan is scoped to the tenant
// explicitly via the app.tenant_id GUC that the relational tenant-tx stamps.
//
// Returns 501 when the instance has no timeseries backend configured.
func (c *adminController) handleTimeseriesKeys(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	series := strings.TrimSpace(ctx.Query("series"))
	if series == "" {
		series = defaultSeries
	}
	if !validSeriesName(series) {
		return forge.BadRequest("invalid series name")
	}

	reqCtx := ctx.Request().Context()
	if !timeseriesConfigured(reqCtx, fab.Timeseries()) {
		return c.timeseriesNotConfigured(ctx)
	}

	// series is validated to [A-Za-z_][A-Za-z0-9_]*, so it is safe to interpolate
	// into the identifier position without quoting. The tenant filter uses the
	// app.tenant_id GUC the relational tenant-tx sets (current_setting(..., true)
	// returns '' when unset, which matches no tenant — a safe closed default).
	var rows []struct {
		Key string `grove:"key"`
	}
	sql := fmt.Sprintf(`SELECT DISTINCT tag_id AS key FROM %s
		WHERE tenant_id = current_setting('app.tenant_id', true)
		ORDER BY key ASC`, series)
	if qErr := fab.Relational().Query(reqCtx, &rows, sql); qErr != nil {
		return renderError(ctx, qErr)
	}

	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, r.Key)
	}
	return ctx.JSON(http.StatusOK, tsKeysResponse{Series: series, Keys: keys})
}

// handleTimeseriesRange serves POST {BasePath}/timeseries/range.
//
// Request body:
//
//	{ "series": "tag_readings", "key": "cpu.load", "from": "...", "to": "...",
//	  "bucketSeconds": 300, "agg": "avg" }
//
// series defaults to tag_readings; an omitted window reads the last 24h. Raw
// points are read from the TSQuerier and, when bucketSeconds > 0, downsampled
// in-memory (the adapter's own time_bucket path is not yet implemented, so the
// endpoint reads raw and aggregates here to honour the bucketed contract).
//
// Returns 501 when the instance has no timeseries backend, and 400 on invalid
// series/key/window.
func (c *adminController) handleTimeseriesRange(ctx forge.Context) error {
	fab, err := c.ext.resolveFabric()
	if err != nil {
		return forge.InternalError(err)
	}

	var req tsRangeRequest
	if decErr := json.NewDecoder(ctx.Request().Body).Decode(&req); decErr != nil {
		return forge.BadRequest("invalid request body: " + decErr.Error())
	}

	series := strings.TrimSpace(req.Series)
	if series == "" {
		series = defaultSeries
	}
	if !validSeriesName(series) {
		return forge.BadRequest("invalid series name")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return forge.BadRequest("field 'key' is required")
	}

	to := time.Now()
	if req.To != "" {
		t, perr := time.Parse(time.RFC3339, req.To)
		if perr != nil {
			return forge.BadRequest("field 'to' must be an RFC3339 timestamp: " + perr.Error())
		}
		to = t
	}
	from := to.Add(-defaultRangeWindow)
	if req.From != "" {
		t, perr := time.Parse(time.RFC3339, req.From)
		if perr != nil {
			return forge.BadRequest("field 'from' must be an RFC3339 timestamp: " + perr.Error())
		}
		from = t
	}
	if !from.Before(to) {
		return forge.BadRequest("'from' must be before 'to'")
	}

	reqCtx := ctx.Request().Context()
	ts := fab.Timeseries()
	if !timeseriesConfigured(reqCtx, ts) {
		return c.timeseriesNotConfigured(ctx)
	}

	// Always read raw points (Bucket = 0): the adapter rejects bucketed Range as
	// not-yet-implemented, so bucketing is done in-memory below.
	var raw []query.Point
	rq := query.RangeQuery{Series: series, Key: key, From: from, To: to}
	if rErr := ts.Range(reqCtx, rq, &raw); rErr != nil {
		return renderError(ctx, rErr)
	}

	resp := tsRangeResponse{Series: series, Key: key, From: from, To: to}
	if req.BucketSeconds > 0 {
		agg := strings.ToLower(strings.TrimSpace(req.Agg))
		if agg == "" {
			agg = "avg"
		}
		bucketed, aggErr := bucketPoints(raw, time.Duration(req.BucketSeconds)*time.Second, agg)
		if aggErr != nil {
			return forge.BadRequest(aggErr.Error())
		}
		resp.Bucketed = true
		resp.Agg = agg
		resp.Points = bucketed
	} else {
		resp.Points = rawToPoints(raw)
	}
	if len(resp.Points) > maxRangePoints {
		resp.Points = resp.Points[:maxRangePoints]
	}
	return ctx.JSON(http.StatusOK, resp)
}

// rawToPoints projects the neutral query.Point slice onto the response shape.
func rawToPoints(points []query.Point) []tsPoint {
	out := make([]tsPoint, 0, len(points))
	for _, p := range points {
		out = append(out, tsPoint{At: p.At, Value: p.Value, Quality: p.Quality})
	}
	return out
}

// bucketPoints downsamples time-ascending raw points into fixed-width buckets,
// aggregating each bucket by agg (avg|min|max|last). Range returns points in
// ascending time order, so same-bucket points are contiguous and a single pass
// suffices. The bucket timestamp is the bucket's floored start (UTC).
func bucketPoints(points []query.Point, width time.Duration, agg string) ([]tsPoint, error) {
	switch agg {
	case "avg", "min", "max", "last":
	default:
		return nil, fmt.Errorf("field 'agg' must be one of avg|min|max|last, got %q", agg)
	}
	if width <= 0 {
		return nil, fmt.Errorf("field 'bucketSeconds' must be a positive number")
	}
	out := make([]tsPoint, 0)
	if len(points) == 0 {
		return out, nil
	}

	w := width.Nanoseconds()
	type bucket struct {
		start   time.Time
		sum     float64
		count   int
		min     float64
		max     float64
		last    float64
		quality int
	}
	var cur *bucket
	flush := func() {
		if cur == nil {
			return
		}
		var v float64
		switch agg {
		case "avg":
			v = cur.sum / float64(cur.count)
		case "min":
			v = cur.min
		case "max":
			v = cur.max
		case "last":
			v = cur.last
		}
		out = append(out, tsPoint{At: cur.start, Value: v, Quality: cur.quality})
	}
	for _, p := range points {
		bStart := time.Unix(0, (p.At.UnixNano()/w)*w).UTC()
		if cur == nil || !cur.start.Equal(bStart) {
			flush()
			cur = &bucket{start: bStart, min: p.Value, max: p.Value}
		}
		cur.sum += p.Value
		cur.count++
		if p.Value < cur.min {
			cur.min = p.Value
		}
		if p.Value > cur.max {
			cur.max = p.Value
		}
		cur.last = p.Value
		cur.quality = p.Quality
	}
	flush()
	return out, nil
}

// validSeriesName reports whether s is a safe SQL identifier ([A-Za-z_] then
// [A-Za-z0-9_]*). It is both the injection guard for the interpolated series
// name and a friendly 400 gate (the adapter would otherwise reject it deeper).
func validSeriesName(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// timeseriesConfigured probes the timeseries port: an unconfigured stub answers
// Range with ErrStoreNotConfigured; the real adapter rejects the empty-series
// probe with a validation error first (not the sentinel), so "not
// ErrStoreNotConfigured" == configured. Mirrors the other capability probes.
func timeseriesConfigured(ctx context.Context, ts query.TSQuerier) bool {
	var pts []query.Point
	return !notConfigured(ts.Range(ctx, query.RangeQuery{}, &pts))
}

// timeseriesNotConfigured returns the 501 response used when the instance has no
// timeseries backend wired, mirroring the not-configured shape used across the
// admin surface so the SPA can branch on a stable error payload.
func (c *adminController) timeseriesNotConfigured(ctx forge.Context) error {
	return ctx.JSON(http.StatusNotImplemented, map[string]string{"error": "timeseries not configured"})
}
