package shard

import (
	"context"

	"github.com/xraph/fabriq/core/livequery"
)

// Live is the catalog-mode live-query router: it resolves the ctx tenant to
// its shard and serves snapshots/refills from that shard's own database.
// Mirrors the Documents/Store/Relational routers.
type Live struct{ set Router }

// NewLive builds the live-query router over a Router.
func NewLive(set Router) *Live { return &Live{set: set} }

func (l *Live) Snapshot(ctx context.Context, q livequery.LiveQuery, limit int) ([]livequery.Row, error) {
	sh, sctx, release, err := l.set.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return sh.Live.Snapshot(sctx, q, limit)
}

func (l *Live) After(ctx context.Context, q livequery.LiveQuery, after livequery.Cursor, limit int) ([]livequery.Row, error) {
	sh, sctx, release, err := l.set.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return sh.Live.After(sctx, q, after, limit)
}

func (l *Live) Members(ctx context.Context, q livequery.LiveQuery) ([]string, error) {
	sh, sctx, release, err := l.set.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return sh.Live.Members(sctx, q)
}
