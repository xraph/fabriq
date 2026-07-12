package remote

import (
	"context"

	"github.com/xraph/fabriq/core/query"
)

// remoteAnalytics is the client face of query.AnalyticsQuerier over the
// transport. Not yet wired (ADR 0009 sequencing): every method returns
// ErrNotImplemented until a follow-on task adds the wire messages.
type remoteAnalytics struct{ t Transport }

var _ query.AnalyticsQuerier = remoteAnalytics{}

func (remoteAnalytics) Track(context.Context, []query.AnalyticsEvent) error {
	return ErrNotImplemented
}

func (remoteAnalytics) Query(context.Context, query.AnalyticsQuery, any) error {
	return ErrNotImplemented
}

func (remoteAnalytics) QueryRaw(context.Context, any, string, ...any) error {
	return ErrNotImplemented
}
