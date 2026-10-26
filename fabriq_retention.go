package fabriq

import (
	"context"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
)

// ErrAggregateErased identifies an aggregate permanently retired by retention.
var ErrAggregateErased = fabriqerr.ErrAggregateErased

// WithErasureAuthorizer enables local aggregate retention under current host grants.
func WithErasureAuthorizer(authorizer command.ErasureAuthorizer) Option {
	return func(s *settings) {
		s.executorOptions = append(s.executorOptions, command.WithErasureAuthorizer(authorizer))
	}
}

// EraseAggregate removes the receipt's named local payload targets and reserves
// the identity. External copies remain unconfirmed unless separately reconciled.
func (f *Fabriq) EraseAggregate(ctx context.Context, request command.ErasureRequest) (command.ErasureReceipt, error) {
	return f.exec.EraseAggregate(ctx, request)
}
