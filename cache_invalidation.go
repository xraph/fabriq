package fabriq

import (
	"context"

	"github.com/xraph/fabriq/core/cache"
	"github.com/xraph/fabriq/core/command"
)

// cacheInvalidator is the post-commit hook that busts cached reads of every
// entity a committed transaction changed. It bumps each distinct entity's
// generation (which orphans all keyspaces declaring that Entity, across the
// write's partitions). Errors are swallowed deliberately: the write is already
// durable, and the per-keyspace TTL is the backstop for a missed invalidation.
type cacheInvalidator struct {
	c cache.Cache
}

func newCacheInvalidator(c cache.Cache) cacheInvalidator { return cacheInvalidator{c: c} }

// AfterCommit implements command.PostCommitHook.
func (ci cacheInvalidator) AfterCommit(ctx context.Context, changes []command.Change) {
	seen := make(map[string]struct{}, len(changes))
	for _, ch := range changes {
		name := ch.Entity.Spec.Name
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		_ = ci.c.InvalidateEntity(ctx, name) // best-effort; TTL backstops a failure
	}
}

var _ command.PostCommitHook = cacheInvalidator{}
