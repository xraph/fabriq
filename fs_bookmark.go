package fabriq

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// AddBookmark bookmarks nodeID for userID. The (tenant, user, node) unique index
// rejects duplicates (surfaced as an error).
func (f *Fabriq) AddBookmark(ctx context.Context, userID, nodeID string, sortOrder int) (string, error) {
	res, err := f.exec.Exec(ctx, command.Command{
		Entity: "fs_bookmark", Op: command.OpCreate,
		Payload: &domain.FsBookmark{UserID: userID, NodeID: nodeID, SortOrder: sortOrder, CreatedAt: time.Now().UTC()},
	})
	if err != nil {
		return "", fmt.Errorf("fabriq: AddBookmark: %w", err)
	}
	return res.AggID, nil
}

// ListUserBookmarks returns a user's bookmarks, ordered by sort_order.
func (f *Fabriq) ListUserBookmarks(ctx context.Context, userID string) ([]domain.FsBookmark, error) {
	var rows []domain.FsBookmark
	err := f.Relational().List(ctx, "fs_bookmark", query.ListQuery{
		Where: query.Where{query.Eq("user_id", userID)}, OrderBy: "sort_order ASC",
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: ListUserBookmarks: %w", err)
	}
	return rows, nil
}

// RemoveBookmark removes a bookmark by id.
func (f *Fabriq) RemoveBookmark(ctx context.Context, id string) error {
	if _, err := f.exec.Exec(ctx, command.Command{Entity: "fs_bookmark", Op: command.OpDelete, AggID: id}); err != nil {
		return fmt.Errorf("fabriq: RemoveBookmark: %w", err)
	}
	return nil
}
