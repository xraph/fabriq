package fabriq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// CreateShareInput carries a share's fields. The seam supplies Token (generated)
// and PasswordHash (bcrypt) — fabriq persists them verbatim.
type CreateShareInput struct {
	NodeID       string     `json:"nodeId"`
	Token        string     `json:"token"`
	Permission   string     `json:"permission"`
	ExpiresAt    *time.Time `json:"expiresAt"`
	MaxDownloads *int       `json:"maxDownloads"`
	PasswordHash string     `json:"-"`
	CreatedBy    string     `json:"createdBy"`
}

// CreateShare persists a share record.
func (f *Fabriq) CreateShare(ctx context.Context, in CreateShareInput) (string, error) {
	perm := in.Permission
	if perm == "" {
		perm = "read"
	}
	res, err := f.exec.Exec(ctx, command.Command{
		Entity: "fs_share", Op: command.OpCreate,
		Payload: &domain.FsShare{
			NodeID: in.NodeID, Token: in.Token, Permission: perm, ExpiresAt: in.ExpiresAt,
			MaxDownloads: in.MaxDownloads, PasswordHash: in.PasswordHash, CreatedBy: in.CreatedBy,
			CreatedAt: time.Now().UTC(),
		},
	})
	if err != nil {
		return "", fmt.Errorf("fabriq: CreateShare: %w", err)
	}
	return res.AggID, nil
}

// GetShareByToken resolves a share by its token (DATA only — expiry/cap/password
// enforcement is the seam's job).
func (f *Fabriq) GetShareByToken(ctx context.Context, token string) (domain.FsShare, error) {
	var rows []domain.FsShare
	err := f.Relational().List(ctx, "fs_share", query.ListQuery{
		Where: query.Where{query.Eq("token", token)}, Limit: 1,
	}, &rows)
	if err != nil {
		return domain.FsShare{}, fmt.Errorf("fabriq: GetShareByToken: %w", err)
	}
	if len(rows) == 0 {
		return domain.FsShare{}, fmt.Errorf("fabriq: GetShareByToken %q: %w", token, fabriqerr.ErrNotFound)
	}
	return rows[0], nil
}

// IncrementShareDownload atomically bumps download_count via a command-plane
// read-modify-write with optimistic concurrency (one retry on version conflict).
func (f *Fabriq) IncrementShareDownload(ctx context.Context, id string) error {
	for attempt := 0; attempt < 2; attempt++ {
		var s domain.FsShare
		if err := f.Relational().Get(ctx, "fs_share", id, &s); err != nil {
			return fmt.Errorf("fabriq: IncrementShareDownload: %w", err)
		}
		s.DownloadCount++
		expected := s.Version
		_, err := f.exec.Exec(ctx, command.Command{
			Entity: "fs_share", Op: command.OpUpdate, AggID: id, Payload: &s, ExpectedVersion: &expected,
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, fabriqerr.ErrVersionConflict) {
			continue // re-read and retry once
		}
		return fmt.Errorf("fabriq: IncrementShareDownload: %w", err)
	}
	return fmt.Errorf("fabriq: IncrementShareDownload: version conflict after retry")
}

// DeleteShare removes a share record.
func (f *Fabriq) DeleteShare(ctx context.Context, id string) error {
	if _, err := f.exec.Exec(ctx, command.Command{Entity: "fs_share", Op: command.OpDelete, AggID: id}); err != nil {
		return fmt.Errorf("fabriq: DeleteShare: %w", err)
	}
	return nil
}

// ListNodeShares returns all shares for a node.
func (f *Fabriq) ListNodeShares(ctx context.Context, nodeID string) ([]domain.FsShare, error) {
	var rows []domain.FsShare
	err := f.Relational().List(ctx, "fs_share", query.ListQuery{
		Where: query.Where{query.Eq("node_id", nodeID)}, OrderBy: "created_at ASC",
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: ListNodeShares: %w", err)
	}
	return rows, nil
}
