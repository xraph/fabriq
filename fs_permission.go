package fabriq

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/query"
	"github.com/xraph/fabriq/domain"
)

// GrantPermission grants principal (type+id) `permission` on nodeID.
func (f *Fabriq) GrantPermission(ctx context.Context, nodeID, principalType, principalID, permission, grantedBy string) (string, error) {
	res, err := f.exec.Exec(ctx, command.Command{
		Entity: "fs_permission", Op: command.OpCreate,
		Payload: &domain.FsPermission{
			NodeID: nodeID, PrincipalType: principalType, PrincipalID: principalID,
			Permission: permission, GrantedBy: grantedBy, CreatedAt: time.Now().UTC(),
		},
	})
	if err != nil {
		return "", fmt.Errorf("fabriq: GrantPermission: %w", err)
	}
	return res.AggID, nil
}

// RevokePermission removes a permission grant by id.
func (f *Fabriq) RevokePermission(ctx context.Context, id string) error {
	if _, err := f.exec.Exec(ctx, command.Command{Entity: "fs_permission", Op: command.OpDelete, AggID: id}); err != nil {
		return fmt.Errorf("fabriq: RevokePermission: %w", err)
	}
	return nil
}

// ListNodePermissions returns all grants on nodeID.
func (f *Fabriq) ListNodePermissions(ctx context.Context, nodeID string) ([]domain.FsPermission, error) {
	var rows []domain.FsPermission
	err := f.Relational().List(ctx, "fs_permission", query.ListQuery{
		Where: query.Where{query.Eq("node_id", nodeID)}, OrderBy: "created_at ASC",
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: ListNodePermissions: %w", err)
	}
	return rows, nil
}

// ListPrincipalPermissions returns all grants to a principal.
func (f *Fabriq) ListPrincipalPermissions(ctx context.Context, principalType, principalID string) ([]domain.FsPermission, error) {
	var rows []domain.FsPermission
	err := f.Relational().List(ctx, "fs_permission", query.ListQuery{
		Where:   query.Where{query.Eq("principal_type", principalType), query.Eq("principal_id", principalID)},
		OrderBy: "created_at ASC",
	}, &rows)
	if err != nil {
		return nil, fmt.Errorf("fabriq: ListPrincipalPermissions: %w", err)
	}
	return rows, nil
}
