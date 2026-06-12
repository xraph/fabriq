package command

import "github.com/xraph/fabriq/core/fabriqerr"

// checkVersion enforces existence and optimistic-concurrency rules against
// the stored version.
func checkVersion(p *preparedCommand, current int64) error {
	switch p.cmd.Op {
	case OpCreate:
		if current != 0 {
			return &fabriqerr.VersionConflictError{
				Aggregate: p.entity.Spec.Name, AggID: p.aggID, Expected: 0, Actual: current,
			}
		}
	case OpUpdate, OpDelete:
		if current == 0 {
			return &fabriqerr.NotFoundError{Entity: p.entity.Spec.Name, ID: p.aggID}
		}
	}
	if p.cmd.ExpectedVersion != nil && *p.cmd.ExpectedVersion != current {
		return &fabriqerr.VersionConflictError{
			Aggregate: p.entity.Spec.Name, AggID: p.aggID,
			Expected: *p.cmd.ExpectedVersion, Actual: current,
		}
	}
	return nil
}
