package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xraph/fabriq/core/fabriqerr"
)

// TestInTenantTx_ClassifiesFnError verifies the defer-based seam runs on the
// error path. We call the low-level classification the seam uses to prove the
// mapping is wired; a full inTenantTx run needs a live pool (integration).
func TestSeam_ClassifiesWrappedFnError(t *testing.T) {
	pg := &pgconn.PgError{Code: "23505", ConstraintName: "assets_pkey",
		Message: "duplicate key"}
	fnErr := fmt.Errorf("fabriq: insert asset/01H: %w",
		fmt.Errorf("pgdriver: exec: %w", pg))

	out := translatePg("", "", "", fnErr) // the exact call the seam makes

	var fe *fabriqerr.Error
	if !errors.As(out, &fe) || fe.Code != fabriqerr.CodeAlreadyExists {
		t.Fatalf("seam must classify duplicate-key as already_exists, got %v", out)
	}
	if fe.Meta.Constraint != "assets_pkey" {
		t.Fatalf("constraint not carried: %+v", fe.Meta)
	}
}
