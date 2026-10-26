package command

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/xraph/fabriq/core/fabriqerr"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

// WithErasureAuthorizer enables explicit host-authorized local payload erasure.
// Without this option the executor exposes no deletion authority.
func WithErasureAuthorizer(authorizer ErasureAuthorizer) ExecutorOption {
	return func(x *Executor) { x.erasureAuthorizer = authorizer }
}

func validErasureGrant(g ErasureGrant) bool {
	for _, value := range []string{g.Actor, g.AuthorityRevision} {
		if strings.TrimSpace(value) == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
	}
	return true
}

// EraseAggregate irreversibly reserves one aggregate identity and removes its
// local command-store payloads. Current host authority encloses the transaction;
// retries return the retained receipt and cannot restore deleted content.
func (x *Executor) EraseAggregate(ctx context.Context, request ErasureRequest) (ErasureReceipt, error) {
	if err := request.Validate(ctx); err != nil {
		return ErasureReceipt{}, err
	}
	if x.erasureAuthorizer == nil {
		return ErasureReceipt{}, fabriqerr.ErrStoreNotConfigured
	}
	value := reflect.ValueOf(x.erasureAuthorizer)
	if (value.Kind() == reflect.Pointer || value.Kind() == reflect.Func || value.Kind() == reflect.Interface) && value.IsNil() {
		return ErasureReceipt{}, fabriqerr.ErrStoreNotConfigured
	}
	ent, ok := x.reg.Get(request.Entity)
	if !ok || ent.Spec.Kind != registry.KindAggregate {
		return ErasureReceipt{}, fabriqerr.New(fabriqerr.CodeInvalidInput, "Aggregate retention requires a registered aggregate.")
	}
	tid, _ := tenant.FromContext(ctx)
	resource := ErasureResource{TenantID: tid, Request: request}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	active, started, finished := true, false, false
	var operationErr, protocolErr error
	var result ErasureReceipt
	denied := func() error {
		return fabriqerr.New(fabriqerr.CodePermissionDenied, "Erasure authorization is unavailable.")
	}
	err := x.erasureAuthorizer.WithErasureAuthorization(attemptCtx, resource, func(tc context.Context, grant ErasureGrant) error {
		mu.Lock()
		if !active || started {
			protocolErr = denied()
			invalid := protocolErr
			mu.Unlock()
			return invalid
		}
		started = true
		mu.Unlock()
		var receipt ErasureReceipt
		perform := func() error {
			if tc == nil || tc.Err() != nil || attemptCtx.Err() != nil {
				return denied()
			}
			actual, tenantErr := tenant.Require(tc)
			if tenantErr != nil || actual != tid || request.Validate(tc) != nil || !validErasureGrant(grant) {
				return denied()
			}
			// Keep host identity values while preventing its replacement context
			// from outliving the authority invocation or the original caller.
			actionCtx, stop := context.WithCancel(attemptCtx)
			defer stop()
			stopHostCancel := context.AfterFunc(tc, stop)
			defer stopHostCancel()
			return x.store.InTenantTx(erasureContext{Context: actionCtx, values: tc}, func(txctx context.Context, tx Tx) error {
				receipt = ErasureReceipt{}
				if err := actionCtx.Err(); err != nil {
					return err
				}
				retention, supported := tx.(ErasureTx)
				if !supported {
					return fabriqerr.ErrStoreNotConfigured
				}
				var eraseErr error
				receipt, eraseErr = retention.EraseAggregate(txctx, ent, request, grant, x.now().UTC())
				if eraseErr != nil {
					return eraseErr
				}
				if err := tc.Err(); err != nil {
					return err
				}
				return actionCtx.Err()
			})
		}
		callbackErr := perform()
		mu.Lock()
		if !active {
			receipt = ErasureReceipt{}
			if callbackErr == nil {
				callbackErr = denied()
			}
		}
		result, operationErr, finished = receipt, callbackErr, true
		mu.Unlock()
		return callbackErr
	})
	mu.Lock()
	active = false
	done := started && finished
	receipt, callbackErr, invalid := result, operationErr, protocolErr
	mu.Unlock()
	cancel()
	if err != nil {
		return ErasureReceipt{}, err
	}
	if invalid != nil {
		return ErasureReceipt{}, invalid
	}
	if !done {
		return ErasureReceipt{}, denied()
	}
	if callbackErr != nil {
		return ErasureReceipt{}, callbackErr
	}
	if err := ctx.Err(); err != nil {
		return ErasureReceipt{}, err
	}
	return receipt, nil
}

// erasureContext preserves the host's principal values without accepting an
// independent lifetime for a privileged transaction.
type erasureContext struct {
	context.Context
	values context.Context
}

func (c erasureContext) Value(key any) any { return c.values.Value(key) }
