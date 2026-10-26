package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/command"
	"github.com/xraph/fabriq/core/registry"
	"github.com/xraph/fabriq/core/tenant"
)

type erasureAuthority func(context.Context, command.ErasureResource, func(context.Context, command.ErasureGrant) error) error

func (f erasureAuthority) WithErasureAuthorization(ctx context.Context, r command.ErasureResource, apply func(context.Context, command.ErasureGrant) error) error {
	return f(ctx, r, apply)
}

type retentionStore struct {
	*fakeStore
	deny     error
	fences   int
	erasures int
}

func (s *retentionStore) InTenantTx(ctx context.Context, apply func(context.Context, command.Tx) error) error {
	return s.fakeStore.InTenantTx(ctx, func(tc context.Context, tx command.Tx) error { return apply(tc, &retentionTx{Tx: tx, store: s}) })
}

type retentionTx struct {
	command.Tx
	store *retentionStore
}

func (tx *retentionTx) FenceAggregate(context.Context, *registry.Entity, string) error {
	tx.store.fences++
	return tx.store.deny
}
func (tx *retentionTx) EraseAggregate(ctx context.Context, _ *registry.Entity, r command.ErasureRequest, g command.ErasureGrant, at time.Time) (command.ErasureReceipt, error) {
	tx.store.erasures++
	tid, _ := tenant.FromContext(ctx)
	return command.ErasureReceipt{TenantID: tid, Request: r, Actor: g.Actor, AuthorityRevision: g.AuthorityRevision, ErasedAt: at, LocalPayloadsErased: true}, nil
}

func TestCommandsFenceErasedAggregateBeforeWrites(t *testing.T) {
	s := &retentionStore{fakeStore: newFakeStore(), deny: errors.New("erased")}
	x, err := command.NewExecutor(cmdRegistry(t), s)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenant.MustWithTenant(context.Background(), "acme")
	_, err = x.Exec(ctx, command.Command{Entity: "site", Op: command.OpCreate, AggID: "reserved", Payload: &cmdSite{Name: "private"}})
	if !errors.Is(err, s.deny) || s.fences != 1 || len(s.outbox) != 0 {
		t.Fatalf("fence=%d error=%v outbox=%d", s.fences, err, len(s.outbox))
	}
}

func TestErasureRequiresHeldCurrentAuthority(t *testing.T) {
	ctx := tenant.MustWithTenant(context.Background(), "acme")
	request := command.ErasureRequest{Entity: "site", AggregateID: "stable", ReasonCode: "expired"}
	for _, mode := range []string{"missing", "denied", "foreign", "valid", "no_callback", "late_callback"} {
		t.Run(mode, func(t *testing.T) {
			s := &retentionStore{fakeStore: newFakeStore()}
			var delayed func(context.Context, command.ErasureGrant) error
			var opts []command.ExecutorOption
			if mode != "missing" {
				opts = append(opts, command.WithErasureAuthorizer(erasureAuthority(func(tc context.Context, r command.ErasureResource, apply func(context.Context, command.ErasureGrant) error) error {
					if r.TenantID != "acme" || r.Request != request {
						t.Fatal("wrong resource")
					}
					if mode == "denied" {
						return errors.New("denied")
					}
					if mode == "no_callback" {
						return nil
					}
					if mode == "late_callback" {
						delayed = apply
						return nil
					}
					if mode == "foreign" {
						tc = tenant.MustWithTenant(tc, "foreign")
					}
					return apply(tc, command.ErasureGrant{Actor: "retention-worker", AuthorityRevision: "1"})
				})))
			}
			x, err := command.NewExecutor(cmdRegistry(t), s, opts...)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := x.EraseAggregate(ctx, request)
			if mode == "valid" {
				if err != nil || s.erasures != 1 || receipt.Actor != "retention-worker" {
					t.Fatalf("%+v %v", receipt, err)
				}
			} else if err == nil || s.erasures != 0 {
				t.Fatalf("erasures=%d err=%v", s.erasures, err)
			}
			if delayed != nil {
				if delayed(ctx, command.ErasureGrant{Actor: "worker", AuthorityRevision: "1"}) == nil || s.erasures != 0 {
					t.Fatal("late callback executed")
				}
			}
		})
	}
}

func TestErasureRejectsDocumentAndMissingCapability(t *testing.T) {
	ctx := tenant.MustWithTenant(context.Background(), "acme")
	auth := erasureAuthority(func(tc context.Context, _ command.ErasureResource, next func(context.Context, command.ErasureGrant) error) error {
		return next(tc, command.ErasureGrant{Actor: "worker", AuthorityRevision: "1"})
	})
	x, err := command.NewExecutor(cmdRegistry(t), newFakeStore(), command.WithErasureAuthorizer(auth))
	if err != nil {
		t.Fatal(err)
	}
	for _, entity := range []string{"page", "missing", "site"} {
		_, err = x.EraseAggregate(ctx, command.ErasureRequest{Entity: entity, AggregateID: "identity", ReasonCode: "expired"})
		if err == nil {
			t.Fatalf("unsupported %s erasure accepted", entity)
		}
	}
}

type blockedRetentionStore struct {
	*retentionStore
	entered, canceled, release chan struct{}
}

func (s *blockedRetentionStore) InTenantTx(ctx context.Context, apply func(context.Context, command.Tx) error) error {
	return s.retentionStore.InTenantTx(ctx, func(tc context.Context, tx command.Tx) error { return apply(tc, blockedRetentionTx{Tx: tx, store: s}) })
}

type blockedRetentionTx struct {
	command.Tx
	store *blockedRetentionStore
}

func (t blockedRetentionTx) FenceAggregate(ctx context.Context, e *registry.Entity, id string) error {
	return t.Tx.(command.ErasureTx).FenceAggregate(ctx, e, id)
}
func (t blockedRetentionTx) EraseAggregate(ctx context.Context, _ *registry.Entity, _ command.ErasureRequest, _ command.ErasureGrant, _ time.Time) (command.ErasureReceipt, error) {
	close(t.store.entered)
	<-ctx.Done()
	close(t.store.canceled)
	<-t.store.release
	return command.ErasureReceipt{}, ctx.Err()
}
func TestErasureCancelsEscapedAuthorityCallback(t *testing.T) {
	ctx := tenant.MustWithTenant(context.Background(), "acme")
	s := &blockedRetentionStore{retentionStore: &retentionStore{fakeStore: newFakeStore()}, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	completed := make(chan error, 1)
	auth := erasureAuthority(func(_ context.Context, _ command.ErasureResource, next func(context.Context, command.ErasureGrant) error) error {
		// Even a supplied context independent of the caller must not escape the guard.
		supplied := tenant.MustWithTenant(context.Background(), "acme")
		go func() { completed <- next(supplied, command.ErasureGrant{Actor: "worker", AuthorityRevision: "1"}) }()
		<-s.entered
		return nil
	})
	x, err := command.NewExecutor(cmdRegistry(t), s, command.WithErasureAuthorizer(auth))
	if err != nil {
		t.Fatal(err)
	}
	returned := make(chan error, 1)
	go func() {
		_, e := x.EraseAggregate(ctx, command.ErasureRequest{Entity: "site", AggregateID: "identity", ReasonCode: "expired"})
		returned <- e
	}()
	select {
	case err = <-returned:
		if err == nil {
			t.Fatal("escaped authority returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("authority return did not close callback lifetime")
	}
	select {
	case <-s.canceled:
	case <-time.After(time.Second):
		t.Fatal("escaped transaction was not canceled")
	}
	close(s.release)
	if err = <-completed; err == nil {
		t.Fatal("escaped action returned success")
	}
	if s.erasures != 0 {
		t.Fatal("escaped callback committed erasure")
	}
}
