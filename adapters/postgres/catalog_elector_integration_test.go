//go:build integration

package postgres_test

import (
	"context"
	"sync"
	"testing"

	"github.com/xraph/fabriq/adapters/postgres"
	"github.com/xraph/fabriq/fabriqtest"
)

// The catalog control DB can elect the singleton reconciler: exactly one of
// two campaigners wins the lock.
func TestCatalogStore_ElectsSingleLeader(t *testing.T) {
	ctx := context.Background()
	dsn := fabriqtest.StartPostgres(t)
	cat, err := postgres.OpenCatalog(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	const key = int64(1002)
	held := make(chan struct{})
	holderCtx, stopHolder := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = cat.Elector(key).Run(holderCtx, func(c context.Context) error {
			close(held)
			<-c.Done()
			return c.Err()
		})
	}()
	<-held

	led, err := cat.Elector(key).TryLead(ctx, func(context.Context) error { return nil })
	if err != nil || led {
		t.Fatalf("second campaigner won a held lock: led=%v err=%v", led, err)
	}

	// Release the held lock and join the holder goroutine BEFORE t.Cleanup
	// closes the catalog's pool — otherwise pool.Close() blocks forever
	// waiting for the still-checked-out dedicated connection to be released.
	stopHolder()
	wg.Wait()
}
