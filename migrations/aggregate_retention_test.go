package migrations

import (
	"context"
	"testing"
)

func TestAggregateRetentionRefusesRollbackWithoutStorage(t *testing.T) {
	if err := migration0036AggregateRetention.Down(context.Background(), nil); err == nil {
		t.Fatal("retention migration allowed rollback without checking storage")
	}
}
