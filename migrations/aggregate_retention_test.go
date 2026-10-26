package migrations

import (
	"context"
	"testing"
)

func TestAggregateRetentionRefusesRollback(t *testing.T) {
	if err := migration0036AggregateRetention.Down(context.Background(), nil); err == nil {
		t.Fatal("retention migration allowed permanent fences to be removed")
	}
}
