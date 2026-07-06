package analytics_test

import (
	"testing"

	"github.com/xraph/fabriq/core/analytics"
)

func BenchmarkApplier_SkipUnmarked(b *testing.B) {
	a := analytics.NewApplier(regWith(nil))
	e := env("widget", "widget.updated", 1, `{"name":"a"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, ok, _ := a.Apply(e)
		if ok {
			b.Fatal("expected skip")
		}
	}
}
