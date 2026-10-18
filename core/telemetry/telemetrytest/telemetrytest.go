// Package telemetrytest holds test helpers for fabriq's tracing: a span
// recorder installed as the global tracer provider, and a scan that proves a
// value never reached a span.
package telemetrytest

import (
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Record installs a span recorder as the global tracer provider for the
// rest of the test and restores the previous provider on cleanup. Tests that
// call it must not run in parallel with other tests that read the global
// provider.
func Record(t testing.TB) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(t.Context())
	})
	return rec
}

// Named returns the ended spans called name.
func Named(rec *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

// AssertNoValue fails the test if value appears in any ended span's name,
// attributes, events, event attributes, links or status description.
func AssertNoValue(t testing.TB, rec *tracetest.SpanRecorder, value string) {
	t.Helper()
	for _, s := range rec.Ended() {
		check := func(where, got string) {
			if strings.Contains(got, value) {
				t.Errorf("span %q %s carries %q: %q", s.Name(), where, value, got)
			}
		}
		check("name", s.Name())
		check("status", s.Status().Description)
		for _, kv := range s.Attributes() {
			check("attribute "+string(kv.Key), kv.Value.String())
		}
		for _, ev := range s.Events() {
			check("event", ev.Name)
			for _, kv := range ev.Attributes {
				check("event attribute "+string(kv.Key), kv.Value.String())
			}
		}
		for _, l := range s.Links() {
			for _, kv := range l.Attributes {
				check("link attribute "+string(kv.Key), kv.Value.String())
			}
		}
	}
}

// Attr returns the string form of a span attribute and whether it is set.
func Attr(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String(), true
		}
	}
	return "", false
}
