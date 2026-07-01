package registry_test

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/xraph/fabriq/core/registry"
)

func TestCoerceToColumn(t *testing.T) {
	ts := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		typ     registry.ColumnType
		in      any
		want    any
		wantErr bool
	}{
		{"text ok", registry.ColText, "hi", "hi", false},
		{"text rejects int", registry.ColText, 3, nil, true},
		{"int from int", registry.ColInt, 3, int64(3), false},
		{"int from int64", registry.ColInt, int64(7), int64(7), false},
		{"int from integral float64", registry.ColInt, float64(3), int64(3), false},
		{"int rejects non-integral float64", registry.ColInt, 3.5, nil, true},
		{"int rejects string", registry.ColInt, "3", nil, true},
		{"float from float64", registry.ColFloat, 2.5, 2.5, false},
		{"float from int", registry.ColFloat, 4, float64(4), false},
		{"bool ok", registry.ColBool, true, true, false},
		{"bool rejects string", registry.ColBool, "true", nil, true},
		{"time from time.Time", registry.ColTime, ts, ts, false},
		{"time from rfc3339", registry.ColTime, "2026-06-30T12:00:00Z", ts, false},
		{"time rejects bad string", registry.ColTime, "nope", nil, true},
		{"json passthrough", registry.ColJSON, map[string]any{"a": 1}, map[string]any{"a": 1}, false},
		{"nil passthrough", registry.ColInt, nil, nil, false},
		{"int rejects overflowing uint64", registry.ColInt, uint64(math.MaxInt64) + 1, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := registry.CoerceToColumn(c.typ, c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %#v, want %#v", got, c.want)
			}
		})
	}
}

func TestColumnTypeString(t *testing.T) {
	cases := []struct {
		typ  registry.ColumnType
		want string
	}{
		{registry.ColText, "text"},
		{registry.ColInt, "int"},
		{registry.ColFloat, "float"},
		{registry.ColBool, "bool"},
		{registry.ColTime, "time"},
		{registry.ColJSON, "json"},
	}
	for _, c := range cases {
		if got := c.typ.String(); got != c.want {
			t.Errorf("ColumnType(%v).String() = %q, want %q", int(c.typ), got, c.want)
		}
	}
}
