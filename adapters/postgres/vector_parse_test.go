package postgres

import (
	"math"
	"testing"
)

func TestParseVectorLiteral(t *testing.T) {
	const tol = 1e-6

	tests := []struct {
		name    string
		input   string
		want    []float32
		wantErr bool
	}{
		{
			name:  "three integers",
			input: "[1,2,3]",
			want:  []float32{1, 2, 3},
		},
		{
			name:  "empty vector",
			input: "[]",
			want:  []float32{},
		},
		{
			name:  "negative and scientific notation",
			input: "[-0.5,1e-3]",
			want:  []float32{-0.5, 0.001},
		},
		{
			name:    "malformed: no brackets",
			input:   "not-a-vector",
			wantErr: true,
		},
		{
			name:    "malformed: bad component",
			input:   "[1,x]",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseVectorLiteral(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch: got %d, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if math.Abs(float64(got[i])-float64(tc.want[i])) > tol {
					t.Errorf("component %d: got %v, want %v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
