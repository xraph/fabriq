package forgeext

import (
	"testing"
	"time"
)

func TestWithBlobGCGrace(t *testing.T) {
	var c Config
	WithBlobGCGrace(2 * time.Hour)(&c)
	if c.BlobGCGrace != 2*time.Hour {
		t.Fatalf("BlobGCGrace = %v, want 2h", c.BlobGCGrace)
	}
}
