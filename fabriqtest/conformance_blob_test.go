package fabriqtest_test

import (
	"testing"

	"github.com/xraph/fabriq/conformance"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestBlobConformanceFakes(t *testing.T) {
	conformance.RunBlob(t, fabriqtest.NewConformanceBackend(t))
}
