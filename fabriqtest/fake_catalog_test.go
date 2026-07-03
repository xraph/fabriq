package fabriqtest_test

import (
	"testing"

	"github.com/xraph/fabriq/core/catalog"
	"github.com/xraph/fabriq/core/catalog/catalogtest"
	"github.com/xraph/fabriq/fabriqtest"
)

func TestFakeCatalog_Contract(t *testing.T) {
	catalogtest.Run(t, func(_ *testing.T) catalog.Catalog {
		return fabriqtest.NewFakeCatalog()
	})
}
