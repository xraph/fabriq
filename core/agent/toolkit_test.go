// core/agent/toolkit_test.go
package agent

import (
	"testing"

	"github.com/xraph/fabriq/fabriqtest"
)

func TestNewToolkit_AppliesDefaults(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	tk, err := NewToolkit(ff, reg, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if tk.cfg.K != defaultK || tk.cfg.Hops != defaultHops || tk.cfg.VectorDims != defaultVectorDims {
		t.Fatalf("defaults not applied: %+v", tk.cfg)
	}
	if tk.cfg.Tokenizer == nil {
		t.Fatal("default tokenizer not set")
	}
}

func TestNewToolkit_RejectsDimMismatch(t *testing.T) {
	reg := testRegistry(t)
	ff := newFakeFabric(t, fabriqtest.NewWorld(reg))
	if _, err := NewToolkit(ff, reg, embedderStub{dims: 1536}, Config{VectorDims: 768}); err == nil {
		t.Fatal("want dim-mismatch error")
	}
}

func TestNewToolkit_NilArgs(t *testing.T) {
	reg := testRegistry(t)
	if _, err := NewToolkit(nil, reg, nil, Config{}); err == nil {
		t.Fatal("want nil-Fabric error")
	}
}
