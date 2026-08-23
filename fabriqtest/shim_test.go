package fabriqtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// exportedTopLevel returns the exported top-level declaration names in a
// package directory, ignoring _test.go files.
//
// This walks the directory and calls parser.ParseFile rather than using
// parser.ParseDir, which is deprecated along with the ast.Package type it
// returns.
func exportedTopLevel(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	out := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					out[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, s := range d.Specs {
					switch s := s.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							out[s.Name.Name] = true
						}
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if n.IsExported() {
								out[n.Name] = true
							}
						}
					}
				}
			}
		}
	}
	return out
}

// TestShimCoversCoreFabriqtest fails when a symbol is added to
// core/fabriqtest without a forwarding entry here. Without this the shim
// rots silently: nothing breaks until an external consumer upgrades and
// cannot find the name they expected.
func TestShimCoversCoreFabriqtest(t *testing.T) {
	core := exportedTopLevel(t, "../core/fabriqtest")
	shim := exportedTopLevel(t, ".")

	var missing []string
	for name := range core {
		if !shim[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("core/fabriqtest exports %v with no entry in fabriqtest/shim.go", missing)
	}
}
