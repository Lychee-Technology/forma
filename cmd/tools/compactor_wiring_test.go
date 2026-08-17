package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestCompactorWiringSetsObjectReader guards the production compactor's
// checksum wiring (#347). runCompactor needs AWS config and a DuckDB merge
// engine, so no unit test can drive it end to end; without this guard the
// ObjectReader assignment could be deleted and every merged base would silently
// go unstamped in production while internal/compaction's own tests stayed green
// (#318). The guard is over the source: every compaction.Compactor literal this
// command builds must set the field.
func TestCompactorWiringSetsObjectReader(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	fset := token.NewFileSet()
	sites := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			lit, isLit := n.(*ast.CompositeLit)
			if !isLit || !isCompactionCompactor(lit.Type) {
				return true
			}
			sites++
			if !litSetsField(lit, "ObjectReader") {
				t.Errorf("%s:%d builds a compaction.Compactor without ObjectReader; merged base entries would go unstamped (#347)",
					name, fset.Position(lit.Pos()).Line)
			}
			return true
		})
	}

	if sites == 0 {
		t.Fatal("found no compaction.Compactor construction in cmd/tools; the guard has lost its subject")
	}
}

// isCompactionCompactor reports whether a composite-literal type is
// compaction.Compactor.
func isCompactionCompactor(expr ast.Expr) bool {
	sel, isSel := expr.(*ast.SelectorExpr)
	if !isSel || sel.Sel.Name != "Compactor" {
		return false
	}
	pkg, isIdent := sel.X.(*ast.Ident)
	return isIdent && pkg.Name == "compaction"
}

// litSetsField reports whether a keyed composite literal assigns the field.
func litSetsField(lit *ast.CompositeLit, field string) bool {
	for _, elt := range lit.Elts {
		kv, isKV := elt.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}
		if key, isIdent := kv.Key.(*ast.Ident); isIdent && key.Name == field {
			return true
		}
	}
	return false
}
