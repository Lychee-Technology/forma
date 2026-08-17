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
// command builds must set the field to the run's own s3Client.
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
			if !litSetsFieldToIdent(lit, "ObjectReader", "s3Client") {
				t.Errorf("%s:%d builds a compaction.Compactor whose ObjectReader is not the run's s3Client; merged base entries would go unstamped (#347)",
					name, fset.Position(lit.Pos()).Line)
			}
			// The parsed flags are the only carrier of
			// --skip-input-checksum-verify; a Compactor built with a Config
			// from anywhere else would silently ignore the operator's opt-out
			// while TestParseCompactorFlags_InputChecksumVerify stayed green.
			if !litSetsFieldToSelector(lit, "Config", "opts", "compact") {
				t.Errorf("%s:%d builds a compaction.Compactor whose Config is not opts.compact; the parsed compactor flags would not reach compaction (#347)",
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

// litSetsFieldToSelector is litSetsFieldToIdent for a `recv.field` value, e.g.
// `Config: opts.compact`.
func litSetsFieldToSelector(lit *ast.CompositeLit, field, wantRecv, wantField string) bool {
	for _, elt := range lit.Elts {
		kv, isKV := elt.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}
		key, isKeyIdent := kv.Key.(*ast.Ident)
		if !isKeyIdent || key.Name != field {
			continue
		}
		sel, isSel := kv.Value.(*ast.SelectorExpr)
		if !isSel || sel.Sel.Name != wantField {
			return false
		}
		recv, isRecvIdent := sel.X.(*ast.Ident)
		return isRecvIdent && recv.Name == wantRecv
	}
	return false
}

// litSetsFieldToIdent reports whether a keyed composite literal assigns the
// named identifier to the field. Presence alone is too weak a pin: `ObjectReader:
// nil` compiles and would keep this guard green while shipping unstamped bases,
// so the value is checked too (mirrors internal/cdc's callPassesOptsS3Client).
func litSetsFieldToIdent(lit *ast.CompositeLit, field, want string) bool {
	for _, elt := range lit.Elts {
		kv, isKV := elt.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}
		key, isKeyIdent := kv.Key.(*ast.Ident)
		if !isKeyIdent || key.Name != field {
			continue
		}
		value, isValueIdent := kv.Value.(*ast.Ident)
		return isValueIdent && value.Name == want
	}
	return false
}
