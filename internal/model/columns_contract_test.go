package model

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// isDeprecated reports whether a declaration carries the Go "Deprecated:"
// marker, on the const block or on the spec itself.
func isDeprecated(gen *ast.GenDecl, vs *ast.ValueSpec) bool {
	for _, doc := range []*ast.CommentGroup{gen.Doc, vs.Doc} {
		if doc != nil && strings.Contains(doc.Text(), "Deprecated:") {
			return true
		}
	}
	return false
}

// mainColumnConstants reads every forma.MainColumn constant out of the root
// package's source, split into the active set and the ones marked
// Deprecated, so the contracts below cannot be satisfied by a stale
// hand-written list: a constant added, removed or deprecated in
// schema_registry.go is seen here without touching this test.
func mainColumnConstants(t *testing.T) (active, deprecated []string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join("..", "..", "schema_registry.go"), nil, parser.ParseComments)
	require.NoError(t, err)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			if ident, ok := vs.Type.(*ast.Ident); !ok || ident.Name != "MainColumn" {
				continue
			}
			for _, v := range vs.Values {
				lit, ok := v.(*ast.BasicLit)
				require.True(t, ok && lit.Kind == token.STRING, "MainColumn constant %v is not a string literal", vs.Names)
				name, err := strconv.Unquote(lit.Value)
				require.NoError(t, err)
				if isDeprecated(gen, vs) {
					deprecated = append(deprecated, name)
				} else {
					active = append(active, name)
				}
			}
		}
	}
	require.NotEmpty(t, active, "no forma.MainColumn constants found")
	sort.Strings(active)
	sort.Strings(deprecated)
	return active, deprecated
}

// #585: the public forma.MainColumn* constants and the runtime column set
// (the writer's allowlist, the read projection, the CDC column order) are the
// same set. A constant outside the set would pass compilation and be refused
// at registration (#557); a runtime column without a constant could only be
// bound by its string. The DDL copies are pinned to the same set by
// EntityMainDDLDrift in their own packages.
func TestMainColumnConstantsMatchRuntimeSet(t *testing.T) {
	runtime := make([]string, 0, len(EntityMainColumnDescriptors))
	for _, desc := range EntityMainColumnDescriptors {
		runtime = append(runtime, desc.Name)
	}
	sort.Strings(runtime)
	active, _ := mainColumnConstants(t)
	require.Equal(t, runtime, active)
}

// A constant kept only for source compatibility (marked Deprecated) names a
// column the runtime does not have: it must never be silently promoted into
// the set, because the marker is what tells a caller that a binding to it is
// refused at registration (#557). Removing the block is a deliberate act and
// leaves this test green.
func TestDeprecatedMainColumnConstantsAreOutsideRuntimeSet(t *testing.T) {
	_, deprecated := mainColumnConstants(t)
	for _, name := range deprecated {
		require.False(t, IsMainTableColumn(name), "deprecated constant %q is a runtime column: either undeprecate it or drop it from the runtime set", name)
	}
}

// ColumnType() infers a column's type from its name; for every runtime column
// that inference agrees with the descriptor's kind, so the #459 round-trip
// matrix judges a binding by the column's real type.
func TestColumnTypeAgreesWithDescriptorKind(t *testing.T) {
	kindOf := map[forma.MainColumnType]ColumnKind{
		forma.MainColumnTypeText:     ColumnKindText,
		forma.MainColumnTypeSmallint: ColumnKindSmallint,
		forma.MainColumnTypeInteger:  ColumnKindInteger,
		forma.MainColumnTypeBigint:   ColumnKindBigint,
		forma.MainColumnTypeDouble:   ColumnKindDouble,
		forma.MainColumnTypeUUID:     ColumnKindUUID,
	}
	for _, desc := range EntityMainColumnDescriptors {
		binding := forma.MainColumnBinding{ColumnName: forma.MainColumn(desc.Name)}
		require.Equal(t, desc.Kind, kindOf[binding.ColumnType()], "column %s", desc.Name)
	}
}
