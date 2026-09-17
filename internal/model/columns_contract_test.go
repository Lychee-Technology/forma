package model

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// mainColumnConstants reads every forma.MainColumn constant out of the root
// package's source, so the contract below cannot be satisfied by a stale
// hand-written list: a constant added or removed in schema_registry.go is
// seen here without touching this test.
func mainColumnConstants(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join("..", "..", "schema_registry.go"), nil, 0)
	require.NoError(t, err)
	var names []string
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
				names = append(names, name)
			}
		}
	}
	require.NotEmpty(t, names, "no forma.MainColumn constants found")
	sort.Strings(names)
	return names
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
	require.Equal(t, runtime, mainColumnConstants(t))
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
