package publication

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const publicationImport = "github.com/lychee-technology/forma/internal/publication"

// ddlConsumers are the production files outside this package allowed to
// import it before the writers and the reader move onto the record together
// (#896): init-db's DDL step and its collation catalog check (#895).
var ddlConsumers = map[string]bool{
	"cmd/tools/init_db_publication.go": true,
	"cmd/tools/init_db_collation.go":   true,
}

// ddlIdentifiers are what those files may use: the tables' names and DDL,
// nothing that reads or writes a record.
var ddlIdentifiers = map[string]bool{
	"Statements":            true,
	"TablesFor":             true,
	"RecordTableSuffix":     true,
	"GenerationTableSuffix": true,
}

// TestOnlyInitDBDDLReachesThePublicationTables keeps #895's boundary until
// #896: a record written while V1 cdc-flush still runs goes stale, and the V2
// reader would then answer the schema from the hot leg alone, dropping every
// V1-flushed row. So outside this package only init-db's DDL step names the
// tables, and no production code spells their names itself. #896 replaces
// this guard when it moves the writers and the reader in one change.
func TestOnlyInitDBDDLReachesThePublicationTables(t *testing.T) {
	root := filepath.Join("..", "..")
	files := productionFilesOutsidePublication(t, root)
	require.Greater(t, len(files), 200, "the module's non-test Go files")
	violations, err := publicationViolations(root, files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches a planted writer and a spelled table name", func(t *testing.T) {
		dir := t.TempDir()
		planted := filepath.Join(dir, "internal", "cdc", "planted.go")
		allowed := filepath.Join(dir, "cmd", "tools", "init_db_publication.go")
		for path, src := range map[string]string{
			planted: "package cdc\n\nimport \"" + publicationImport + "\"\n\nvar _ = publication.TablesFor\n\n" +
				"const table = \"entity_main_publication\"\n",
			allowed: "package main\n\nimport pub \"" + publicationImport + "\"\n\nvar _ = pub.Statements\nvar _ = pub.Publish\n",
		} {
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			require.NoError(t, os.WriteFile(path, []byte(src), 0o600))
		}
		violations, err := publicationViolations(dir, []string{planted, allowed})
		require.NoError(t, err)
		require.Equal(t, []string{
			"cmd/tools/init_db_publication.go uses publication.Publish",
			"internal/cdc/planted.go imports " + publicationImport,
			`internal/cdc/planted.go spells a publication table name: "entity_main_publication"`,
		}, violations)
	})
}

// productionFilesOutsidePublication answers the non-test Go files under
// root, skipping this package and hidden and build directories.
func productionFilesOutsidePublication(t *testing.T, root string) []string {
	t.Helper()
	self := filepath.Join(root, "internal", "publication")
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == self || (path != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "build")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}

// publicationViolations answers, sorted, each import of this package from a
// file outside ddlConsumers, each use of an identifier outside ddlIdentifiers,
// and each string literal that spells a publication table's suffix.
func publicationViolations(root string, files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, path := range files {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		rel = filepath.ToSlash(rel)
		violations = append(violations, fileViolations(rel, file)...)
	}
	sort.Strings(violations)
	return violations, nil
}

func fileViolations(rel string, file *ast.File) []string {
	var violations []string
	name := ""
	for _, imp := range file.Imports {
		if path, _ := strconv.Unquote(imp.Path.Value); path != publicationImport {
			continue
		}
		name = "publication"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if !ddlConsumers[rel] {
			violations = append(violations, rel+" imports "+publicationImport)
		}
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			if x, ok := n.X.(*ast.Ident); ok && name != "" && x.Name == name && !ddlIdentifiers[n.Sel.Name] {
				violations = append(violations, rel+" uses publication."+n.Sel.Name)
			}
		case *ast.BasicLit:
			if n.Kind == token.STRING && strings.Contains(n.Value, RecordTableSuffix) {
				violations = append(violations, rel+" spells a publication table name: "+n.Value)
			}
		}
		return true
	})
	return violations
}
