package publication

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const publicationImport = "github.com/lychee-technology/forma/internal/publication"

// manifestV2Import is the package that produces and fetches V2 manifest
// objects (#898). It may import this package freely, and nothing in
// production may import it until #896 moves the writers and the reader.
//
// The two directories are Go packages, not trees: a subpackage of either is
// another package, held to every rule.
const (
	manifestV2Import = "github.com/lychee-technology/forma/internal/manifestv2"
	manifestV2Dir    = "internal/manifestv2"
	publicationDir   = "internal/publication"
)

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

// TestOnlyInitDBDDLReachesThePublicationTables is a static import and
// literal check that keeps an early dependency on this package out until
// #896: a record written while V1 cdc-flush still runs goes stale, and the V2
// reader would then answer the schema from the hot leg alone, dropping every
// V1-flushed row. It scans every production file of the module, and applies
// two rules:
//
//   - The publication rule, to every file outside this package: it refuses an
//     import of this package outside ddlConsumers and the internal/manifestv2
//     package, a dot import anywhere (it would hide which identifiers a file
//     uses), a use of anything but ddlIdentifiers in ddlConsumers, and a
//     string literal that spells a publication table name.
//   - The dormancy rule, to every file outside the internal/manifestv2
//     package, this package's and the ddlConsumers included: it refuses a
//     production import of internal/manifestv2, so no binary reaches it
//     before #896.
//
// Exemptions are by package directory, never by subtree: a subpackage of
// either package is held to both rules.
//
// It is not a guarantee against publication record I/O: an allowlisted file
// could still build a statement over a name TablesFor returns. What shows that
// init-db sends no such statement is the traced SQL of each run in
// TestInitDBCreatesThePublicationTablesOnceAndWritesNoRecord (cmd/tools).
// #896 replaces this guard when it moves the writers and the reader in one
// change.
func TestOnlyInitDBDDLReachesThePublicationTables(t *testing.T) {
	root := filepath.Join("..", "..")
	files, err := productionFiles(root)
	require.NoError(t, err)
	require.Greater(t, len(files), 200, "the module's non-test Go files")
	require.Contains(t, files, filepath.Join(root, publicationDir, "publish.go"), "this package's own files are scanned for the dormancy rule")
	violations, err := publicationViolations(root, files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches a planted writer, a dot import, a spelled table name and a manifest caller", func(t *testing.T) {
		dir := t.TempDir()
		for rel, src := range map[string]string{
			"internal/cdc/planted.go": "package cdc\n\nimport \"" + publicationImport + "\"\n\nvar _ = publication.TablesFor\n\n" +
				"const table = \"entity_main_publication\"\n",
			"cmd/tools/init_db_publication.go": "package main\n\nimport pub \"" + publicationImport + "\"\n\nvar _ = pub.Statements\nvar _ = pub.Publish\n",
			// An allowlisted file whose dot import would let it call
			// Publish with no selector for the guard to see.
			"cmd/tools/init_db_collation.go": "package main\n\nimport . \"" + publicationImport + "\"\n\nvar _ = Publish\n",
			// A production caller of the dormant manifest package.
			"internal/federated/planted.go": "package federated\n\nimport \"" + manifestV2Import + "\"\n\nvar _ = manifestv2.Verify\n",
			// The manifest package itself may use all of this package,
			// and import itself, but never spell a table.
			"internal/manifestv2/planted.go": "package manifestv2\n\nimport (\n\t\"" + publicationImport + "\"\n\t_ \"" + manifestV2Import + "\"\n)\n\n" +
				"var _ = publication.Publish\nvar _ = publication.ManifestFiles\n\nconst table = \"x_publication\"\n",
			// A subpackage of the manifest package is another package:
			// it neither calls the dormant package nor uses this one.
			"internal/manifestv2/sub/planted.go": "package sub\n\nimport (\n\t\"" + publicationImport + "\"\n\t\"" + manifestV2Import + "\"\n)\n\n" +
				"var _ = publication.Publish\nvar _ = manifestv2.Verify\n",
			// This package may spell its tables, but not call the
			// dormant package, and its subpackages are held to both
			// rules.
			"internal/publication/planted.go":          "package publication\n\nimport _ \"" + manifestV2Import + "\"\n\nconst table = \"y_publication\"\n",
			"internal/publication/sub/planted.go":      "package sub\n\nimport \"" + manifestV2Import + "\"\n\nvar _ = manifestv2.Verify\n",
			"internal/publication/sub/planted_test.go": "package sub\n\nimport \"" + manifestV2Import + "\"\n\nvar _ = manifestv2.Verify\n",
			".hidden/planted.go":                       "package hidden\n\nimport \"" + manifestV2Import + "\"\n",
		} {
			file := filepath.Join(dir, filepath.FromSlash(rel))
			require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o755))
			require.NoError(t, os.WriteFile(file, []byte(src), 0o600))
		}
		files, err := productionFiles(dir)
		require.NoError(t, err)
		require.Len(t, files, 8, "the walk scans every production file but tests and hidden directories")
		violations, err := publicationViolations(dir, files)
		require.NoError(t, err)
		require.Equal(t, []string{
			"cmd/tools/init_db_collation.go dot-imports " + publicationImport,
			"cmd/tools/init_db_publication.go uses publication.Publish",
			"internal/cdc/planted.go imports " + publicationImport,
			`internal/cdc/planted.go spells a publication table name: "entity_main_publication"`,
			"internal/federated/planted.go imports " + manifestV2Import,
			`internal/manifestv2/planted.go spells a publication table name: "x_publication"`,
			"internal/manifestv2/sub/planted.go imports " + manifestV2Import,
			"internal/manifestv2/sub/planted.go imports " + publicationImport,
			"internal/manifestv2/sub/planted.go uses publication.Publish",
			"internal/publication/planted.go imports " + manifestV2Import,
			"internal/publication/sub/planted.go imports " + manifestV2Import,
		}, violations)
	})

	t.Run("the scanner names the file it cannot parse", func(t *testing.T) {
		dir := t.TempDir()
		broken := filepath.Join(dir, "broken.go")
		require.NoError(t, os.WriteFile(broken, []byte("package"), 0o600))
		_, err := publicationViolations(dir, []string{broken})
		require.ErrorContains(t, err, "parse "+broken)
	})
}

// productionFiles answers the non-test Go files under root, skipping hidden
// and build directories. It skips no package: what each rule exempts is
// fileViolations' to decide.
func productionFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %s: %w", p, err)
		}
		if d.IsDir() {
			if p != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "build") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list the production files under %s: %w", root, err)
	}
	return files, nil
}

// publicationViolations answers, sorted, each violation of the dormancy rule
// and the publication rule (TestOnlyInitDBDDLReachesThePublicationTables)
// among files.
func publicationViolations(root string, files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for imports of the publication package: %w", name, err)
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return nil, fmt.Errorf("name %s relative to the module root %s: %w", name, root, err)
		}
		rel = filepath.ToSlash(rel)
		violations = append(violations, fileViolations(rel, file)...)
	}
	sort.Strings(violations)
	return violations, nil
}

// fileViolations applies both rules to one file. pkg, the file's directory,
// is the package the exemptions name: a subpackage is never exempt.
func fileViolations(rel string, file *ast.File) []string {
	pkg := path.Dir(rel)
	violations := dormancyViolations(rel, pkg, file)
	if pkg == publicationDir {
		return violations // this package may spell and use itself
	}
	return append(violations, publicationRuleViolations(rel, pkg == manifestV2Dir, file)...)
}

// dormancyViolations refuses an import of internal/manifestv2 from any file
// outside that package.
func dormancyViolations(rel, pkg string, file *ast.File) []string {
	if pkg == manifestV2Dir {
		return nil
	}
	var violations []string
	for _, imp := range file.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p == manifestV2Import {
			violations = append(violations, rel+" imports "+manifestV2Import)
		}
	}
	return violations
}

// publicationRuleViolations refuses an import of this package outside
// ddlConsumers and the manifest package (inV2), a dot import of it, a use of
// an identifier outside ddlIdentifiers, and a string literal that spells a
// publication table's suffix.
func publicationRuleViolations(rel string, inV2 bool, file *ast.File) []string {
	var violations []string
	name := ""
	for _, imp := range file.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p != publicationImport {
			continue
		}
		name = "publication"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name == "." {
			violations = append(violations, rel+" dot-imports "+publicationImport)
		}
		if !ddlConsumers[rel] && !inV2 {
			violations = append(violations, rel+" imports "+publicationImport)
		}
	}
	if inV2 {
		name = "" // the manifest package may use every identifier
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
