package production

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const manifestImport = "github.com/lychee-technology/forma/internal/manifest"

// clearVerb opens a statement that deletes or truncates a table's rows.
const clearVerb = `\b(?:delete\s+from|truncate(?:\s+table)?)\s+(?:only\s+)?`

var (
	// changeLogClear matches a statement that deletes or truncates change_log
	// entries, in any case, across line breaks, and with the table quoted or
	// qualified by its schema ("public".change_log).
	changeLogClear = regexp.MustCompile(`(?is)` + clearVerb + `(?:"?[a-z_][a-z0-9_]*"?\s*\.\s*)?"?change_log\b`)
	// clearTargetNext matches a fragment that ends where the statement it
	// opens names its target: "DELETE FROM " + Tables.ChangeLog.
	clearTargetNext = regexp.MustCompile(`(?is)` + clearVerb + `$`)
	// clearTargetVerb matches a format that takes the target of a delete or
	// truncate as an operand: Sprintf("DELETE FROM %s", Tables.ChangeLog).
	clearTargetVerb = regexp.MustCompile(`(?is)` + clearVerb + `%`)
	// formatVerb matches one operand-consuming verb of a format string, or an
	// escaped percent sign that consumes none.
	formatVerb = regexp.MustCompile(`%%|%[-+# 0-9.]*[a-zA-Z]`)
)

// changeLogHelpers is the one file of the package that may clear change_log:
// the helpers that name why a test does it (onboarded or flushed).
var changeLogHelpers = map[string]bool{"handoff.go": true}

// manifestWrapperTests are the unit tests of the harness's own manifest
// wrappers, which run without the e2e tag and drive them with fake stores:
// that loadManifestsFrom and loadKeptDeltaEntries only treat
// manifest.ErrObjectNotFound as absent, and that an injected S3 fault never
// classifies as manifest.IsNotFound (#464). They test the wrappers, so they
// name the package the wrappers wrap.
var manifestWrapperTests = map[string]bool{
	"faults_test.go":             true,
	"manifest_load_gate_test.go": true,
}

// manifestDocumentHelpers are the harness functions that reach the V1
// manifest document itself (its ETag, its store, its path) rather than the
// published files. Only the harness sources call them: a test compares
// publications through publication_state.go and stages a fault through
// publication_faults.go.
var manifestDocumentHelpers = map[string]bool{
	"loadManifestWithETag": true,
	"saveManifestWithETag": true,
	"replaceManifestTier":  true,
	"manifestAccess":       true,
	"manifestResolver":     true,
}

// manifestDocumentFields are the fields of the V1 manifest that describe the
// document rather than the files it publishes.
var manifestDocumentFields = map[string]bool{"Version": true, "UpdatedAtMs": true}

// TestE2ETestsReachV1StandInsOnlyThroughHelpers keeps the production e2e
// tests off the Protocol V1 stand-ins #896 replaces (#901):
//
//   - No file but changeLogHelpers clears change_log, in a string literal or
//     in SQL that takes Tables.ChangeLog as the target of a delete or
//     truncate. V2 never clears it, and gives each intent the helpers name a
//     different replacement.
//   - No test file but manifestWrapperTests imports internal/manifest. E2E
//     scenarios read the published inventory through manifest_access.go and
//     the other harness sources, whose bodies #896 rewrites.
//   - No test file but manifestWrapperTests reaches the V1 manifest document:
//     it calls none of manifestDocumentHelpers, selects none of
//     manifestDocumentFields and assigns no Files. V2 publishes immutable
//     generations behind a publication record, so a test that edited or
//     versioned the document could not move with the helper bodies.
//
// An exemption that no longer imports the package fails too, so the list
// cannot outlive its reason. The scan parses every Go file of the package,
// whatever its build tags. It is syntactic: a statement assembled at run time
// from anything but a literal and Tables.ChangeLog is not caught, and
// another type's Version field reads as the manifest's.
func TestE2ETestsReachV1StandInsOnlyThroughHelpers(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.Greater(t, len(files), 150, "the package's Go files, e2e-tagged tests included")
	violations, err := standInViolations(files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches planted stand-ins and a stale exemption", func(t *testing.T) {
		planted, want := plantedStandIns()
		dir := t.TempDir()
		var names []string
		for name, src := range planted {
			file := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(file, []byte(src), 0o600))
			names = append(names, file)
		}
		violations, err := standInViolations(names)
		require.NoError(t, err)
		require.Equal(t, want, violations)
	})

	t.Run("the scanner names the file it cannot parse", func(t *testing.T) {
		broken := filepath.Join(t.TempDir(), "broken_test.go")
		require.NoError(t, os.WriteFile(broken, []byte("package"), 0o600))
		_, err := standInViolations([]string{broken})
		require.ErrorContains(t, err, "parse "+broken)
	})
}

// plantedStandIns answers planted files, by base name, and the sorted
// violations standInViolations must answer for them. The statements are split
// across literals so that this file does not plant a violation of its own.
func plantedStandIns() (map[string]string, []string) {
	table := "change_log"
	stmt := "DELETE FROM " + table
	trunc := "TRUNCATE TABLE " + table
	qualified := "DELETE FROM public." + table + " WHERE schema_id = $1"
	quoted := `TRUNCATE TABLE "public"."` + table + `"`
	pkg := "package p\n\n"
	planted := map[string]string{
		"handoff.go":        pkg + "const q = \"" + stmt + "\"\n\nvar _ = \"DELETE FROM \" + e.Tables.ChangeLog\n",
		"cdc.go":            pkg + "import _ \"" + manifestImport + "\"\n\nconst q = `" + stmt + " WHERE flushed_at <> 0`\n",
		"lower_e2e_test.go": "//go:build e2e\n\n" + pkg + "const q = \"delete\\n\\tfrom " + table + "\"\n",
		"trunc_e2e_test.go": pkg + "const q = `" + trunc + "`\n",
		"qualified_test.go": pkg + "const q = `" + qualified + "`\n\nconst r = `" + quoted + "`\n",
		"concat_e2e_test.go": pkg + "func f() {\n\tenv.ExecSQL(ctx, \"DELETE FROM \"+env.Tables.ChangeLog+\" WHERE schema_id = $1\")\n" +
			"\t_ = \"SELECT COUNT(*) FROM \" + env.Tables.ChangeLog\n\t_ = \"DELETE FROM \" + env.Tables.EntityMain\n}\n",
		"format_e2e_test.go": pkg + "var q = fmt.Sprintf(\"SELECT %d, 100%%; DELETE FROM %s WHERE schema_id = %d\", " +
			"1, sqlutil.SanitizeIdentifier(env.Tables.ChangeLog), 2)\n\n" +
			"var r = fmt.Sprintf(\"DELETE FROM %s WHERE schema_id = %d\", env.Tables.EntityMain, env.Tables.ChangeLog)\n",
		"import_e2e_test.go": pkg + "import m \"" + manifestImport + "\"\n\nvar _ = m.FilterByTier\n",
		"faults_test.go":     pkg + "import _ \"" + manifestImport + "\"\n\nvar _ = m.Version\n",
		"document_e2e_test.go": pkg + "func f() {\n\tm, etag := loadManifestWithETag(t, ctx, env, s)\n\tm.Files = nil\n" +
			"\tm.Files[0].Checksum = \"\"\n\t_ = m.Version\n\tsaveManifestWithETag(t, ctx, env, s, m, etag)\n}\n",
		// A harness source may reach the document; a test may read files.
		"publication_faults.go":      pkg + "func f() { m, _ := loadManifestWithETag(); m.Files = nil; _ = m.Version }\n",
		"published_e2e_test.go":      pkg + "func f() { for _, e := range m.Files { _ = e.Path }; x := m.Files; _ = x }\n",
		"sibling_e2e_test.go":        pkg + "const q = \"" + stmt + "_archive\"\n\nconst r = \"DELETE FROM public." + table + "_archive\"\n",
		"manifest_load_gate_test.go": pkg + "const q = \"SELECT 1\"\n",
	}
	want := []string{
		`cdc.go clears change_log: "` + stmt + ` WHERE flushed_at <> 0"`,
		`concat_e2e_test.go clears change_log: "DELETE FROM " + Tables.ChangeLog`,
		"document_e2e_test.go assigns the V1 manifest's Files",
		"document_e2e_test.go assigns the V1 manifest's Files",
		"document_e2e_test.go reaches the V1 manifest document through loadManifestWithETag",
		"document_e2e_test.go reaches the V1 manifest document through saveManifestWithETag",
		"document_e2e_test.go reads the V1 manifest's Version",
		`format_e2e_test.go clears change_log: "SELECT %d, 100%%; DELETE FROM %s WHERE schema_id = %d" with Tables.ChangeLog`,
		"import_e2e_test.go imports " + manifestImport,
		`lower_e2e_test.go clears change_log: "delete\n\tfrom ` + table + `"`,
		"manifest_load_gate_test.go is exempt from the manifest import rule but no longer imports " + manifestImport,
		`qualified_test.go clears change_log: "` + qualified + `"`,
		`qualified_test.go clears change_log: "TRUNCATE TABLE \"public\".\"` + table + `\""`,
		`trunc_e2e_test.go clears change_log: "` + trunc + `"`,
	}
	return planted, want
}

// standInViolations answers, sorted, each violation of the rules of
// TestE2ETestsReachV1StandInsOnlyThroughHelpers among files, which are named
// by their base name.
func standInViolations(files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for V1 stand-ins: %w", name, err)
		}
		base := filepath.Base(name)
		violations = append(violations, manifestImportViolations(base, file)...)
		if strings.HasSuffix(base, "_test.go") && !manifestWrapperTests[base] {
			violations = append(violations, manifestDocumentViolations(base, file)...)
		}
		if !changeLogHelpers[base] {
			violations = append(violations, changeLogClearViolations(base, file)...)
		}
	}
	sort.Strings(violations)
	return violations, nil
}

// manifestImportViolations applies the import rule to one file.
func manifestImportViolations(base string, file *ast.File) []string {
	if !strings.HasSuffix(base, "_test.go") {
		return nil
	}
	imports := false
	for _, spec := range file.Imports {
		if p, err := strconv.Unquote(spec.Path.Value); err == nil && p == manifestImport {
			imports = true
		}
	}
	switch {
	case imports && !manifestWrapperTests[base]:
		return []string{base + " imports " + manifestImport}
	case !imports && manifestWrapperTests[base]:
		return []string{base + " is exempt from the manifest import rule but no longer imports " + manifestImport}
	}
	return nil
}

// manifestDocumentViolations applies the manifest document rule to one test
// file.
func manifestDocumentViolations(base string, file *ast.File) []string {
	var violations []string
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Ident:
			if manifestDocumentHelpers[n.Name] {
				violations = append(violations, base+" reaches the V1 manifest document through "+n.Name)
			}
		case *ast.SelectorExpr:
			if manifestDocumentFields[n.Sel.Name] {
				violations = append(violations, base+" reads the V1 manifest's "+n.Sel.Name)
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if selectsFiles(lhs) {
					violations = append(violations, base+" assigns the V1 manifest's Files")
				}
			}
		}
		return true
	})
	return violations
}

// selectsFiles reports whether an assignment target writes a manifest's
// Files or an element of them: m.Files, m.Files[i] or m.Files[i].Checksum.
func selectsFiles(lhs ast.Expr) bool {
	for {
		switch e := lhs.(type) {
		case *ast.SelectorExpr:
			if e.Sel.Name == "Files" {
				return true
			}
			lhs = e.X
		case *ast.IndexExpr:
			lhs = e.X
		case *ast.ParenExpr:
			lhs = e.X
		default:
			return false
		}
	}
}

// changeLogClearViolations applies the change_log rule to one file: to every
// string literal, to each "+" chain whose literal fragment hands its target
// to Tables.ChangeLog, and to each Sprintf whose format does.
func changeLogClearViolations(base string, file *ast.File) []string {
	var violations []string
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.BasicLit:
			if value, ok := stringLit(n); ok && changeLogClear.MatchString(value) {
				violations = append(violations, fmt.Sprintf("%s clears change_log: %q", base, value))
			}
		case *ast.BinaryExpr:
			if value, ok := concatClearsChangeLog(n); ok {
				violations = append(violations, fmt.Sprintf("%s clears change_log: %q + Tables.ChangeLog", base, value))
			}
		case *ast.CallExpr:
			if format, ok := formatClearsChangeLog(n); ok {
				violations = append(violations, fmt.Sprintf("%s clears change_log: %q with Tables.ChangeLog", base, format))
			}
		}
		return true
	})
	return violations
}

// stringLit answers the value of a string literal.
func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	value, err := strconv.Unquote(lit.Value)
	return value, err == nil
}

// concatClearsChangeLog reports the literal fragment of a "+" expression that
// opens a delete or truncate whose target is the next operand, when that
// operand names Tables.ChangeLog. The chain a+b+c parses as (a+b)+c, so the
// expression's own right operand is the one that follows its left side's last
// fragment; each sub-chain is visited on its own.
func concatClearsChangeLog(e *ast.BinaryExpr) (string, bool) {
	if e.Op != token.ADD || !namesChangeLogTable(e.Y) {
		return "", false
	}
	left := e.X
	if chain, ok := left.(*ast.BinaryExpr); ok && chain.Op == token.ADD {
		left = chain.Y
	}
	value, ok := stringLit(left)
	if !ok || !clearTargetNext.MatchString(value) {
		return "", false
	}
	return value, true
}

// formatClearsChangeLog reports the format of a Sprintf call whose verb for
// the target of a delete or truncate takes Tables.ChangeLog as its operand.
func formatClearsChangeLog(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Sprintf" || len(call.Args) < 2 {
		return "", false
	}
	format, ok := stringLit(call.Args[0])
	if !ok {
		return "", false
	}
	for _, loc := range clearTargetVerb.FindAllStringIndex(format, -1) {
		operand := 0
		for _, verb := range formatVerb.FindAllStringIndex(format[:loc[1]-1], -1) {
			if format[verb[0]:verb[1]] != "%%" {
				operand++
			}
		}
		if 1+operand < len(call.Args) && namesChangeLogTable(call.Args[1+operand]) {
			return format, true
		}
	}
	return "", false
}

// namesChangeLogTable reports whether e mentions Tables.ChangeLog, directly
// or through a call such as sqlutil.SanitizeIdentifier(env.Tables.ChangeLog).
func namesChangeLogTable(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "ChangeLog" {
			return !found
		}
		switch x := sel.X.(type) {
		case *ast.SelectorExpr:
			found = found || x.Sel.Name == "Tables"
		case *ast.Ident:
			found = found || x.Name == "Tables"
		}
		return !found
	})
	return found
}
