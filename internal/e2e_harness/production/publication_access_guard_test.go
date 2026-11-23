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

// publicationImports are the Protocol V2 packages that hold a schema's
// published inventory: its record and its immutable manifests.
var publicationImports = map[string]bool{
	"github.com/lychee-technology/forma/internal/manifestv2":  true,
	"github.com/lychee-technology/forma/internal/publication": true,
}

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

// publicationHelperTests are the test files that may import the Protocol V2
// packages: the fault helpers that publish a test-made generation
// (publication_faults_test.go), the record initializer provisioning installs
// (publication_records_test.go), and the unit tests of the harness's own
// inventory reader and fault-injecting client, which drive them with fake
// stores (manifest_load_gate_test.go, faults_test.go).
var publicationHelperTests = map[string]bool{
	"faults_test.go":              true,
	"manifest_load_gate_test.go":  true,
	"publication_faults_test.go":  true,
	"publication_records_test.go": true,
	"reconcile_test.go":           true,
}

// TestE2ETestsReachPublicationOnlyThroughHelpers keeps the production e2e
// tests on the tools' own Protocol V2 publication (#901, #896):
//
//   - No file clears change_log, in a string literal or in SQL that takes
//     Tables.ChangeLog as the target of a delete or truncate. V2 never clears
//     it: cdc-flush and the cdc-init handoff take rows out of the dirty set
//     by marking their entries, and handoff.go's helpers only check that
//     they did.
//   - No test file but publicationHelperTests imports internal/manifestv2 or
//     internal/publication. E2E scenarios observe the published inventory
//     through manifest_access.go and publication_state.go, and stage a fault
//     in it only through publication_faults_test.go.
//
// An exemption that no longer imports either package fails too, so the list
// cannot outlive its reason. The scan parses every Go file of the package,
// whatever its build tags. It is syntactic: a statement assembled at run time
// from anything but a literal and Tables.ChangeLog is not caught.
func TestE2ETestsReachPublicationOnlyThroughHelpers(t *testing.T) {
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
	pubImport := "github.com/lychee-technology/forma/internal/publication"
	v2Import := "github.com/lychee-technology/forma/internal/manifestv2"
	planted := map[string]string{
		"handoff.go":        pkg + "const q = \"" + stmt + "\"\n\nvar _ = \"DELETE FROM \" + e.Tables.ChangeLog\n",
		"cdc.go":            pkg + "const q = `" + stmt + " WHERE flushed_at <> 0`\n",
		"lower_e2e_test.go": "//go:build e2e\n\n" + pkg + "const q = \"delete\\n\\tfrom " + table + "\"\n",
		"trunc_e2e_test.go": pkg + "const q = `" + trunc + "`\n",
		"qualified_test.go": pkg + "const q = `" + qualified + "`\n\nconst r = `" + quoted + "`\n",
		"concat_e2e_test.go": pkg + "func f() {\n\tenv.ExecSQL(ctx, \"DELETE FROM \"+env.Tables.ChangeLog+\" WHERE schema_id = $1\")\n" +
			"\t_ = \"SELECT COUNT(*) FROM \" + env.Tables.ChangeLog\n\t_ = \"DELETE FROM \" + env.Tables.EntityMain\n}\n",
		"format_e2e_test.go": pkg + "var q = fmt.Sprintf(\"SELECT %d, 100%%; DELETE FROM %s WHERE schema_id = %d\", " +
			"1, sqlutil.SanitizeIdentifier(env.Tables.ChangeLog), 2)\n\n" +
			"var r = fmt.Sprintf(\"DELETE FROM %s WHERE schema_id = %d\", env.Tables.EntityMain, env.Tables.ChangeLog)\n",
		"record_e2e_test.go":          pkg + "import p \"" + pubImport + "\"\n\nvar _ = p.Publish\n",
		"generation_e2e_test.go":      pkg + "import (\n\tm \"" + v2Import + "\"\n\tp \"" + pubImport + "\"\n)\n\nvar _, _ = m.Fetch, p.Read\n",
		"publication_faults_test.go":  pkg + "import p \"" + pubImport + "\"\n\nvar _ = p.Publish\n",
		"faults_test.go":              pkg + "import _ \"" + v2Import + "\"\n",
		"published_e2e_test.go":       pkg + "func f() { for _, e := range m.Files { _ = e.Path }; x := m.Files; _ = x }\n",
		"sibling_e2e_test.go":         pkg + "const q = \"" + stmt + "_archive\"\n\nconst r = \"DELETE FROM public." + table + "_archive\"\n",
		"manifest_load_gate_test.go":  pkg + "const q = \"SELECT 1\"\n",
		"publication_records_test.go": pkg + "import _ \"" + pubImport + "\"\n",
	}
	want := []string{
		`cdc.go clears change_log: "` + stmt + ` WHERE flushed_at <> 0"`,
		`concat_e2e_test.go clears change_log: "DELETE FROM " + Tables.ChangeLog`,
		`format_e2e_test.go clears change_log: "SELECT %d, 100%%; DELETE FROM %s WHERE schema_id = %d" with Tables.ChangeLog`,
		"generation_e2e_test.go imports " + v2Import,
		"generation_e2e_test.go imports " + pubImport,
		`handoff.go clears change_log: "DELETE FROM " + Tables.ChangeLog`,
		`handoff.go clears change_log: "` + stmt + `"`,
		`lower_e2e_test.go clears change_log: "delete\n\tfrom ` + table + `"`,
		"manifest_load_gate_test.go is exempt from the publication import rule but imports neither package",
		`qualified_test.go clears change_log: "` + qualified + `"`,
		`qualified_test.go clears change_log: "TRUNCATE TABLE \"public\".\"` + table + `\""`,
		"record_e2e_test.go imports " + pubImport,
		`trunc_e2e_test.go clears change_log: "` + trunc + `"`,
	}
	return planted, want
}

// standInViolations answers, sorted, each violation of the rules of
// TestE2ETestsReachPublicationOnlyThroughHelpers among files, which are
// named by their base name.
func standInViolations(files []string) ([]string, error) {
	var violations []string
	fset := token.NewFileSet()
	for _, name := range files {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s for publication stand-ins: %w", name, err)
		}
		base := filepath.Base(name)
		violations = append(violations, importViolations(base, file)...)
		violations = append(violations, changeLogClearViolations(base, file)...)
	}
	sort.Strings(violations)
	return violations, nil
}

// importViolations applies the import rules to one test file.
func importViolations(base string, file *ast.File) []string {
	if !strings.HasSuffix(base, "_test.go") {
		return nil
	}
	var violations []string
	publicationImported := false
	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		switch {
		case err != nil:
		case publicationImports[p]:
			publicationImported = true
			if !publicationHelperTests[base] {
				violations = append(violations, base+" imports "+p)
			}
		}
	}
	if !publicationImported && publicationHelperTests[base] {
		violations = append(violations, base+" is exempt from the publication import rule but imports neither package")
	}
	return violations
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
