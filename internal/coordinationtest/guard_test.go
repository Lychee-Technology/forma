package coordinationtest

import (
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Guards on the package itself: the adapter uses the public API only
// (#654 rule 1), every mapped LTFlow operation has an adapter method that
// cites it (rule 3), nothing sleeps or skips under -short (rules 7 and 8),
// and the suite stays where the CI Test job runs it (rule 8).

const modulePath = "github.com/lychee-technology/forma"

func TestAdapterImportsOnlyThePublicAPI(t *testing.T) {
	files := adapterFiles(t, ".")
	require.GreaterOrEqual(t, len(files), 8, "the adapter's store files")
	violations, err := importViolations(files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches a planted internal import", func(t *testing.T) {
		dir := t.TempDir()
		for _, f := range files {
			data, err := os.ReadFile(f)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.Base(f)), data, 0o600))
		}
		planted := "package coordinationtest\n\nimport _ \"" + modulePath + "/internal/model\"\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "planted.go"), []byte(planted), 0o600))
		violations, err := importViolations(adapterFiles(t, dir))
		require.NoError(t, err)
		require.Equal(t, []string{"planted.go imports " + modulePath + "/internal/model"}, violations)
	})
}

// adapterFiles answers the non-test Go files of dir.
func adapterFiles(t *testing.T, dir string) []string {
	t.Helper()
	all, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)
	var files []string
	for _, f := range all {
		if !strings.HasSuffix(f, "_test.go") {
			files = append(files, f)
		}
	}
	return files
}

// importViolations answers each import of files outside rule 1's allow-list:
// forma, forma/factory, github.com/google/uuid and the standard library.
func importViolations(files []string) ([]string, error) {
	fset := token.NewFileSet()
	var violations []string
	for _, f := range files {
		parsed, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			return nil, err
		}
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, err
			}
			if !allowedImport(path) {
				violations = append(violations, filepath.Base(f)+" imports "+path)
			}
		}
	}
	return violations, nil
}

func allowedImport(path string) bool {
	switch path {
	case modulePath, modulePath + "/factory", "github.com/google/uuid":
		return true
	}
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

func TestNoFixedSleepsOrShortGates(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.Greater(t, len(files), 20)
	violations, err := sleepsAndShortGates(files)
	require.NoError(t, err)
	require.Empty(t, violations)

	t.Run("the guard catches an aliased sleep, a short gate and a dot import", func(t *testing.T) {
		dir := t.TempDir()
		planted := map[string]string{
			"aliased.go": "package p\n\nimport clock \"time\"\n\nfunc f() { clock.Sleep(1) }\n",
			"short.go":   "package p\n\nimport \"testing\"\n\nfunc g() bool { return testing.Short() }\n",
			"dot.go":     "package p\n\nimport . \"time\"\n\nfunc h() { Sleep(1) }\n",
		}
		var paths []string
		for name, src := range planted {
			path := filepath.Join(dir, name)
			require.NoError(t, os.WriteFile(path, []byte(src), 0o600))
			paths = append(paths, path)
		}
		violations, err := sleepsAndShortGates(paths)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"aliased.go calls time.Sleep", "short.go calls testing.Short", "dot.go dot-imports time"}, violations)
	})
}

// sleepsAndShortGates answers each use of time.Sleep and testing.Short in
// files, under whatever name the file imports the package, and each dot
// import of either package, which would hide such a use.
func sleepsAndShortGates(files []string) ([]string, error) {
	banned := map[string]string{"time": "Sleep", "testing": "Short"}
	fset := token.NewFileSet()
	var violations []string
	for _, f := range files {
		parsed, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			return nil, err
		}
		names := map[string]string{} // local name -> package path
		for _, spec := range parsed.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return nil, err
			}
			if _, ok := banned[path]; !ok {
				continue
			}
			name := path
			if spec.Name != nil {
				name = spec.Name.Name
			}
			if name == "." {
				violations = append(violations, filepath.Base(f)+" dot-imports "+path)
			}
			names[name] = path
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && names[pkg.Name] != "" && banned[names[pkg.Name]] == sel.Sel.Name {
				violations = append(violations, filepath.Base(f)+" calls "+names[pkg.Name]+"."+sel.Sel.Name)
			}
			return true
		})
	}
	return violations, nil
}

func TestSuiteRunsInTheCITestJob(t *testing.T) {
	pkg := reflect.TypeOf(env{}).PkgPath()
	require.True(t, strings.HasPrefix(pkg, modulePath+"/internal/"), "%s is under internal/", pkg)
	require.NotContains(t, pkg+"/", "/internal/e2e_harness/",
		"the E2E jobs have no PostgreSQL service; the suite belongs to the Test job")
	ci, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	require.NoError(t, err)
	var raceRuns []string
	for _, line := range strings.Split(string(ci), "\n") {
		if strings.Contains(line, "go test") && strings.Contains(line, "-race") {
			raceRuns = append(raceRuns, line)
		}
	}
	require.Len(t, raceRuns, 1, "the Test job runs go test -race once")
	require.Contains(t, strings.Fields(raceRuns[0]), "./internal/...", "the Test job's go test -race covers ./internal/...")
	// Any of these could take the package back out of the run.
	for _, narrowing := range []string{"$(", "`", "go list", "grep", "|", "-skip", "-run", "-short"} {
		require.NotContains(t, raceRuns[0], narrowing, "the Test job's go test -race runs every package and test as listed")
	}
}

// mapping is #654's mapping table: each LTFlow operation as README.md's
// table names it, and the adapter methods that implement it, each with the
// LTFlow method its doc comment cites.
var mapping = []struct {
	operation string
	methods   map[string]string
}{
	{"Repository.InTx", map[string]string{"Store.InTx": "Repository.InTx"}},
	{"InstanceStore.Create", map[string]string{"instanceStore.Create": "InstanceStore.Create"}},
	{"InstanceStore.Update", map[string]string{"instanceStore.Update": "InstanceStore.Update"}},
	{"InstanceStore.FenceVersion", map[string]string{"instanceStore.FenceVersion": "InstanceStore.FenceVersion"}},
	{"InstanceStore.Get", map[string]string{"instanceStore.Get": "InstanceStore.Get", "instanceStore.ListByFilter": "InstanceStore.ListByFilter"}},
	{"TaskStore.Insert", map[string]string{"taskStore.Insert": "TaskStore.Insert", "taskStore.GetByTokenHash": "TaskStore.GetByTokenHash"}},
	{"TaskStore.UpdateStatus", map[string]string{"taskStore.UpdateStatus": "TaskStore.UpdateStatus"}},
	{"TaskStore.CancelPendingForInstanceVersion", map[string]string{"taskStore.CancelPendingForInstance": "TaskStore.CancelPendingForInstance"}},
	{"TaskStore.CompleteMatchingAndCancelPending", map[string]string{
		"taskStore.CompleteMatchingAndCancelPending": "TaskStore.CompleteMatchingAndCancelPending"}},
	{"IdempotencyStore.Upsert", map[string]string{"idempotencyStore.Upsert": "IdempotencyStore.Upsert"}},
	{"WorkflowLinkStore.Insert", map[string]string{"linkStore.Insert": "WorkflowLinkStore.Insert", "linkStore.UpdateStatus": "WorkflowLinkStore.UpdateStatus"}},
	{"OutboxStore.Insert", map[string]string{"outboxStore.Insert": "OutboxStore.Insert"}},
	{"OutboxStore.ClaimBatch", map[string]string{"outboxStore.ClaimBatch": "OutboxStore.ClaimBatch",
		"outboxStore.ClaimBatchByTopics": "OutboxTopicClaimer.ClaimBatchByTopics"}},
	{"OutboxStore.MarkPublished", map[string]string{"outboxStore.MarkPublished": "OutboxStore.MarkPublished",
		"outboxStore.MarkFailed": "OutboxStore.MarkFailed", "outboxStore.MarkDeadLetter": "OutboxStore.MarkDeadLetter"}},
	{"HistoryStore.Insert", map[string]string{"historyStore.Insert": "HistoryStore.Insert"}},
	{"HistoryStore.UpsertDeduped", map[string]string{"historyStore.UpsertDeduped": "HistoryStore.UpsertDeduped"}},
	{"DefinitionStore", map[string]string{"definitionStore.SaveDraft": "DefinitionStore.SaveDraft",
		"definitionStore.SaveActive": "DefinitionStore.SaveActive", "definitionStore.GetByVersion": "DefinitionStore.GetByVersion"}},
	{"RetentionStore.DeleteEligibleTasks", map[string]string{"taskStore.DeleteEligible": "RetentionStore.DeleteEligibleTasks",
		"idempotencyStore.DeleteEligible": "IdempotencyStore.DeleteEligible"}},
	{"RetentionStore.MarkHistoryArchived", map[string]string{"historyStore.MarkArchived": "RetentionStore.MarkHistoryArchived",
		"taskStore.MarkResolvedForInstanceEligible": "TaskStore.MarkResolvedForInstanceEligible"}},
}

func TestEveryMappedOperationHasACitingMethod(t *testing.T) {
	docs := methodDocs(t, adapterFiles(t, "."))
	readme, err := os.ReadFile("README.md")
	require.NoError(t, err)
	operations := make([]string, len(mapping))
	for i, row := range mapping {
		operations[i] = row.operation
	}
	require.Len(t, operations, 19, "#654's mapping table has 19 rows")
	require.ElementsMatch(t, operations, readmeOperations(string(readme)), "README.md's mapping table and the mapping checked here list the same operations")
	for _, row := range mapping {
		for method, cited := range row.methods {
			text, ok := docs[method]
			require.True(t, ok, "the adapter has %s for %s", method, row.operation)
			require.Contains(t, text, "LTFlow: "+cited, "%s cites the LTFlow method it mirrors", method)
		}
	}
}

// readmeOperations answers the LTFlow operation of each row of README.md's
// mapping table: the row's first backticked name, cut at any argument list.
func readmeOperations(readme string) []string {
	var operations []string
	for _, line := range strings.Split(readme, "\n") {
		rest, ok := strings.CutPrefix(line, "| `")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(rest, "`")
		name, _, _ = strings.Cut(name, "(")
		operations = append(operations, name)
	}
	return operations
}

// methodDocs answers each method's doc comment, keyed type.method, with its
// lines joined by spaces.
func methodDocs(t *testing.T, files []string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	parsed := make([]*ast.File, 0, len(files))
	for _, f := range files {
		file, err := parser.ParseFile(fset, f, nil, parser.ParseComments)
		require.NoError(t, err)
		parsed = append(parsed, file)
	}
	pkg, err := doc.NewFromFiles(fset, parsed, modulePath+"/internal/coordinationtest", doc.AllDecls)
	require.NoError(t, err)
	docs := map[string]string{}
	for _, typ := range pkg.Types {
		for _, m := range typ.Methods {
			docs[typ.Name+"."+m.Name] = strings.Join(strings.Fields(m.Doc), " ")
		}
	}
	return docs
}
