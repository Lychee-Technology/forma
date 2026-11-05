// Package dsqllint is the static Aurora DSQL compatibility gate (#740) that
// `make lint-dsql` runs. It collects the corpus of typed SQL records the
// repository's TestDSQLCorpus tests export, lints them with the pinned
// dsql-lint, and compares the diagnostics with the known findings by exact
// count. docs/dsql-lint.md says what it covers and what it does not.
package dsqllint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// Repository paths the gate reads, relative to the root.
const (
	KnownPath     = "internal/dsqllint/known_findings.txt"
	FloorPath     = "internal/dsqllint/corpus_floor.txt"
	RegressionDir = "internal/dsqllint/testdata/regression"
)

// Config is one gate run.
type Config struct {
	// Root is the repository root.
	Root string
	// Tool is a verified dsql-lint ToolVersion executable (EnsureTool).
	Tool string
	// CorpusDir, when set, receives the records and the SQL linted from them
	// and is kept; it must not exist yet. Unset, a temporary directory is used.
	CorpusDir string
	Out       io.Writer
}

// ErrGateFailed is returned when the gate ran and found problems; the details
// are already written to Config.Out.
var ErrGateFailed = errors.New("DSQL lint gate failed")

// Run is the gate: prove the tool still catches the regression fixtures, export
// the corpus, check its records, lint them, and compare the findings with the
// known ones.
func Run(ctx context.Context, cfg Config) error {
	known, floors, err := loadInputs(cfg.Root)
	if err != nil {
		return fmt.Errorf("load the gate's inputs: %w", err)
	}
	if problems := selfTest(ctx, cfg); len(problems) > 0 {
		return report(cfg.Out, "regression fixture", problems)
	}
	dir, cleanup, err := corpusDir(cfg.CorpusDir)
	if err != nil {
		return fmt.Errorf("prepare the corpus directory: %w", err)
	}
	defer cleanup()

	units, err := collectCorpus(ctx, cfg, dir)
	if err != nil {
		return fmt.Errorf("collect the corpus: %w", err)
	}
	problems, err := checkCorpus(ctx, floors, units)
	if err != nil {
		return fmt.Errorf("check the corpus: %w", err)
	}
	if len(problems) > 0 {
		return report(cfg.Out, "corpus", problems)
	}

	sqlDir := filepath.Join(dir, "sql")
	if err := os.Mkdir(sqlDir, 0o755); err != nil {
		return fmt.Errorf("create the corpus SQL directory: %w", err)
	}
	for _, u := range units {
		if err := u.writeSQL(sqlDir); err != nil {
			return fmt.Errorf("prepare the lint: %w", err)
		}
	}
	findings, err := lintUnits(ctx, cfg.Tool, units)
	if err != nil {
		return fmt.Errorf("lint the units: %w", err)
	}
	v := Judge(findings, known)
	printSummary(cfg.Out, summarize(units), v, known)
	if !v.Passed() {
		return ErrGateFailed
	}
	return nil
}

func loadInputs(root string) ([]Known, []Floor, error) {
	k, err := os.Open(filepath.Join(root, KnownPath))
	if err != nil {
		return nil, nil, fmt.Errorf("open known findings: %w", err)
	}
	defer func() { _ = k.Close() }()
	known, err := ParseKnown(k)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", KnownPath, err)
	}
	f, err := os.Open(filepath.Join(root, FloorPath))
	if err != nil {
		return nil, nil, fmt.Errorf("open corpus floor: %w", err)
	}
	defer func() { _ = f.Close() }()
	floors, err := ParseFloor(f)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", FloorPath, err)
	}
	return known, floors, nil
}

func corpusDir(keep string) (string, func(), error) {
	if keep == "" {
		dir, err := os.MkdirTemp("", "forma-dsql-corpus-")
		if err != nil {
			return "", nil, fmt.Errorf("create corpus directory: %w", err)
		}
		return dir, func() { _ = os.RemoveAll(dir) }, nil
	}
	if _, err := os.Stat(keep); err == nil {
		return "", nil, fmt.Errorf("corpus directory %s already exists; pick a new one so no stale unit is linted", keep)
	}
	if err := os.MkdirAll(keep, 0o755); err != nil {
		return "", nil, fmt.Errorf("create corpus directory %s: %w", keep, err)
	}
	abs, err := filepath.Abs(keep)
	if err != nil {
		return "", nil, fmt.Errorf("resolve corpus directory %s: %w", keep, err)
	}
	return abs, func() {}, nil
}

// collectCorpus exports the corpus twice and loads the first export. A corpus
// that differs between the exports fails: the gate's result must depend on the
// code alone, and a statement rendered from a map's iteration order or the
// clock would make known findings flaky.
func collectCorpus(ctx context.Context, cfg Config, dir string) ([]*unit, error) {
	pkgs, err := corpusPackages(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("list the corpus packages: %w", err)
	}
	if len(pkgs) == 0 {
		return nil, errors.New("no package has a TestDSQLCorpus test: refusing to lint nothing")
	}
	records := filepath.Join(dir, "records")
	again := filepath.Join(dir, "records-again")
	for _, d := range []string{records, again} {
		if err := os.Mkdir(d, 0o755); err != nil {
			return nil, fmt.Errorf("create corpus export directory: %w", err)
		}
		if err := exportCorpus(ctx, cfg, d, pkgs); err != nil {
			return nil, fmt.Errorf("export to %s: %w", filepath.Base(d), err)
		}
	}
	if err := compareExports(records, again); err != nil {
		return nil, fmt.Errorf("compare the exports: %w", err)
	}
	if err := os.RemoveAll(again); err != nil {
		return nil, fmt.Errorf("remove the second corpus export: %w", err)
	}
	return loadUnits(records)
}

// corpusTest finds a TestDSQLCorpus test function in a Go test file.
var corpusTest = regexp.MustCompile(`(?m)^func TestDSQLCorpus\w*\(`)

// corpusPackages answers the packages under root with a TestDSQLCorpus test,
// so a corpus test in a new package is exported without anyone having to list
// it. It looks for tests, not SQL. Nested modules are not part of this one.
func corpusPackages(root string) ([]string, error) {
	set := make(map[string]bool)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk %s: %w", path, err)
		}
		if d.IsDir() {
			if path != root && skipDir(path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		text, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		if corpusTest.Match(text) {
			rel, err := filepath.Rel(root, filepath.Dir(path))
			if err != nil {
				return fmt.Errorf("resolve %s: %w", path, err)
			}
			set["./"+filepath.ToSlash(rel)] = true
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("find the corpus packages: %w", err)
	}
	return sortedKeys(set), nil
}

func skipDir(path, name string) bool {
	if name == "testdata" || name == "build" || name == "node_modules" || strings.HasPrefix(name, ".") {
		return true
	}
	_, err := os.Stat(filepath.Join(path, "go.mod"))
	return err == nil
}

// exportCorpus runs every TestDSQLCorpus* test with CorpusDirEnv set. A failing
// test fails the gate.
func exportCorpus(ctx context.Context, cfg Config, dir string, pkgs []string) error {
	args := append([]string{"test", "-count=1", "-run", "^TestDSQLCorpus"}, pkgs...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = cfg.Root
	cmd.Env = append(os.Environ(), corpus.CorpusDirEnv+"="+dir)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		_, _ = cfg.Out.Write(out.Bytes())
		return fmt.Errorf("export the DSQL corpus (go %s): %w", strings.Join(args, " "), err)
	}
	return nil
}

// compareExports fails unless directories a and b hold the same files with the
// same bytes.
func compareExports(a, b string) error {
	first, err := readDirFiles(a)
	if err != nil {
		return fmt.Errorf("read the first export: %w", err)
	}
	second, err := readDirFiles(b)
	if err != nil {
		return fmt.Errorf("read the second export: %w", err)
	}
	names := make(map[string]bool, len(first)+len(second))
	for name := range first {
		names[name] = true
	}
	for name := range second {
		names[name] = true
	}
	var differ []string
	for name := range names {
		x, inFirst := first[name]
		y, inSecond := second[name]
		if !inFirst || !inSecond || !bytes.Equal(x, y) {
			differ = append(differ, name)
		}
	}
	if len(differ) > 0 {
		sort.Strings(differ)
		return fmt.Errorf("the corpus is not deterministic: two exports differ in %s", strings.Join(differ, ", "))
	}
	return nil
}

func readDirFiles(dir string) (map[string][]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list corpus export: %w", err)
	}
	files := make(map[string][]byte, len(entries))
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("read corpus export: %w", err)
		}
		files[e.Name()] = data
	}
	return files, nil
}

// checkCorpus is what the gate requires of the records before it lints them:
// every one holds a statement, and the corpus has lost nothing.
func checkCorpus(ctx context.Context, floors []Floor, units []*unit) ([]error, error) {
	checker, err := newEmptinessChecker(ctx)
	if err != nil {
		return nil, fmt.Errorf("start the empty-statement check: %w", err)
	}
	defer checker.close()
	problems, err := checkNotEmpty(ctx, checker, units)
	if err != nil {
		return nil, fmt.Errorf("run the empty-statement check: %w", err)
	}
	return append(problems, checkFloor(floors, units)...), nil
}
