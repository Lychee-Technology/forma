package dsqllint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// Repository paths the gate reads, relative to the root.
const (
	InventoryPath = "internal/dsqllint/inventory.txt"
	KnownPath     = "internal/dsqllint/known_findings.txt"
	RegressionDir = "internal/dsqllint/testdata/regression"
)

// Config is one gate run.
type Config struct {
	// Root is the repository root.
	Root string
	// Tool is a verified dsql-lint ToolVersion executable (EnsureTool).
	Tool string
	// CorpusDir, when set, receives the rendered corpus and is kept; it
	// must not exist yet. Unset, a temporary directory is used.
	CorpusDir string
	Out       io.Writer
}

// ErrGateFailed is returned when the gate ran and found problems; the
// details are already written to Config.Out.
var ErrGateFailed = errors.New("DSQL lint gate failed")

// Run is the gate: check the inventory, prove the tool still catches the
// regression fixtures, render the corpus, lint it, and compare the
// findings with the known ones.
func Run(ctx context.Context, cfg Config) error {
	inv, known, err := loadInputs(cfg.Root)
	if err != nil {
		return err
	}
	entries := inv.Entries
	if problems := checkAll(cfg.Root, entries, known); len(problems) > 0 {
		return report(cfg.Out, "inventory", problems)
	}
	if problems := selfTest(ctx, cfg); len(problems) > 0 {
		return report(cfg.Out, "regression fixture", problems)
	}

	dir, cleanup, err := corpusDir(cfg.CorpusDir)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := exportCorpus(ctx, cfg, dir, renderedPackages(entries)); err != nil {
		return err
	}
	targets, err := collectTargets(cfg.Root, dir, entries)
	if err != nil {
		return err
	}
	if problems := checkCorpusOrigins(entries, targets.origins); len(problems) > 0 {
		return report(cfg.Out, "corpus coverage", problems)
	}

	findings, err := lintTargets(ctx, cfg.Tool, targets)
	if err != nil {
		return err
	}
	v := Judge(findings, known)
	printSummary(cfg.Out, inv, targets, v, known)
	if len(v.Unexpected) > 0 || len(v.Stale) > 0 {
		return ErrGateFailed
	}
	return nil
}

func loadInputs(root string) (Inventory, []Known, error) {
	f, err := os.Open(filepath.Join(root, InventoryPath))
	if err != nil {
		return Inventory{}, nil, fmt.Errorf("open inventory: %w", err)
	}
	defer func() { _ = f.Close() }()
	inv, err := ParseInventory(f)
	if err != nil {
		return Inventory{}, nil, fmt.Errorf("%s: %w", InventoryPath, err)
	}
	k, err := os.Open(filepath.Join(root, KnownPath))
	if err != nil {
		return Inventory{}, nil, fmt.Errorf("open known findings: %w", err)
	}
	defer func() { _ = k.Close() }()
	known, err := ParseKnown(k)
	if err != nil {
		return Inventory{}, nil, fmt.Errorf("%s: %w", KnownPath, err)
	}
	return inv, known, nil
}

// checkAll is every check that needs neither the tool nor the corpus: the
// inventory against the code, and each known finding against the inventory.
func checkAll(root string, entries []Entry, known []Known) []error {
	snap, err := Scan(root)
	if err != nil {
		return []error{err}
	}
	problems := CheckInventory(entries, snap)
	return append(problems, CheckKnownOrigins(entries, known)...)
}

// CheckKnownOrigins refuses a known finding whose origin is not a rendered
// inventory location: it could never match, or would accept SQL the corpus
// must not hold.
func CheckKnownOrigins(entries []Entry, known []Known) []error {
	rendered := renderedSet(entries)
	var problems []error
	for _, k := range known {
		if !rendered[k.Origin] {
			problems = append(problems, fmt.Errorf("%s line %d: origin %s is not a rendered inventory location", KnownPath, k.Line, k.Origin))
		}
	}
	return problems
}

func renderedSet(entries []Entry) map[string]bool {
	set := make(map[string]bool)
	for _, e := range entries {
		if e.Class == ClassRendered {
			set[e.Location] = true
		}
	}
	return set
}

// checkCorpusOrigins holds the corpus and the inventory to each other: every
// origin the corpus names is a rendered location (so PostgreSQL-only or
// DuckDB SQL never enters the DSQL gate), and every rendered location was
// rendered (so a skipped or deleted export cannot shrink the gate quietly).
func checkCorpusOrigins(entries []Entry, origins map[string]bool) []error {
	classOf := make(map[string]Class, len(entries))
	for _, e := range entries {
		classOf[e.Location] = e.Class
	}
	var problems []error
	for _, origin := range sortedKeys(origins) {
		switch class, ok := classOf[origin]; {
		case !ok:
			problems = append(problems, fmt.Errorf("corpus origin %s is not in the inventory", origin))
		case class != ClassRendered:
			problems = append(problems, fmt.Errorf("corpus origin %s is classified %s, so its SQL must not be in the DSQL corpus", origin, class))
		}
	}
	for _, e := range entries {
		if e.Class == ClassRendered && !origins[e.Location] {
			problems = append(problems, fmt.Errorf("rendered location %s (inventory line %d) is in no corpus unit: its TestDSQLCorpus export is missing, skipped or mislabelled", e.Location, e.Line))
		}
	}
	return problems
}

// renderedPackages answers the packages whose TestDSQLCorpus may render a
// rendered Go location.
func renderedPackages(entries []Entry) []string {
	set := make(map[string]bool)
	for _, e := range entries {
		if e.Class == ClassRendered && strings.HasSuffix(e.File(), ".go") {
			set["./"+filepath.ToSlash(filepath.Dir(e.File()))] = true
		}
	}
	return sortedKeys(set)
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

// exportCorpus runs every TestDSQLCorpus* test with CorpusDirEnv set. A failing
// test fails the gate.
func exportCorpus(ctx context.Context, cfg Config, dir string, pkgs []string) error {
	if len(pkgs) == 0 {
		return fmt.Errorf("the inventory has no rendered Go location: nothing to export")
	}
	args := append([]string{"test", "-count=1", "-run", "^TestDSQLCorpus"}, pkgs...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = cfg.Root
	cmd.Env = append(os.Environ(), corpus.CorpusDirEnv+"="+dir)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		_, _ = cfg.Out.Write(out.Bytes())
		return fmt.Errorf("render the DSQL corpus (go %s): %w", strings.Join(args, " "), err)
	}
	return nil
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func report(out io.Writer, what string, problems []error) error {
	fmt.Fprintf(out, "DSQL lint gate: %d %s problem(s):\n", len(problems), what)
	for _, p := range problems {
		fmt.Fprintf(out, "  - %v\n", p)
	}
	return ErrGateFailed
}
