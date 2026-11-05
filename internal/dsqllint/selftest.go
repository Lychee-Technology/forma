package dsqllint

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// expectPrefix marks a rule a regression fixture must trigger.
const expectPrefix = "-- expect: "

// selfTest lints each regression fixture on its own and requires exactly the
// rules its expect comments name: an invalid fixture proves the pinned tool
// still rejects what it must, a valid one that it accepts what it must. A tool
// upgrade that changes either fails here first.
func selfTest(ctx context.Context, cfg Config) []error {
	paths, err := filepath.Glob(filepath.Join(cfg.Root, RegressionDir, "*.sql"))
	if err != nil || len(paths) == 0 {
		return []error{fmt.Errorf("no regression fixtures in %s (%v)", RegressionDir, err)}
	}
	sort.Strings(paths)
	var problems []error
	var sawInvalid, sawValid bool
	for _, p := range paths {
		want, err := expectedRules(p)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		sawInvalid = sawInvalid || len(want) > 0
		sawValid = sawValid || len(want) == 0
		byFile, err := runLint(ctx, cfg.Tool, []string{p})
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", filepath.Base(p), err))
			continue
		}
		got := map[string]bool{}
		for _, d := range byFile[p] {
			got[d.Rule] = true
		}
		if g, w := strings.Join(sortedKeys(got), ","), strings.Join(sortedKeys(want), ","); g != w {
			problems = append(problems, fmt.Errorf("%s: dsql-lint reported rules [%s], the fixture expects [%s]", filepath.Base(p), g, w))
		}
	}
	if !sawInvalid || !sawValid {
		problems = append(problems, fmt.Errorf("%s needs at least one fixture with expect comments and one without", RegressionDir))
	}
	return problems
}

func expectedRules(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open regression fixture: %w", err)
	}
	defer func() { _ = f.Close() }()
	want := map[string]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if rule, ok := strings.CutPrefix(strings.TrimSpace(scanner.Text()), expectPrefix); ok {
			want[strings.TrimSpace(rule)] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return want, nil
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
