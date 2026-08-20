package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestBootstrapLambda_CanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := bootstrapLambda(ctx, zap.NewNop().Sugar())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

// knownManagerBuilders are the cmd/ directories that build an EntityManager
// today. The walk below discovers directories rather than reading this list, so
// this is a floor and never a ceiling: it exists only so a walk that finds
// nothing — wrong root, a scan that matches no files — fails loudly instead of
// passing vacuously.
//
// Because it is indexed by directory name, renaming an entry point turns this
// red until the name here is updated. That is deliberate: a rename is exactly
// when the walk silently reading the wrong tree is most likely.
var knownManagerBuilders = []string{"lambda", "server", "sample"}

// TestEntityConfigFromEnvWiredInEveryWriteEntryPoint guards that every entry point
// building an EntityManager overlays the entity config from the environment, so
// the #314 staged rollout (VALIDATE_UPDATES_STRICT) is available on all
// deployment targets rather than only the ones someone remembered to wire.
//
// For each child directory of cmd/ it reads every non-test Go file in that
// child's whole subtree and matches the "factory.NewEntityManager" prefix, so an
// entry point added later is read without anyone updating this test, and the
// WithConfig / WithConfigContext wrappers — and any future one — all count.
// Before #321 the directories were a hardcoded pair and the match was the exact
// WithConfigContext spelling, so the regression this comment names could not fail
// it: a scratch cmd/apiworker went unread, and the already existing cmd/sample
// used the unmatched WithConfig wrapper.
//
// The unit of judgement is that whole subtree, not a single file. One cmd/<name>
// tree is one binary, so an entry point that builds its manager outside main.go,
// or applies the overlay from a sibling file or a helper subpackage, is wired and
// must pass. The cost is the converse: a tree where the overlay call sits in a
// file that is not on the path to the manager also passes. Attributing the
// overlay to the file that builds the manager would trade that for false
// failures on the ordinary split-across-files wiring, which is the more common
// shape — and a substring scan cannot tell a reachable call from a dead one
// either way.
//
// This is a source-level guard rather than a wiring test on purpose.
// bootstrapLambda takes no injection seams and connects to a real database before
// reaching the config assignment, so exercising the wiring would need a live
// Postgres/DSQL — a heavyweight harness for a one-line assignment. What a source
// scan cannot see remains, for the call and the overlay alike: writing either
// symbol in a comment satisfies the scan without wiring anything. Observed, not
// assumed — the first #321 probe passed on its own doc comment. Closing that
// needs an AST parse (#429).
//
// It lives in this package because cmd/ has no shared test package.
func TestEntityConfigFromEnvWiredInEveryWriteEntryPoint(t *testing.T) {
	// Relative to this package directory, which is the cwd for `go test`.
	const cmdDir = ".."

	entries, err := os.ReadDir(cmdDir)
	if err != nil {
		t.Fatalf("failed to read cmd directory %s: %v", cmdDir, err)
	}

	buildsManager := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		builds, overlays, err := scanEntryPointDir(filepath.Join(cmdDir, entry.Name()))
		if err != nil {
			t.Fatalf("failed to scan cmd/%s: %v", entry.Name(), err)
		}
		if !builds {
			continue
		}
		buildsManager[entry.Name()] = true
		if !overlays {
			t.Errorf("cmd/%s builds an EntityManager but never calls "+
				"bootstrap.EntityConfigFromEnv, so VALIDATE_UPDATES_STRICT is silently "+
				"ignored on that deployment target (#314)", entry.Name())
		}
	}

	for _, name := range knownManagerBuilders {
		if !buildsManager[name] {
			t.Errorf("the walk did not classify cmd/%s as building an EntityManager; "+
				"it is reading the wrong tree or matching nothing, so its green would be "+
				"meaningless (#321)", name)
		}
	}
}

// scanEntryPointDir reports whether any non-test Go file anywhere in dir's
// subtree builds an EntityManager, and whether any of them applies the
// environment overlay.
//
// The whole subtree, not just dir itself: a nested package under cmd/<name> is
// part of that binary, and an entry point that grew a subdirectory would
// otherwise go unread — the same "never looked" hole #321 is about.
func scanEntryPointDir(dir string) (builds, overlays bool, err error) {
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", path, err)
		}
		text := string(source)
		// The prefix, not one exact wrapper: WithConfig and WithConfigContext both
		// build a manager, and so will the next wrapper.
		if strings.Contains(text, "factory.NewEntityManager") {
			builds = true
		}
		if strings.Contains(text, "bootstrap.EntityConfigFromEnv") {
			overlays = true
		}
		return nil
	})
	if err != nil {
		return false, false, fmt.Errorf("failed to walk %s: %w", dir, err)
	}
	return builds, overlays, nil
}
