package main

import (
	"context"
	"errors"
	"fmt"
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
// nothing — wrong root, renamed directory, a scan that matches no files — fails
// loudly instead of passing vacuously.
var knownManagerBuilders = []string{"lambda", "server", "sample"}

// TestEntityConfigFromEnvWiredInEveryWriteEntryPoint guards that every entry point
// building an EntityManager overlays the entity config from the environment, so
// the #314 staged rollout (VALIDATE_UPDATES_STRICT) is available on all
// deployment targets rather than only the ones someone remembered to wire.
//
// It walks every directory under cmd/ and matches the "factory.NewEntityManager"
// prefix, so an entry point added later is read without anyone updating this
// test, and the WithConfig / WithConfigContext wrappers — and any future one —
// all count. Before #321 the directories were a hardcoded pair and the match was
// the exact WithConfigContext spelling, so the regression this comment names
// could not fail it: a scratch cmd/apiworker went unread, and the already
// existing cmd/sample used the unmatched WithConfig wrapper.
//
// Classification is per directory, not per file, so an entry point that builds
// its manager outside main.go is still covered.
//
// This is a source-level guard rather than a wiring test on purpose.
// bootstrapLambda takes no injection seams and connects to a real database before
// reaching the config assignment, so exercising the wiring would need a live
// Postgres/DSQL — a heavyweight harness for a one-line assignment. What a source
// scan cannot see remains: replacing the call with a comment naming the symbol
// still passes.
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

// scanEntryPointDir reports whether any non-test Go file directly in dir builds
// an EntityManager, and whether any of them applies the environment overlay.
func scanEntryPointDir(dir string) (builds, overlays bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, false, fmt.Errorf("failed to read %s: %w", dir, err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		source, err := os.ReadFile(path)
		if err != nil {
			return false, false, fmt.Errorf("failed to read %s: %w", path, err)
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
	}
	return builds, overlays, nil
}
