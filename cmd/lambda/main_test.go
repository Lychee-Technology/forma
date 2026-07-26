package main

import (
	"context"
	"errors"
	"os"
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

// TestEntityConfigFromEnvWiredInEveryWriteEntryPoint guards that every entry point
// serving the write API overlays the entity config from the environment, so the
// #314 staged rollout (VALIDATE_UPDATES_STRICT) is available on all deployment
// targets rather than only the ones someone remembered to wire.
//
// This is a source-level guard rather than a wiring test on purpose.
// bootstrapLambda takes no injection seams and connects to a real database before
// reaching the config assignment, so exercising the wiring would need a live
// Postgres/DSQL — a heavyweight harness for a one-line assignment. Scanning the
// source costs nothing and catches the regression that actually matters: a third
// entry point added later that builds an EntityManager and forgets the overlay.
//
// It lives in this package because cmd/ has no shared test package; it
// deliberately covers cmd/server too rather than being duplicated in both.
func TestEntityConfigFromEnvWiredInEveryWriteEntryPoint(t *testing.T) {
	// Relative to this package directory, which is the cwd for `go test`.
	entryPoints := map[string]string{
		"cmd/lambda": "main.go",
		"cmd/server": "../server/main.go",
	}

	for name, path := range entryPoints {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read %s entry point %s: %v", name, path, err)
		}
		text := string(source)

		// Only entry points that actually build an EntityManager need the overlay.
		if !strings.Contains(text, "NewEntityManagerWithConfigContext") {
			continue
		}
		if !strings.Contains(text, "bootstrap.EntityConfigFromEnv") {
			t.Errorf("%s builds an EntityManager but never calls "+
				"bootstrap.EntityConfigFromEnv, so VALIDATE_UPDATES_STRICT is silently "+
				"ignored on that deployment target (#314)", name)
		}
	}
}
