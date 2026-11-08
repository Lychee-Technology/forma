package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/bootstrap"
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

// TestBootstrapLambda_RejectsUnparsableIntegerEnv pins #600 on the Lambda
// entry point: a set but unparsable integer variable fails the cold start
// with an *EnvError naming it. The DSQL case matters most: its pool settings
// used to be read inside createDSQLPool, after the AWS config load, and an
// unparsable value kept the default there too. The context is already
// canceled, so the env check is shown to run before any I/O; reaching the
// pool would report context.Canceled instead.
func TestBootstrapLambda_RejectsUnparsableIntegerEnv(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{
			name: "postgres",
			env:  map[string]string{"DSQL_ENDPOINT": "", "DB_PORT": "tcp://10.0.0.7:5432", "MAX_BATCH_SIZE": "1_000"},
			want: []string{`DB_PORT="tcp://10.0.0.7:5432"`, `MAX_BATCH_SIZE="1_000"`},
		},
		{
			name: "dsql",
			env:  map[string]string{"DSQL_ENDPOINT": "example.dsql.us-east-2.on.aws", "DB_MAX_CONNECTIONS": "twenty", "DB_TIMEOUT_SECONDS": "30s"},
			want: []string{`DB_MAX_CONNECTIONS="twenty"`, `DB_TIMEOUT_SECONDS="30s"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := bootstrapLambda(ctx, zap.NewNop().Sugar())

			var envErr *bootstrap.EnvError
			if !errors.As(err, &envErr) {
				t.Fatalf("expected an *bootstrap.EnvError, got %v", err)
			}
			if errors.Is(err, context.Canceled) {
				t.Fatalf("the environment must be refused before the pool is opened, got %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("cold-start error must name %s, got %v", want, err)
				}
			}
		})
	}
}

// TestBootstrapLambda_RejectsUnrecognisedBooleanEnv pins #642 on the Lambda
// entry point: VALIDATE_UPDATES_STRICT and METRICS_STDOUT set to a value that
// is not a boolean fail the cold start with an *EnvError naming each, where
// they used to read as false, and they join the integer overlays' failures.
// Both are read before the database mode is chosen, so one mode covers them.
// The context is already canceled, so the check is shown to run before any
// I/O.
func TestBootstrapLambda_RejectsUnrecognisedBooleanEnv(t *testing.T) {
	t.Setenv("DSQL_ENDPOINT", "")
	t.Setenv("VALIDATE_UPDATES_STRICT", "on")
	t.Setenv("METRICS_STDOUT", "yes")
	t.Setenv("MAX_BATCH_SIZE", "1_000")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := bootstrapLambda(ctx, zap.NewNop().Sugar())

	var envErr *bootstrap.EnvError
	if !errors.As(err, &envErr) {
		t.Fatalf("expected an *bootstrap.EnvError, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("the environment must be refused before the pool is opened, got %v", err)
	}
	for _, want := range []string{`VALIDATE_UPDATES_STRICT="on"`, `METRICS_STDOUT="yes"`, `MAX_BATCH_SIZE="1_000"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("cold-start error must name %s, got %v", want, err)
		}
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
// For each child directory of cmd/ it parses every non-test Go file in that
// child's whole subtree, so an entry point added later is read without anyone
// updating this test (#321). A tree builds a manager when its code refers to
// any factory.NewEntityManager* function, so the WithConfig / WithConfigContext
// wrappers, and any future one, all count.
//
// The scan reads the AST, not the text (#429). Before that a comment naming
// either symbol satisfied it: the first #321 mutation probe passed on its own
// doc comment. Now only code counts, and the overlay counts only where it is
// called from a declaration reachable from main or package initialisation.
// Reachability is followed across files and into helper packages of the same
// tree, so an entry point that builds its manager in one file and applies the
// overlay from a sibling file or a helper subpackage still passes, while an
// overlay call in a function nothing calls, or in a tree package main does not
// import, no longer does. The fixture tests in entrypoint_scan_fixture_test.go
// pin each of those cases.
//
// Reachability is syntactic, with no type checking, and over-approximates: it
// does not prove the overlay runs before the manager is built, only that the
// binary can reach it. A contrived evasion (a local variable shadowing the name
// of a dead overlay function) can still pass, but a wired entry point split
// across files or packages does not fail.
//
// This is a source-level guard rather than a wiring test on purpose.
// bootstrapLambda takes no injection seams and connects to a real database before
// reaching the config assignment, so exercising the wiring would need a live
// Postgres/DSQL, a heavyweight harness for a one-line assignment.
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
		scan, err := scanEntryPointTree(filepath.Join(cmdDir, entry.Name()), formaModulePath+"/cmd/"+entry.Name())
		if err != nil {
			t.Fatalf("failed to scan cmd/%s: %v", entry.Name(), err)
		}
		if !scan.builds {
			continue
		}
		buildsManager[entry.Name()] = true
		if !scan.overlays {
			t.Errorf("cmd/%s builds an EntityManager but never reaches a call to "+
				"bootstrap.EntityConfigFromEnv from main, so VALIDATE_UPDATES_STRICT is "+
				"silently ignored on that deployment target (#314)", entry.Name())
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
