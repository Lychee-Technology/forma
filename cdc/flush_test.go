package cdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	internalcdc "github.com/lychee-technology/forma/internal/cdc"
	"go.uber.org/zap"
)

func TestRunOnceRequiresSchemaRegistry(t *testing.T) {
	_, err := RunOnce(context.Background(), Config{}, nil, false, zap.NewNop(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "schema registry is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunnerRunOnceRequiresSchemaRegistry(t *testing.T) {
	runner := NewRunner(zap.NewNop())
	_, err := runner.RunOnce(context.Background(), Config{}, nil, false, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "schema registry is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// emptyRegistry satisfies the registry precondition; the manifest
// configuration refusals come before any schema is looked up.
type emptyRegistry struct{}

func (emptyRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, errors.New("no schemas")
}

func (emptyRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, errors.New("no schemas")
}

func (emptyRegistry) GetSchemaByName(string) (int16, forma.JSONSchema, error) {
	return 0, forma.JSONSchema{}, errors.New("no schemas")
}

func (emptyRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, errors.New("no schemas")
}

func (emptyRegistry) ListSchemas() []string { return nil }

// #474: a caller outside the module's internal packages must be able to tell
// the manifest configuration refusals from operational failures with
// errors.Is, through both public entry points. Neither case needs AWS,
// Postgres or DuckDB: the configuration is refused before any of them is set
// up.
func TestRunOnceManifestConfigurationRefusalsMatchPublicSentinels(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"template-less run without glob-only", Config{}, ErrManifestStoreUnwired},
		{"glob-only run with a template", Config{GlobOnly: true, ManifestTemplate: "manifest/{{.SchemaID}}.json"}, ErrGlobOnlyWithTemplate},
	}
	entrypoints := map[string]func(Config) error{
		"RunOnce": func(cfg Config) error {
			_, err := RunOnce(context.Background(), cfg, nil, false, zap.NewNop(), emptyRegistry{})
			return err
		},
		"FlushRunner.RunOnce": func(cfg Config) error {
			runner := NewFlushRunner(zap.NewNop())
			defer func() { _ = runner.Close() }()
			_, err := runner.RunOnce(context.Background(), cfg, nil, false, emptyRegistry{})
			return err
		},
	}
	for _, tc := range cases {
		for entry, run := range entrypoints {
			t.Run(tc.name+"/"+entry, func(t *testing.T) {
				err := run(tc.cfg)
				if !errors.Is(err, tc.want) {
					t.Fatalf("errors.Is(%v, %v) = false", err, tc.want)
				}
			})
		}
	}
}

// #768: a lost schema lock reaches RunOnce's error, so a scheduler on the
// public API must be able to tell it (retry later) from a configuration or
// data error with errors.Is. The real path needs a database whose lease
// lapses mid-flush, so this builds the error the way the flush path does: a
// held context cancelled with the loss as its cause (the lease renewal loop),
// named by WithLockLoss over the cut-short work (processSchema), then wrapped
// per schema and joined (processSchemas). internal/cdc's
// TestProcessSchemas_ReturnsALostLock drives that path itself.
func TestErrSchemaLockLostMatchesTheFlushPathError(t *testing.T) {
	if ErrSchemaLockLost != internalcdc.ErrSchemaLockLost {
		t.Fatal("cdc.ErrSchemaLockLost must be the value internal/cdc returns")
	}
	held, cancel := context.WithCancelCause(context.Background())
	cancel(fmt.Errorf("%w: schema 7's lease was not renewed within 45s", internalcdc.ErrSchemaLockLost))
	work := fmt.Errorf("execute flush: %w", held.Err())
	err := errors.Join(
		fmt.Errorf("schema 7: %w", internalcdc.WithLockLoss(held, work)),
		fmt.Errorf("schema 8: %w", errors.New("unrelated failure")),
	)
	if !errors.Is(err, ErrSchemaLockLost) {
		t.Fatalf("errors.Is(%v, cdc.ErrSchemaLockLost) = false", err)
	}
	if errors.Is(errors.New("schema lock lost"), ErrSchemaLockLost) {
		t.Fatal("matching must be by value, not by message")
	}
}

// #768: a schema whose attribute metadata cache cannot be resolved fails the
// flush pre-flight, and both RunOnce paths return that error. The real path
// needs AWS and PostgreSQL before it reaches the pre-flight, so this builds
// the error the way the flush path does: the registry lookup failure wrapped
// by resolveRequiredAttrCache, or its empty-cache refusal, then wrapped by
// processSchemas' pre-flight frame. internal/cdc's
// TestProcessSchemas_AbortsWhenSchemaCacheUnavailable drives that path itself.
func TestErrSchemaAttrCacheUnavailableMatchesTheFlushPathError(t *testing.T) {
	if ErrSchemaAttrCacheUnavailable != internalcdc.ErrSchemaAttrCacheUnavailable {
		t.Fatal("cdc.ErrSchemaAttrCacheUnavailable must be the value internal/cdc returns")
	}
	lookupErr := errors.New("schema unavailable")
	cases := map[string]error{
		"registry lookup failed": fmt.Errorf("resolve attribute cache for schema %d: %w: %w",
			7, internalcdc.ErrSchemaAttrCacheUnavailable, lookupErr),
		"empty cache": fmt.Errorf("schema %d has an empty attribute metadata cache: %w",
			7, internalcdc.ErrSchemaAttrCacheUnavailable),
	}
	for name, cause := range cases {
		t.Run(name, func(t *testing.T) {
			err := fmt.Errorf("cdc flush pre-flight: %w", cause)
			if !errors.Is(err, ErrSchemaAttrCacheUnavailable) {
				t.Fatalf("errors.Is(%v, cdc.ErrSchemaAttrCacheUnavailable) = false", err)
			}
			if errors.Is(err, ErrSchemaLockLost) {
				t.Fatal("a configuration failure must not match the retryable lost-lock sentinel")
			}
		})
	}
	if errors.Is(errors.New("schema attribute metadata cache unavailable"), ErrSchemaAttrCacheUnavailable) {
		t.Fatal("matching must be by value, not by message")
	}
}
