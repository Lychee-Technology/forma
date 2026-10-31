package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
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
