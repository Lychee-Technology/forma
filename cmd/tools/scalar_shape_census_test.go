package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
)

const censusDummyPassword = "census-dummy-secret-696"

// The census's help and usage text never print the database password the
// environment configures (#696 review): its flag has no printable default,
// and the fallback is resolved after parsing.
func TestScalarShapeCensusHelpOmitsThePassword(t *testing.T) {
	t.Setenv("DB_PASSWORD", censusDummyPassword)
	for _, args := range [][]string{{"-h"}, {"-no-such-flag"}} {
		var out strings.Builder
		_ = runScalarShapeCensusOut(context.Background(), args, &out)
		if !strings.Contains(out.String(), "-db-password") {
			t.Fatalf("%v: expected the usage text, got %q", args, out.String())
		}
		if strings.Contains(out.String(), censusDummyPassword) {
			t.Fatalf("%v: the usage text prints the configured password:\n%s", args, out.String())
		}
	}
}

// The password is still the flag's, else DB_PASSWORD's, else postgres.
func TestScalarShapeCensusPasswordPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string
		args []string
		want string
	}{
		{"flag", censusDummyPassword, []string{"-db-password", "from-flag"}, "from-flag"},
		{"environment", censusDummyPassword, nil, censusDummyPassword},
		{"built-in", "", nil, "postgres"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DB_PASSWORD", tc.env)
			old := toolPostgresPoolFn
			t.Cleanup(func() { toolPostgresPoolFn = old })
			var got string
			stop := errors.New("stop before connecting")
			toolPostgresPoolFn = func(_ context.Context, cfg forma.DatabaseConfig) (*pgxpool.Pool, error) {
				got = cfg.Password
				return nil, stop
			}
			args := append([]string{"-schema-dir", t.TempDir(), "-schema-registry-table", "schema_registry"}, tc.args...)
			var out strings.Builder
			if err := runScalarShapeCensusOut(context.Background(), args, &out); !errors.Is(err, stop) {
				t.Fatalf("expected the pool request, got %v\n%s", err, out.String())
			}
			if got != tc.want {
				t.Fatalf("password: got %q, want %q", got, tc.want)
			}
		})
	}
}

// Where a parent is an object in one alternative and an array of objects in
// another, a record's indices do not say which the row took (#696 review):
// two indices under items.phones are a pre-#204 array's element inside an
// array of objects, or an array stored at an element of that array inside
// the object. The census claims no certainty either way: the depth every
// reading admits is no finding, a depth only one admits is unverified, and
// only a depth past every reading is verified.
func TestScalarShapeCensusAmbiguousAncestry(t *testing.T) {
	dir := t.TempDir()
	phones := `{"type": "object", "properties": {"phones": {"type": "array", "items": {"type": "string"}}}}`
	writeSchemaConsistencyArtifacts(t, dir, "ambiguous",
		`{"type": "object", "properties": {"items": {"anyOf": [`+phones+`, {"type": "array", "items": `+phones+`}]}}}`,
		`{"items.phones": {"attributeID": 1, "valueType": "text"}}`)
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	id, cache, err := registry.GetSchemaAttributeCacheByName("ambiguous")
	if err != nil {
		t.Fatalf("load attributes: %v", err)
	}
	declared := transform.SchemaShapesOf(registry, id).Declared("items.phones")
	for depth, want := range map[int]shapeCensusClass{1: "", 2: shapeUnverified, 3: shapeVerified} {
		class, legitimate := classifyShapeRecords(declared, cache["items.phones"], depth)
		if class != want || legitimate != 1 {
			t.Fatalf("depth %d: got %q with %d legitimate indices, want %q with 1", depth, class, legitimate, want)
		}
	}
}
