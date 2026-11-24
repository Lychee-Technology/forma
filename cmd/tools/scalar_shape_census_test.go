package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
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
