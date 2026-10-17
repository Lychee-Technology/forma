package main

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
)

// DSQL_ENDPOINT opens the Aurora DSQL pool, so it must also select the engine
// that locks rows the DSQL way; a DB_ENGINE that contradicts it is refused
// rather than letting one of them silently win (#633).
func TestLambdaEngineFollowsTheDSQLEndpoint(t *testing.T) {
	cases := []struct {
		name       string
		configured forma.DatabaseEngine
		endpoint   string
		want       forma.DatabaseEngine
		refused    bool
	}{
		{name: "postgres default", want: ""},
		{name: "postgres explicit", configured: forma.EnginePostgreSQL, want: forma.EnginePostgreSQL},
		{name: "endpoint implies dsql", endpoint: "example.dsql.us-east-2.on.aws", want: forma.EngineAuroraDSQL},
		{name: "endpoint and dsql agree", configured: forma.EngineAuroraDSQL, endpoint: "example.dsql.us-east-2.on.aws", want: forma.EngineAuroraDSQL},
		{name: "endpoint contradicts postgres", configured: forma.EnginePostgreSQL, endpoint: "example.dsql.us-east-2.on.aws", refused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := lambdaEngine(tc.configured, tc.endpoint)
			if tc.refused {
				var configErr *forma.ConfigError
				if !errors.As(err, &configErr) || configErr.Field != "database.engine" {
					t.Fatalf("expected a database.engine *forma.ConfigError, got %v", err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("lambdaEngine(%q, %q) = %q, %v; want %q", tc.configured, tc.endpoint, got, err, tc.want)
			}
		})
	}
}

// The resolved engine reaches the manager's config, and an unknown DB_ENGINE
// fails the cold start in Config.Validate before any I/O.
func TestLambdaConfigFromEnvResolvesTheEngine(t *testing.T) {
	t.Setenv("DSQL_ENDPOINT", "example.dsql.us-east-2.on.aws")
	t.Setenv("DB_ENGINE", "")
	config, dbConfig, err := lambdaConfigFromEnv(t.TempDir())
	if err != nil {
		t.Fatalf("lambdaConfigFromEnv: %v", err)
	}
	if config.Database.Engine != forma.EngineAuroraDSQL || dbConfig.Engine != forma.EngineAuroraDSQL {
		t.Fatalf("expected aurora-dsql in both configs, got %q and %q", config.Database.Engine, dbConfig.Engine)
	}

	t.Setenv("DSQL_ENDPOINT", "")
	t.Setenv("DB_ENGINE", "aurora")
	_, _, err = lambdaConfigFromEnv(t.TempDir())
	var configErr *forma.ConfigError
	if !errors.As(err, &configErr) || configErr.Field != "database.engine" {
		t.Fatalf("expected a database.engine *forma.ConfigError, got %v", err)
	}
}
