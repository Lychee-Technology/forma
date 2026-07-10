//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDumpArtifacts drives a full write->flush->query cycle, forces a diff,
// and asserts every artifact kind exists and parses.
func TestDumpArtifacts(t *testing.T) {
	cluster := SharedCluster(t)
	tmp := t.TempDir()
	t.Setenv("E2E_ARTIFACTS_DIR", tmp)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	simple := DefaultSchemaFixtures()[0]

	script := env.GenerateScript(ScriptSpec{Schema: simple, Creates: 4, Updates: 2, Deletes: 1})
	if err := env.ApplyEvents(ctx, script...); err != nil {
		t.Fatalf("apply events: %v", err)
	}
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := env.Query(ctx, Query{Schema: simple, Limit: 10}); err != nil {
		t.Fatalf("query: %v", err)
	}

	// Force a diff so diff.json is written.
	state, err := env.ExpectedState(simple)
	if err != nil {
		t.Fatalf("expected state: %v", err)
	}
	expected, err := state.Run(Query{Schema: simple, Limit: 10})
	if err != nil {
		t.Fatalf("oracle run: %v", err)
	}
	expected.Total++ // deliberate mismatch
	env.lastDiff = &Diff{Query: Query{Schema: simple}, ExpectedTotal: expected.Total, ActualTotal: expected.Total - 1}

	dir, err := env.DumpArtifacts(ctx)
	if err != nil {
		t.Fatalf("dump artifacts: %v (dir %s)", err, dir)
	}
	if !strings.HasPrefix(dir, tmp) {
		t.Fatalf("artifact dir %s not under E2E_ARTIFACTS_DIR %s", dir, tmp)
	}

	for _, name := range []string{"run.json", "events.json", "change_log.json", "s3_listing.json", "manifest_20.json", "query_1.json", "diff.json"} {
		assertJSONArtifact(t, filepath.Join(dir, name))
	}

	// run.json must never carry the database password (external infra can
	// supply real credentials via PRODUCTION_E2E_EXTERNAL_PG_DSN).
	runInfo, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatalf("read run.json: %v", err)
	}
	if cluster.PGPassword != "" && strings.Contains(string(runInfo), cluster.PGPassword) {
		t.Error("run.json contains the postgres password")
	}
	if !strings.Contains(string(runInfo), redactedPassword) {
		t.Error("run.json pg_dsn is not redacted")
	}

	parquetFiles, err := filepath.Glob(filepath.Join(dir, "parquet", "*.json"))
	if err != nil || len(parquetFiles) == 0 {
		t.Fatalf("no parquet artifacts found (err=%v)", err)
	}
	var hasSchema, hasSample bool
	for _, f := range parquetFiles {
		assertJSONArtifact(t, f)
		if strings.HasSuffix(f, ".schema.json") {
			hasSchema = true
		}
		if strings.HasSuffix(f, ".sample.json") {
			hasSample = true
		}
	}
	if !hasSchema || !hasSample {
		t.Fatalf("parquet artifacts missing schema/sample pair: %v", parquetFiles)
	}

	// query_1.json must expose the rendered SQL and its parameters.
	data, err := os.ReadFile(filepath.Join(dir, "query_1.json"))
	if err != nil {
		t.Fatalf("read query_1.json: %v", err)
	}
	var payload struct {
		Plan struct {
			Sources []struct {
				Engine string   `json:"Engine"`
				SQL    string   `json:"SQL"`
				Params []string `json:"Params"`
			} `json:"Sources"`
		} `json:"plan"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("parse query_1.json: %v", err)
	}
	foundDuck := false
	for _, src := range payload.Plan.Sources {
		if src.Engine == "duckdb" && src.SQL != "" {
			foundDuck = true
		}
	}
	if !foundDuck {
		t.Error("query_1.json has no duckdb source with rendered SQL")
	}
}

// TestDumpArtifacts_UnprovisionedEnv covers the provisioning-failure path:
// even when the per-test database and DuckDB never came up, the dump must
// produce the uniform artifact set (with unavailable markers) instead of
// nothing.
func TestDumpArtifacts_UnprovisionedEnv(t *testing.T) {
	cluster := SharedCluster(t)
	tmp := t.TempDir()
	t.Setenv("E2E_ARTIFACTS_DIR", tmp)

	e := &Env{
		T:            t,
		Cluster:      cluster,
		RunID:        cluster.RunID + "/unprovisioned",
		DBName:       "e2e_never_created",
		S3Prefix:     "e2e/" + cluster.RunID + "/unprovisioned",
		provisionErr: fmt.Errorf("create database e2e_never_created: simulated failure"),
	}

	dir, err := e.DumpArtifacts(context.Background())
	if err != nil {
		t.Fatalf("dump artifacts for unprovisioned env: %v (dir %s)", err, dir)
	}

	for _, name := range []string{"run.json", "events.json", "change_log.json", "s3_listing.json", "diff.json"} {
		assertJSONArtifact(t, filepath.Join(dir, name))
	}

	runInfo, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		t.Fatalf("read run.json: %v", err)
	}
	if !strings.Contains(string(runInfo), "simulated failure") {
		t.Error("run.json does not record the provisioning error")
	}
	changeLog, err := os.ReadFile(filepath.Join(dir, "change_log.json"))
	if err != nil {
		t.Fatalf("read change_log.json: %v", err)
	}
	if !strings.Contains(string(changeLog), "unavailable") {
		t.Error("change_log.json missing the unavailable marker for an unprovisioned database")
	}
}

func assertJSONArtifact(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("artifact %s missing: %v", path, err)
	}
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("artifact %s is not valid JSON: %v", path, err)
	}
}
