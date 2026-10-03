package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// serveManifest stands in for the bucket: it answers the path-style GET for
// one manifest object and fails the test on any other request, so a pass that
// stops being a no-op is caught here rather than against a store that cannot
// answer it.
func serveManifest(t *testing.T, path string, m manifest.Manifest) (endpoint string, gets *atomic.Int32) {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	gets = &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != path {
			t.Errorf("unexpected S3 request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		gets.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, gets
}

// TestRunCompactor_MetricsStdout drives the compactor subcommand itself, flag
// parsing through RunOnce, against an S3 endpoint holding one manifest (#594).
// The pass is a no-op: a quarter of the base's rows sit in delta, under the 50%
// rewrite threshold. That is enough to emit compaction_dirty_ratio, so the
// writer the subcommand was handed shows whether the Compactor it built
// carries a sink: with METRICS_STDOUT on the gauge arrives as one JSON line,
// and with it unset the same pass writes nothing.
func TestRunCompactor_MetricsStdout(t *testing.T) {
	oldLogger := toolLoggerFactoryProd
	t.Cleanup(func() { toolLoggerFactoryProd = oldLogger })
	toolLoggerFactoryProd = func(...zap.Option) (*zap.Logger, error) { return zap.NewNop(), nil }

	for _, tc := range []struct {
		name     string
		env      string
		wantLine bool
	}{
		{name: "on", env: "true", wantLine: true},
		{name: "unset", env: "", wantLine: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainLoaded := false
			stubToolAWSChainLoader(t, &chainLoaded)
			setStaticS3Env(t)
			t.Setenv(bootstrap.MetricsStdoutEnv, tc.env)

			endpoint, gets := serveManifest(t, "/bkt/manifest/7.json", manifest.Manifest{
				SchemaID: 7,
				Version:  1,
				Files: []manifest.FileEntry{
					{Tier: "base", Path: "s3://bkt/data/7/base/a.parquet", RowIDMin: "a", RowIDMax: "b", SizeBytes: 4096, RowCount: 100},
					{Tier: "delta", Path: "s3://bkt/data/7/delta/b.parquet", RowIDMin: "c", RowIDMax: "d", SizeBytes: 1024, RowCount: 25},
				},
			})

			var out bytes.Buffer
			err := runCompactorOut(context.Background(), []string{
				"--schema-id", "7",
				"--s3-bucket", "bkt",
				"--s3-endpoint", endpoint,
				"--s3-use-path=true",
				"--s3-use-ssl=false",
				"--dirty-ratio-pct", "50",
			}, &out)
			if err != nil {
				t.Fatalf("compactor run: %v", err)
			}
			if gets.Load() == 0 {
				t.Fatal("the pass never loaded the manifest, so it proves nothing about what it emits")
			}

			if !tc.wantLine {
				if out.Len() != 0 {
					t.Fatalf("METRICS_STDOUT is unset, so the run must write no metric line; got %q", out.String())
				}
				return
			}
			assertDirtyRatioLine(t, out.String())
		})
	}
}

// assertDirtyRatioLine requires every line of a successful run's metric stream
// to be a forma_metric object, and one of them to be schema 7's dirty ratio.
func assertDirtyRatioLine(t *testing.T, stream string) {
	t.Helper()
	type line struct {
		Type   string            `json:"type"`
		Name   string            `json:"name"`
		Kind   string            `json:"kind"`
		Unit   string            `json:"unit"`
		Value  float64           `json:"value"`
		Labels map[string]string `json:"labels"`
	}
	if stream == "" {
		t.Fatal("METRICS_STDOUT=true must put the pass's metrics on the writer; it stayed empty")
	}
	found := false
	for _, raw := range strings.Split(strings.TrimSuffix(stream, "\n"), "\n") {
		var got line
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Fatalf("metric stream line is not one JSON object: %q (%v)", raw, err)
		}
		if got.Type != "forma_metric" {
			t.Fatalf("metric stream carries a line that is not a forma_metric: %q", raw)
		}
		if got.Name != "compaction_dirty_ratio" {
			continue
		}
		found = true
		if got.Kind != "gauge" || got.Unit != "ratio" || got.Value != 0.25 || len(got.Labels) != 1 || got.Labels["schema_id"] != "7" {
			t.Fatalf("compaction_dirty_ratio line = %q, want a ratio gauge of 0.25 labelled schema_id=7", raw)
		}
	}
	if !found {
		t.Fatalf("the metric stream has no compaction_dirty_ratio line: %q", stream)
	}
}
