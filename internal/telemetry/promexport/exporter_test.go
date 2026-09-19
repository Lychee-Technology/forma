package promexport

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/telemetry"
)

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	return string(body)
}

// TestExporterScrapesEveryKind pins the mapping from catalogue kinds onto
// Prometheus types: counters accumulate, gauges hold the last value, histograms
// bucket observations — and every series carries the descriptor's labels under
// the catalogue name, unprefixed.
func TestExporterScrapesEveryKind(t *testing.T) {
	exp, err := New(zap.NewNop())
	require.NoError(t, err)
	ctx := context.Background()

	exp.Emit(ctx, "entity_report_only_validation_violation_total",
		map[string]string{"schema_id": "7", "schema_name": "lead", "kind": "required"}, int64(1))
	exp.Emit(ctx, "entity_report_only_validation_violation_total",
		map[string]string{"schema_id": "7", "schema_name": "lead", "kind": "required"}, int64(1))
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"schema_id": "7"}, 0.5)
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"schema_id": "7"}, 0.25)
	exp.Emit(ctx, "fed_query_latency_histogram", map[string]string{"stage": "execution"}, int64(30))

	body := scrape(t, exp.Handler())
	require.Contains(t, body, `entity_report_only_validation_violation_total{kind="required",schema_id="7",schema_name="lead"} 2`)
	require.Contains(t, body, `compaction_dirty_ratio{schema_id="7"} 0.25`)
	require.Contains(t, body, `fed_query_latency_histogram_bucket{stage="execution",le="50"} 1`)
	require.Contains(t, body, `fed_query_latency_histogram_bucket{stage="execution",le="25"} 0`)
	require.Contains(t, body, `fed_query_latency_histogram_sum{stage="execution"} 30`)
	require.Contains(t, body, `fed_query_latency_histogram_count{stage="execution"} 1`)
	// The Go runtime and process collectors ride along, as operators expect.
	require.Contains(t, body, "go_goroutines")
}

// TestExporterDropsWhatTheCatalogueRefuses pins the failure mode for an
// emission the contract does not cover: it is counted under the exporter's own
// drop counter and never panics or invents a series.
func TestExporterDropsWhatTheCatalogueRefuses(t *testing.T) {
	exp, err := New(zap.NewNop())
	require.NoError(t, err)
	ctx := context.Background()

	exp.Emit(ctx, "not_in_catalogue", map[string]string{"a": "b"}, int64(1))
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"schema_id": "7"}, int64(1)) // gauge wants float64
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"wrong": "7"}, 0.5)          // label key mismatch

	body := scrape(t, exp.Handler())
	require.NotContains(t, body, "not_in_catalogue")
	require.NotContains(t, body, `compaction_dirty_ratio{`)
	require.Contains(t, body, `forma_telemetry_dropped_total{reason="unknown_metric"} 1`)
	require.Contains(t, body, `forma_telemetry_dropped_total{reason="value_type"} 1`)
	require.Contains(t, body, `forma_telemetry_dropped_total{reason="labels"} 1`)
}

// TestExporterRegistersEveryCatalogueEntry guards that New fails loudly if a
// descriptor cannot be registered (duplicate name, bad label) instead of
// silently serving a partial catalogue.
func TestExporterRegistersEveryCatalogueEntry(t *testing.T) {
	exp, err := New(zap.NewNop())
	require.NoError(t, err)
	for _, d := range telemetry.Catalogue() {
		_, ok := exp.instruments[d.Name]
		require.Truef(t, ok, "descriptor %s has no instrument", d.Name)
	}
	require.False(t, strings.Contains(scrape(t, exp.Handler()), "\n\n\n"))
}
