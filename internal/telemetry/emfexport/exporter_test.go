package emfexport

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func fixedClock() time.Time { return time.Unix(1_700_000_000, 5_000_000).UTC() }

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		require.NoErrorf(t, json.Unmarshal([]byte(line), &m), "line %q is not JSON", line)
		out = append(out, m)
	}
	return out
}

// TestExporterWritesOneEMFLinePerEmission pins the CloudWatch Embedded Metric
// Format shape: a single-line JSON object whose _aws block declares the
// namespace, one dimension set in catalogue label order, and the metric with
// its unit, alongside the dimension values and the metric value as top-level
// keys.
func TestExporterWritesOneEMFLinePerEmission(t *testing.T) {
	var buf bytes.Buffer
	exp := New(&buf, "forma-test", zap.NewNop())
	exp.now = fixedClock

	exp.Emit(context.Background(), "entity_report_only_validation_violation_total",
		map[string]string{"kind": "required", "schema_name": "lead", "schema_id": "7"}, int64(1))

	lines := decodeLines(t, &buf)
	require.Len(t, lines, 1)
	require.True(t, strings.HasSuffix(buf.String(), "\n"))
	require.Equal(t, 1, strings.Count(buf.String(), "\n"))

	got := lines[0]
	require.Equal(t, "7", got["schema_id"])
	require.Equal(t, "lead", got["schema_name"])
	require.Equal(t, "required", got["kind"])
	require.Equal(t, float64(1), got["entity_report_only_validation_violation_total"])

	aws := got["_aws"].(map[string]any)
	require.Equal(t, float64(1_700_000_000_005), aws["Timestamp"])
	metrics := aws["CloudWatchMetrics"].([]any)
	require.Len(t, metrics, 1)
	block := metrics[0].(map[string]any)
	require.Equal(t, "forma-test", block["Namespace"])
	require.Equal(t, []any{[]any{"schema_id", "schema_name", "kind"}}, block["Dimensions"])
	require.Equal(t, []any{map[string]any{
		"Name": "entity_report_only_validation_violation_total",
		"Unit": "Count",
	}}, block["Metrics"])
}

// TestExporterMapsUnits pins kind/unit → CloudWatch unit: latency histograms
// are Milliseconds, ratio gauges are None, counters are Count.
func TestExporterMapsUnits(t *testing.T) {
	var buf bytes.Buffer
	exp := New(&buf, "forma-test", zap.NewNop())
	ctx := context.Background()

	exp.Emit(ctx, "fed_query_latency_histogram", map[string]string{"stage": "execution"}, int64(30))
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"schema_id": "7"}, 0.25)
	exp.Emit(ctx, "fed_query_row_count", map[string]string{"source": "pg"}, int64(12))

	lines := decodeLines(t, &buf)
	require.Len(t, lines, 3)
	unitOf := func(m map[string]any) string {
		block := m["_aws"].(map[string]any)["CloudWatchMetrics"].([]any)[0].(map[string]any)
		return block["Metrics"].([]any)[0].(map[string]any)["Unit"].(string)
	}
	require.Equal(t, "Milliseconds", unitOf(lines[0]))
	require.Equal(t, float64(30), lines[0]["fed_query_latency_histogram"])
	require.Equal(t, "None", unitOf(lines[1]))
	require.Equal(t, 0.25, lines[1]["compaction_dirty_ratio"])
	require.Equal(t, "Count", unitOf(lines[2]))
	require.Equal(t, float64(12), lines[2]["fed_query_row_count"])
}

// TestExporterDropsWhatTheCatalogueRefuses pins that an off-contract emission
// writes nothing: CloudWatch would otherwise create a metric under a shape no
// dashboard expects.
func TestExporterDropsWhatTheCatalogueRefuses(t *testing.T) {
	var buf bytes.Buffer
	exp := New(&buf, "forma-test", zap.NewNop())
	ctx := context.Background()

	exp.Emit(ctx, "not_in_catalogue", map[string]string{"a": "b"}, int64(1))
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"schema_id": "7"}, int64(1))
	exp.Emit(ctx, "compaction_dirty_ratio", map[string]string{"wrong": "7"}, 0.5)

	require.Empty(t, buf.String())
}
