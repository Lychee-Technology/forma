package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func TestJSONLineMetricEmitterWritesOneStableLinePerMetric(t *testing.T) {
	var buf bytes.Buffer
	e := NewJSONLineMetricEmitter(&buf).(*jsonLineMetricEmitter)
	e.now = func() time.Time { return time.Date(2026, 9, 19, 6, 0, 0, 0, time.UTC) }

	e.EmitMetric(context.Background(), forma.Metric{
		Name:   "entity_report_only_validation_violation_total",
		Kind:   forma.MetricKindCounter,
		Unit:   forma.MetricUnitCount,
		Labels: map[string]string{"schema_id": "12", "schema_name": "lead", "kind": "constraint"},
		Value:  1,
	})
	e.EmitMetric(context.Background(), forma.Metric{Name: "fed_query_pushdown_efficiency", Kind: forma.MetricKindGauge, Unit: forma.MetricUnitRatio, Value: 0.25})

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	require.Len(t, lines, 2, "one line per emission:\n%s", buf.String())
	require.JSONEq(t, `{"type":"forma_metric","ts":"2026-09-19T06:00:00Z",`+
		`"name":"entity_report_only_validation_violation_total","kind":"counter","unit":"count","value":1,`+
		`"labels":{"kind":"constraint","schema_id":"12","schema_name":"lead"}}`, lines[0])
	require.JSONEq(t, `{"type":"forma_metric","ts":"2026-09-19T06:00:00Z",`+
		`"name":"fed_query_pushdown_efficiency","kind":"gauge","unit":"ratio","value":0.25,"labels":{}}`, lines[1])
}

func TestJSONLineMetricEmitterSerializesConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	e := NewJSONLineMetricEmitter(&buf)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.EmitMetric(context.Background(), forma.Metric{Name: "fed_query_row_count", Labels: map[string]string{"source": "pg"}, Value: 1})
		}()
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	require.Len(t, lines, 50)
	for _, line := range lines {
		var got map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &got), "line is not one JSON object: %q", line)
		require.Equal(t, "forma_metric", got["type"])
	}
}

func TestJSONLineMetricEmitterTreatsNilWriterAsDiscard(t *testing.T) {
	e := NewJSONLineMetricEmitter(nil)
	require.NotPanics(t, func() {
		e.EmitMetric(context.Background(), forma.Metric{Name: "fed_query_row_count", Labels: map[string]string{"source": "pg"}, Value: 1})
	})
}

// TestMetricEmitterFromEnv pins the entrypoint contract: unset or anything
// but true/1 leaves Forma on its no-op default, so a deployment that never
// heard of METRICS_STDOUT gets no new stdout output.
func TestMetricEmitterFromEnv(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", false}, {"false", false}, {"0", false}, {"yes", false}, {"true", true}, {"TRUE", true}, {"1", true},
	} {
		t.Run("METRICS_STDOUT="+tc.value, func(t *testing.T) {
			t.Setenv(MetricsStdoutEnv, tc.value)
			var buf bytes.Buffer
			e := MetricEmitterFromEnv(&buf)
			if !tc.want {
				require.Nil(t, e)
				return
			}
			require.NotNil(t, e)
			e.EmitMetric(context.Background(), forma.Metric{Name: "fed_query_row_count", Labels: map[string]string{"source": "pg"}, Value: 3})
			require.Contains(t, buf.String(), `"name":"fed_query_row_count"`)
		})
	}
}
