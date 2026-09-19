package bootstrap

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/lychee-technology/forma"
)

// MetricsStdoutEnv is the one telemetry switch the shipped entrypoints read:
// "true"/"1" makes every metric the Forma instance emits a JSON line on
// stdout. Off by default, so an unconfigured process writes nothing extra.
const MetricsStdoutEnv = "METRICS_STDOUT"

// MetricEmitterFromEnv returns the stdout JSON-line emitter when
// METRICS_STDOUT is on, and nil — Forma's no-op default — otherwise. It is
// what cmd/server and cmd/lambda set on Config.Metrics.Emitter (#423): the
// demo binaries choose no metrics backend, they make the events visible.
func MetricEmitterFromEnv(w io.Writer) forma.MetricEmitter {
	if !EnvBool(MetricsStdoutEnv, false) {
		return nil
	}
	return NewJSONLineMetricEmitter(w)
}

// metricLine is the wire shape of one emitted metric. The field set is
// stable: a log pipeline can key on "type":"forma_metric" and read the
// catalogued name, kind, unit and labels verbatim.
type metricLine struct {
	Type   string            `json:"type"`
	TS     string            `json:"ts"`
	Name   string            `json:"name"`
	Kind   forma.MetricKind  `json:"kind"`
	Unit   forma.MetricUnit  `json:"unit"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels"`
}

type jsonLineMetricEmitter struct {
	mu  sync.Mutex
	w   io.Writer
	now func() time.Time
}

// NewJSONLineMetricEmitter writes one JSON object per emitted metric,
// newline-terminated, to w. Writes are serialized so concurrent emissions
// never interleave. A write error is dropped: telemetry never fails the
// operation that emitted, and stdout has no better place to report to. A nil
// w discards every line for the same reason: the emitter must stay total
// rather than panic on first use.
func NewJSONLineMetricEmitter(w io.Writer) forma.MetricEmitter {
	if w == nil {
		w = io.Discard
	}
	return &jsonLineMetricEmitter{w: w, now: time.Now}
}

func (e *jsonLineMetricEmitter) EmitMetric(_ context.Context, m forma.Metric) {
	labels := m.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	line, err := json.Marshal(metricLine{
		Type:   "forma_metric",
		TS:     e.now().UTC().Format(time.RFC3339Nano),
		Name:   m.Name,
		Kind:   m.Kind,
		Unit:   m.Unit,
		Value:  m.Value,
		Labels: labels,
	})
	if err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, _ = e.w.Write(append(line, '\n'))
}
