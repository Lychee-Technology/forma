// Package emfexport adapts the telemetry emitter hook to the CloudWatch
// Embedded Metric Format (#423): one single-line JSON object per emission,
// written to a log stream CloudWatch Logs already ingests (Lambda stdout, an
// ECS awslogs driver), from which CloudWatch extracts the metric with no
// collector, sidecar or extension in the path.
//
// Format reference: the _aws block declares the namespace, one dimension set
// and the metric name/unit; the dimension values and the metric value sit
// beside it as top-level keys.
package emfexport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/telemetry"
)

// Exporter is a telemetry.Emitter that writes EMF lines to a writer.
type Exporter struct {
	mu        sync.Mutex
	w         io.Writer
	namespace string
	logger    *zap.Logger
	now       func() time.Time
}

// New builds an exporter writing to w under the given CloudWatch namespace.
func New(w io.Writer, namespace string, logger *zap.Logger) *Exporter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Exporter{w: w, namespace: namespace, logger: logger, now: time.Now}
}

type metricDirective struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type metricBlock struct {
	Namespace  string            `json:"Namespace"`
	Dimensions [][]string        `json:"Dimensions"`
	Metrics    []metricDirective `json:"Metrics"`
}

type awsBlock struct {
	Timestamp         int64         `json:"Timestamp"`
	CloudWatchMetrics []metricBlock `json:"CloudWatchMetrics"`
}

// Emit is the telemetry.Emitter. An emission outside the catalogue contract is
// dropped and logged rather than written: CloudWatch would otherwise mint a
// metric under a shape no dashboard expects.
func (e *Exporter) Emit(_ context.Context, name string, labels map[string]string, value any) {
	desc, ok := telemetry.Lookup(name)
	if !ok {
		e.drop(name, fmt.Errorf("metric %q is not in the telemetry catalogue", name))
		return
	}
	v, err := desc.Kind.Coerce(value)
	if err != nil {
		e.drop(name, err)
		return
	}
	values, err := desc.LabelValues(labels)
	if err != nil {
		e.drop(name, err)
		return
	}

	record := make(map[string]any, len(desc.Labels)+2)
	for i, key := range desc.Labels {
		record[key] = values[i]
	}
	record[desc.Name] = v
	record["_aws"] = awsBlock{
		Timestamp: e.now().UnixMilli(),
		CloudWatchMetrics: []metricBlock{{
			Namespace:  e.namespace,
			Dimensions: [][]string{append([]string(nil), desc.Labels...)},
			Metrics:    []metricDirective{{Name: desc.Name, Unit: unitOf(desc)}},
		}},
	}
	line, err := json.Marshal(record)
	if err != nil {
		e.drop(name, fmt.Errorf("failed to encode EMF record: %w", err))
		return
	}
	e.write(name, append(line, '\n'))
}

// unitOf maps the catalogue unit onto the CloudWatch unit vocabulary.
func unitOf(d telemetry.Descriptor) string {
	switch d.Unit {
	case telemetry.UnitMilliseconds:
		return "Milliseconds"
	case telemetry.UnitCount:
		return "Count"
	default:
		return "None"
	}
}

func (e *Exporter) write(name string, line []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.w.Write(line); err != nil {
		e.logger.Warn("failed to write EMF telemetry line", zap.String("metric", name), zap.Error(err))
	}
}

func (e *Exporter) drop(name string, err error) {
	e.logger.Warn("telemetry emission dropped by EMF exporter", zap.String("metric", name), zap.Error(err))
}
