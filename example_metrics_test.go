package forma_test

import (
	"context"
	"fmt"
	"sort"

	"github.com/lychee-technology/forma"
)

// logEmitter stands in for an application's own telemetry backend: an
// embedder adapts forma.Metric onto whatever it already uses (OpenTelemetry
// instruments, a Prometheus registry, StatsD, a log line) and hands the
// adapter to Forma through Config.Metrics.Emitter. Forma links none of them.
type logEmitter struct{}

func (logEmitter) EmitMetric(_ context.Context, m forma.Metric) {
	keys := make([]string, 0, len(m.Labels))
	for k := range m.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("%s %s %s %v", m.Name, m.Kind, m.Unit, m.Value)
	for _, k := range keys {
		fmt.Printf(" %s=%s", k, m.Labels[k])
	}
	fmt.Println()
}

func ExampleMetricEmitter() {
	cfg := forma.NewConfig(forma.WithMetricEmitter(logEmitter{}))
	// cfg is then passed to factory.NewEntityManagerWithConfigContext; every
	// metric that instance emits reaches logEmitter. This is what one
	// emission looks like on the wire:
	cfg.Metrics.Emitter.EmitMetric(context.Background(), forma.Metric{
		Name:   "entity_report_only_validation_violation_total",
		Kind:   forma.MetricKindCounter,
		Unit:   forma.MetricUnitCount,
		Labels: map[string]string{"schema_id": "12", "schema_name": "lead", "kind": "constraint"},
		Value:  1,
	})
	// Output: entity_report_only_validation_violation_total counter count 1 kind=constraint schema_id=12 schema_name=lead
}

// ExampleMetricCatalogue shows an emitter pre-registering one instrument per
// descriptor, the way a Prometheus vector or an OpenTelemetry instrument
// needs its name and label keys before the first emission.
func ExampleMetricCatalogue() {
	for _, d := range forma.MetricCatalogue() {
		if d.Kind == forma.MetricKindHistogram {
			fmt.Printf("%s: %s in %s, labels %v\n", d.Name, d.Kind, d.Unit, d.Labels)
		}
	}
	// Output: fed_query_latency_histogram: histogram in milliseconds, labels [stage]
}
