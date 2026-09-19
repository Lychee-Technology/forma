// Package promexport adapts the telemetry emitter hook to a Prometheus
// registry served over a scrape endpoint (#423). One instrument is built per
// catalogue descriptor at construction, so the wire names and label sets the
// catalogue pins are the only ones that can ever be served.
package promexport

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/telemetry"
)

// latencyBucketsMS spans the federated-query stages: sub-millisecond
// translation up to a 30s execution that a keyset page could take on cold S3.
var latencyBucketsMS = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}

// instrument is the Prometheus vector behind one descriptor. Exactly one of
// the three is set, by the descriptor's kind.
type instrument struct {
	desc      telemetry.Descriptor
	counter   *prometheus.CounterVec
	gauge     *prometheus.GaugeVec
	histogram *prometheus.HistogramVec
}

// Exporter is a telemetry.Emitter that records into a private registry.
type Exporter struct {
	registry    *prometheus.Registry
	instruments map[string]instrument
	dropped     *prometheus.CounterVec
	logger      *zap.Logger
}

// New builds an exporter with one instrument per catalogue descriptor plus the
// standard Go runtime and process collectors. It fails if any descriptor
// cannot be registered, so a partial catalogue is never served.
func New(logger *zap.Logger) (*Exporter, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("failed to register go collector: %w", err)
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("failed to register process collector: %w", err)
	}
	dropped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "forma_telemetry_dropped_total",
		Help: "Telemetry emissions the exporter refused because they did not match the catalogue, by reason.",
	}, []string{"reason"})
	if err := reg.Register(dropped); err != nil {
		return nil, fmt.Errorf("failed to register telemetry drop counter: %w", err)
	}

	exp := &Exporter{registry: reg, instruments: map[string]instrument{}, dropped: dropped, logger: logger}
	for _, d := range telemetry.Catalogue() {
		inst, err := newInstrument(d)
		if err != nil {
			return nil, fmt.Errorf("failed to build instrument for %s: %w", d.Name, err)
		}
		if err := reg.Register(inst.collector()); err != nil {
			return nil, fmt.Errorf("failed to register metric %s: %w", d.Name, err)
		}
		exp.instruments[d.Name] = inst
	}
	return exp, nil
}

func newInstrument(d telemetry.Descriptor) (instrument, error) {
	inst := instrument{desc: d}
	switch d.Kind {
	case telemetry.KindCounter:
		inst.counter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: d.Name, Help: d.Help}, d.Labels)
	case telemetry.KindGauge:
		inst.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: d.Name, Help: d.Help}, d.Labels)
	case telemetry.KindHistogram:
		inst.histogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: d.Name, Help: d.Help, Buckets: latencyBucketsMS,
		}, d.Labels)
	default:
		return instrument{}, fmt.Errorf("descriptor kind %q has no prometheus mapping", string(d.Kind))
	}
	return inst, nil
}

func (i instrument) collector() prometheus.Collector {
	switch {
	case i.counter != nil:
		return i.counter
	case i.gauge != nil:
		return i.gauge
	default:
		return i.histogram
	}
}

// Handler serves the registry in the Prometheus exposition format.
func (e *Exporter) Handler() http.Handler {
	return promhttp.HandlerFor(e.registry, promhttp.HandlerOpts{})
}

// Emit is the telemetry.Emitter. An emission outside the catalogue contract is
// dropped and counted under forma_telemetry_dropped_total rather than served
// under a shape no dashboard expects.
func (e *Exporter) Emit(_ context.Context, name string, labels map[string]string, value any) {
	inst, ok := e.instruments[name]
	if !ok {
		e.drop("unknown_metric", name, fmt.Errorf("metric %q is not in the telemetry catalogue", name))
		return
	}
	v, err := inst.desc.Kind.Coerce(value)
	if err != nil {
		e.drop("value_type", name, err)
		return
	}
	values, err := inst.desc.LabelValues(labels)
	if err != nil {
		e.drop("labels", name, err)
		return
	}
	switch {
	case inst.counter != nil:
		inst.counter.WithLabelValues(values...).Add(v)
	case inst.gauge != nil:
		inst.gauge.WithLabelValues(values...).Set(v)
	default:
		inst.histogram.WithLabelValues(values...).Observe(v)
	}
}

func (e *Exporter) drop(reason, name string, err error) {
	e.dropped.WithLabelValues(reason).Inc()
	e.logger.Warn("telemetry emission dropped by prometheus exporter",
		zap.String("metric", name), zap.String("reason", reason), zap.Error(err))
}
