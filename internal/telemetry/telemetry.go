// Package telemetry is the per-instance metrics sink the rest of the module
// emits through (#423). A Sink wraps the forma.MetricEmitter an embedder set on
// Config.Metrics.Emitter; every Emit* helper builds one catalogued
// forma.Metric and hands it over. There is no process-global emitter: two
// Forma instances in one process each own their Sink, and a component built
// without one (a nil *Sink) emits nothing.
//
// The sink is the safety boundary between Forma's business logic and the
// embedder's telemetry code. An emission that does not match its
// forma.MetricCatalogue descriptor — a Forma bug — is dropped and logged, never
// panicked on, and a panic inside the embedder's EmitMetric is recovered, so
// telemetry can never fail the write, query or compaction pass that emitted.
package telemetry

import (
	"context"
	"fmt"
	"sort"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// Sink emits catalogued metrics to one forma.MetricEmitter. The zero value
// and a nil *Sink are both no-ops.
type Sink struct {
	emitter forma.MetricEmitter
}

// NewSink wraps an emitter. A nil emitter yields a sink that emits nothing;
// callers need not special-case the unconfigured default.
func NewSink(emitter forma.MetricEmitter) *Sink {
	if emitter == nil {
		return nil
	}
	return &Sink{emitter: emitter}
}

// emit resolves name against the catalogue, checks the label keys, and hands
// the metric to the emitter. Both checks guard a Forma programming error, so
// they log at Error; a panic from the emitter is the embedder's bug and logs
// at Warn. Neither propagates.
func (s *Sink) emit(ctx context.Context, name string, labels map[string]string, value float64) {
	if s == nil || s.emitter == nil {
		return
	}
	desc, ok := forma.LookupMetric(name)
	if !ok {
		zap.S().Errorw("telemetry: dropping emission of a metric that is not in forma.MetricCatalogue", "name", name)
		return
	}
	if !desc.LabelKeysMatch(labels) {
		zap.S().Errorw("telemetry: dropping emission whose label keys do not match the catalogue",
			"name", name, "expected", desc.Labels, "got", labelKeys(labels))
		return
	}
	defer func() {
		if r := recover(); r != nil {
			zap.S().Warnw("telemetry: metric emitter panicked; the emission is lost and the operation continues",
				"name", name, "panic", fmt.Sprint(r))
		}
	}()
	s.emitter.EmitMetric(ctx, forma.Metric{
		Name:   desc.Name,
		Kind:   desc.Kind,
		Unit:   desc.Unit,
		Labels: labels,
		Value:  value,
	})
}

// labelKeys lists the keys of a rejected label set in sorted order so the
// drop log reads the same on every run.
func labelKeys(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// EmitLatency records a latency measure (milliseconds) for a named stage.
// name: "fed_query_latency_histogram" with label {"stage": "<translation|execution|streaming>"}
func (s *Sink) EmitLatency(ctx context.Context, stage string, ms int64) {
	s.emit(ctx, "fed_query_latency_histogram", map[string]string{"stage": stage}, float64(ms))
}

// EmitRowCount records row counts per source.
// name: "fed_query_row_count" with label {"source": "pg"|"duckdb"}
func (s *Sink) EmitRowCount(ctx context.Context, source string, rows int64) {
	s.emit(ctx, "fed_query_row_count", map[string]string{"source": source}, float64(rows))
}

// EmitPushdownEfficiency records pushdown efficiency as a ratio.
// name: "fed_query_pushdown_efficiency" with label {"schema_id": "<id>"}
func (s *Sink) EmitPushdownEfficiency(ctx context.Context, schemaID int16, ratio float64) {
	s.emit(ctx, "fed_query_pushdown_efficiency", schemaLabels(schemaID), ratio)
}

// EmitCompactionManifestContractViolation records a contract violation event when
// compaction detects SaveManifest succeeded without metadata advancement.
// name: "compaction_manifest_contract_violation_total" with label {"schema_id": "<id>"}
func (s *Sink) EmitCompactionManifestContractViolation(ctx context.Context, schemaID int16) {
	s.emit(ctx, "compaction_manifest_contract_violation_total", schemaLabels(schemaID), 1)
}

// EmitCompactionDirtyRatio records the evaluated dirty ratio for a compaction pass.
// name: "compaction_dirty_ratio" with label {"schema_id": "<id>"}
func (s *Sink) EmitCompactionDirtyRatio(ctx context.Context, schemaID int16, ratio float64) {
	s.emit(ctx, "compaction_dirty_ratio", schemaLabels(schemaID), ratio)
}

// EmitCompactionRewritePending records that a rewrite is needed but cannot be applied yet.
// name: "compaction_rewrite_pending_total" with label {"schema_id": "<id>"}
func (s *Sink) EmitCompactionRewritePending(ctx context.Context, schemaID int16) {
	s.emit(ctx, "compaction_rewrite_pending_total", schemaLabels(schemaID), 1)
}

// EmitParquetChecksumMismatch records a parquet object whose bytes no longer
// hash to the checksum its manifest entry carries (#347).
// name: "parquet_checksum_mismatch_total" with label {"schema_id": "<id>"}
func (s *Sink) EmitParquetChecksumMismatch(ctx context.Context, schemaID int16) {
	s.emit(ctx, "parquet_checksum_mismatch_total", schemaLabels(schemaID), 1)
}

// EmitCompactionRewriteApplied records a committed dirty-ratio rewrite (#188).
// name: "compaction_rewrite_applied_total" with label {"schema_id": "<id>"}
func (s *Sink) EmitCompactionRewriteApplied(ctx context.Context, schemaID int16) {
	s.emit(ctx, "compaction_rewrite_applied_total", schemaLabels(schemaID), 1)
}

// EmitReportOnlyValidationViolation records a write that violated its entity
// JSON schema and was accepted because strict update validation is off (#317).
// kind is "required" (the document lacks a property) or "constraint" (a
// present value is illegal); package internal classifies it.
// name: "entity_report_only_validation_violation_total" with labels
// {"schema_id": "<id>", "schema_name": "<name>", "kind": "required"|"constraint"}
func (s *Sink) EmitReportOnlyValidationViolation(ctx context.Context, schemaID int16, schemaName, kind string) {
	s.emit(ctx, "entity_report_only_validation_violation_total", map[string]string{
		"schema_id":   fmt.Sprintf("%d", schemaID),
		"schema_name": schemaName,
		"kind":        kind,
	}, 1)
}

func schemaLabels(schemaID int16) map[string]string {
	return map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
}
