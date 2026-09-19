// Package telemetry is the metrics hook the rest of the module emits through.
// Every Emit* helper routes one catalogued metric (catalogue.go) to the
// registered Emitter; the default is a no-op. The shipped entrypoints register
// a backend from their metrics config (#423, docs/telemetry.md): the
// Prometheus exporter in promexport or the CloudWatch EMF exporter in
// emfexport. Both map an emission onto their backend from its Descriptor
// alone, which is why every helper must have one.
package telemetry

import (
	"context"
	"fmt"
	"sync"
)

// Emitter is the callback signature used by telemetry hooks. name is a
// catalogued Descriptor.Name, labels carries exactly that descriptor's label
// keys, and value is the Go type its Kind prescribes (see Kind).
type Emitter func(ctx context.Context, name string, labels map[string]string, value any)

var (
	teleMu   sync.Mutex
	teleImpl Emitter = func(ctx context.Context, name string, labels map[string]string, value any) {
		// noop by default
	}
)

// RegisterTelemetryEmitter registers the process-wide emitter: a backend
// adapter from an entrypoint (bootstrap.InstallMetrics) or a test stub. Passing
// nil resets the emitter back to the default no-op implementation.
func RegisterTelemetryEmitter(fn Emitter) {
	teleMu.Lock()
	defer teleMu.Unlock()
	if fn == nil {
		teleImpl = func(ctx context.Context, name string, labels map[string]string, value any) {}
		return
	}
	teleImpl = fn
}

func currentEmitter() Emitter {
	teleMu.Lock()
	fn := teleImpl
	teleMu.Unlock()
	return fn
}

// EmitLatency records a latency measure (milliseconds) for a named stage.
// name: "fed_query_latency_histogram" with label {"stage": "<translation|execution|streaming>"}
func EmitLatency(ctx context.Context, stage string, ms int64) {
	labels := map[string]string{"stage": stage}
	currentEmitter()(ctx, fedQueryLatency.Name, labels, ms)
}

// EmitRowCount records row counts per source.
// name: "fed_query_row_count" with label {"source": "pg"|"s3"|"duckdb"}
func EmitRowCount(ctx context.Context, source string, rows int64) {
	labels := map[string]string{"source": source}
	currentEmitter()(ctx, fedQueryRowCount.Name, labels, rows)
}

// EmitPushdownEfficiency records pushdown efficiency as a ratio (float64).
// name: "fed_query_pushdown_efficiency" with label {"schema_id": "<id>"}
func EmitPushdownEfficiency(ctx context.Context, schemaID int16, ratio float64) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, fedQueryPushdownEfficiency.Name, labels, ratio)
}

// EmitCompactionManifestContractViolation records a contract violation event when
// compaction detects SaveManifest succeeded without metadata advancement.
// name: "compaction_manifest_contract_violation_total" with label {"schema_id": "<id>"}
func EmitCompactionManifestContractViolation(ctx context.Context, schemaID int16) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, compactionManifestContractViolation.Name, labels, int64(1))
}

// EmitCompactionDirtyRatio records the evaluated dirty ratio for a compaction pass.
// name: "compaction_dirty_ratio" with label {"schema_id": "<id>"}
func EmitCompactionDirtyRatio(ctx context.Context, schemaID int16, ratio float64) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, compactionDirtyRatio.Name, labels, ratio)
}

// EmitCompactionRewritePending records that a rewrite is needed but cannot be applied yet.
// name: "compaction_rewrite_pending_total" with label {"schema_id": "<id>"}
func EmitCompactionRewritePending(ctx context.Context, schemaID int16) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, compactionRewritePending.Name, labels, int64(1))
}

// EmitParquetChecksumMismatch records a parquet object whose bytes no longer
// hash to the checksum its manifest entry carries (#347).
// name: "parquet_checksum_mismatch_total" with label {"schema_id": "<id>"}
func EmitParquetChecksumMismatch(ctx context.Context, schemaID int16) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, parquetChecksumMismatch.Name, labels, int64(1))
}

// EmitCompactionRewriteApplied records a committed dirty-ratio rewrite (#188).
// name: "compaction_rewrite_applied_total" with label {"schema_id": "<id>"}
func EmitCompactionRewriteApplied(ctx context.Context, schemaID int16) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, compactionRewriteApplied.Name, labels, int64(1))
}

// EmitReportOnlyValidationViolation records a write that violated its entity
// JSON schema and was accepted because strict update validation is off (#317).
// kind is "required" (the document lacks a property) or "constraint" (a
// present value is illegal); package internal classifies it.
// name: "entity_report_only_validation_violation_total" with labels
// {"schema_id": "<id>", "schema_name": "<name>", "kind": "required"|"constraint"}
func EmitReportOnlyValidationViolation(ctx context.Context, schemaID int16, schemaName, kind string) {
	labels := map[string]string{
		"schema_id":   fmt.Sprintf("%d", schemaID),
		"schema_name": schemaName,
		"kind":        kind,
	}
	currentEmitter()(ctx, reportOnlyValidationViolation.Name, labels, int64(1))
}
