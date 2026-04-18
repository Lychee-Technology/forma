package telemetry

import (
	"context"
	"fmt"
	"sync"
)

// Emitter is the callback signature used by telemetry hooks.
type Emitter func(ctx context.Context, name string, labels map[string]string, value any)

var (
	teleMu   sync.Mutex
	teleImpl Emitter = func(ctx context.Context, name string, labels map[string]string, value any) {
		// noop by default
	}
)

// RegisterTelemetryEmitter registers a custom emitter function. Callers can provide
// an OpenTelemetry-backed emitter or a test stub. Passing nil resets the emitter
// back to the default no-op implementation.
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
	currentEmitter()(ctx, "fed_query_latency_histogram", labels, ms)
}

// EmitRowCount records row counts per source.
// name: "fed_query_row_count" with label {"source": "pg"|"s3"|"duckdb"}
func EmitRowCount(ctx context.Context, source string, rows int64) {
	labels := map[string]string{"source": source}
	currentEmitter()(ctx, "fed_query_row_count", labels, rows)
}

// EmitPushdownEfficiency records pushdown efficiency as a ratio (float64).
// name: "fed_query_pushdown_efficiency" with label {"schema_id": "<id>"}
func EmitPushdownEfficiency(ctx context.Context, schemaID int16, ratio float64) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, "fed_query_pushdown_efficiency", labels, ratio)
}

// EmitCompactionManifestContractViolation records a contract violation event when
// compaction detects SaveManifest succeeded without metadata advancement.
// name: "compaction_manifest_contract_violation_total" with label {"schema_id": "<id>"}
func EmitCompactionManifestContractViolation(ctx context.Context, schemaID int16) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, "compaction_manifest_contract_violation_total", labels, int64(1))
}

// EmitCompactionDirtyRatio records the evaluated dirty ratio for a compaction pass.
// name: "compaction_dirty_ratio" with label {"schema_id": "<id>"}
func EmitCompactionDirtyRatio(ctx context.Context, schemaID int16, ratio float64) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, "compaction_dirty_ratio", labels, ratio)
}

// EmitCompactionRewritePending records that a rewrite is needed but cannot be applied yet.
// name: "compaction_rewrite_pending_total" with label {"schema_id": "<id>"}
func EmitCompactionRewritePending(ctx context.Context, schemaID int16) {
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	currentEmitter()(ctx, "compaction_rewrite_pending_total", labels, int64(1))
}
