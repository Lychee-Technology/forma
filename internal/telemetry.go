package internal

import (
	"context"
	"fmt"
	"sync"
)

// telemetry.go
// Lightweight telemetry hook layer used by the federated query engine.
// This file exposes simple emitter functions the rest of the codebase can call.
// The implementation is intentionally minimal: callers may register a real OpenTelemetry
// emitter (or a test stub) via RegisterTelemetryEmitter. By default the emitter is a no-op,
// avoiding any hard dependency on an OTEL SDK in this change set.

type telemetryEmitter func(ctx context.Context, name string, labels map[string]string, value any)

var (
	teleMu   sync.Mutex
	teleImpl telemetryEmitter = func(ctx context.Context, name string, labels map[string]string, value any) {
		// noop by default
	}
)

// RegisterTelemetryEmitter registers a custom emitter function. Callers (e.g. service
// wiring) can provide an OpenTelemetry-backed emitter or a test meter.
func RegisterTelemetryEmitter(fn telemetryEmitter) {
	teleMu.Lock()
	defer teleMu.Unlock()
	if fn == nil {
		teleImpl = func(ctx context.Context, name string, labels map[string]string, value any) {}
		return
	}
	teleImpl = fn
}

// EmitLatency records a latency measure (milliseconds) for a named stage.
// name: "fed_query_latency_histogram" with label {"stage": "<translation|execution|streaming>"}
//
// TOCTOU note: the emitter function is copied under teleMu and then called
// outside the lock. This means a concurrent RegisterTelemetryEmitter call can
// swap the implementation between the read and the invocation, so the emitter
// that is actually called may differ from the one registered at the time of the
// call. This is intentional: holding the lock across the emitter call would
// risk deadlock if the emitter itself performs any operation that tries to
// register a new emitter. Callers must not depend on strict ordering guarantees
// between RegisterTelemetryEmitter and subsequent Emit* calls.
func EmitLatency(ctx context.Context, stage string, ms int64) {
	teleMu.Lock()
	fn := teleImpl
	teleMu.Unlock()
	labels := map[string]string{"stage": stage}
	fn(ctx, "fed_query_latency_histogram", labels, ms)
}

// EmitRowCount records row counts per source.
// name: "fed_query_row_count" with label {"source": "pg"|"s3"|"duckdb"}
func EmitRowCount(ctx context.Context, source string, rows int64) {
	teleMu.Lock()
	fn := teleImpl
	teleMu.Unlock()
	labels := map[string]string{"source": source}
	fn(ctx, "fed_query_row_count", labels, rows)
}

// EmitPushdownEfficiency records pushdown efficiency as a ratio (float64).
// name: "fed_query_pushdown_efficiency" with label {"schema_id": "<id>"}
func EmitPushdownEfficiency(ctx context.Context, schemaID int16, ratio float64) {
	teleMu.Lock()
	fn := teleImpl
	teleMu.Unlock()
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	fn(ctx, "fed_query_pushdown_efficiency", labels, ratio)
}

// EmitCompactionManifestContractViolation records a contract violation event when
// compaction detects SaveManifest succeeded without metadata advancement.
// name: "compaction_manifest_contract_violation_total" with label {"schema_id": "<id>"}
func EmitCompactionManifestContractViolation(ctx context.Context, schemaID int16) {
	teleMu.Lock()
	fn := teleImpl
	teleMu.Unlock()
	labels := map[string]string{"schema_id": fmt.Sprintf("%d", schemaID)}
	fn(ctx, "compaction_manifest_contract_violation_total", labels, int64(1))
}
