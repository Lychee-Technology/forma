package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// metricRecorder is the test emitter: it keeps every metric a manager built
// with it emits.
type metricRecorder struct{ events []forma.Metric }

func (r *metricRecorder) EmitMetric(_ context.Context, m forma.Metric) {
	r.events = append(r.events, m)
}

// TestReportOnlyUpdateEmitsAggregateCounter pins #317's telemetry half: every
// accepted violation increments entity_report_only_validation_violation_total
// with the schema and kind the flip decision is made over. The emitter is the
// one the embedder set on Config.Metrics.Emitter (#423); the default has none,
// so this is behavior-neutral there and the milestone log line below is its
// default-visible counterpart.
func TestReportOnlyUpdateEmitsAggregateCounter(t *testing.T) {
	rec := &metricRecorder{}
	manager, _, _ := newValidationHarnessWithEmitter(t, false, rec)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)
	require.Empty(t, rec.events, "a valid create emits nothing")

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"name": "banana"}))
	require.NoError(t, err)

	require.Len(t, rec.events, 1, "one accepted violation must emit exactly one increment")
	require.Equal(t, forma.Metric{
		Name: "entity_report_only_validation_violation_total",
		Kind: forma.MetricKindCounter,
		Unit: forma.MetricUnitCount,
		Labels: map[string]string{
			"schema_id":   "100",
			"schema_name": "test",
			"kind":        "constraint",
		},
		Value: 1,
	}, rec.events[0])
}

// TestReportOnlyDefaultEmitsNothing pins the unconfigured default (#423): a
// manager built without Config.Metrics.Emitter accepts the same violation and
// no telemetry code runs — there is no global to have been set by anyone else.
func TestReportOnlyDefaultEmitsNothing(t *testing.T) {
	manager, _ := newValidatingManager(t, false)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"name": "banana"}))
	require.NoError(t, err)
}

// TestMetricEmitterIsPerInstance is #423's invariant: two managers in one
// process each report to their own emitter. Instance B's violation never
// reaches instance A's emitter, and a manager without an emitter stays silent
// while another one in the same process is configured.
func TestMetricEmitterIsPerInstance(t *testing.T) {
	recA, recB := &metricRecorder{}, &metricRecorder{}
	managerA, _, _ := newValidationHarnessWithEmitter(t, false, recA)
	managerB, _, _ := newValidationHarnessWithEmitter(t, false, recB)
	managerNone, _ := newValidatingManager(t, false)

	violate := func(manager forma.EntityManager) {
		created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
		require.NoError(t, err)
		_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"name": "banana"}))
		require.NoError(t, err)
	}

	violate(managerB)
	require.Empty(t, recA.events, "instance A must not see instance B's emission")
	require.Len(t, recB.events, 1)

	violate(managerA)
	violate(managerNone)
	require.Len(t, recA.events, 1)
	require.Len(t, recB.events, 1, "instance B must not see instance A's emission")
}

// TestPanickingMetricEmitterDoesNotFailTheWrite pins the safety boundary
// (#423): the embedder's telemetry code throwing is contained inside the sink,
// so the report-only write it was reporting still succeeds.
func TestPanickingMetricEmitterDoesNotFailTheWrite(t *testing.T) {
	boom := forma.MetricEmitterFunc(func(context.Context, forma.Metric) { panic("emitter bug") })
	manager, repo, _ := newValidationHarnessWithEmitter(t, false, boom)
	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	updated, err := manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"name": "banana"}))
	require.NoError(t, err, "a telemetry failure must never fail the operation that emitted")
	require.NotNil(t, updated)
	stored, err := repo.GetPersistentRecord(context.Background(), model.StorageTables{}, 100, created.RowID)
	require.NoError(t, err)
	require.NotNil(t, stored, "the accepted write must still reach storage")
}
