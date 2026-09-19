//go:build e2e

package production

import (
	"context"
	"fmt"
	"sync"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
)

// metricRecorder is an embedder's MetricEmitter: it keeps every metric the
// manager it was handed to emits. Emissions arrive on the goroutine doing
// the work, so it locks.
type metricRecorder struct {
	mu     sync.Mutex
	events []forma.Metric
}

func (r *metricRecorder) EmitMetric(_ context.Context, m forma.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, m)
}

func (r *metricRecorder) byName(name string) []forma.Metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []forma.Metric
	for _, m := range r.events {
		if m.Name == name {
			out = append(out, m)
		}
	}
	return out
}

// TestFactoryWiring_MetricEmitter is the composition-root proof for #423:
// the emitter an embedder sets on Config.Metrics.Emitter is the one the
// manager's write path and the federated engine emit to, and nothing else in
// the process — a second manager built without one — reaches it.
//
// Everything goes through the PUBLIC surface: a forma.Config, the factory,
// forma.EntityManager.Update and .Query. The factory's two wiring lines
// (config → internal.NewEntityManager, federated.WithMetricEmitter) are
// deliberately not unit-covered; if either were dropped, only this test fails.
func TestFactoryWiring_MetricEmitter(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	rec := &metricRecorder{}
	loud := newFactoryManagerWithEmitter(ctx, t, env, rec)
	silent := newFactoryManagerWithEmitter(ctx, t, env, nil)

	created, err := loud.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: wide.Name},
		Type:             forma.OperationCreate,
		Data:             map[string]any{"title": "metrics", "note": "text"},
	})
	if err != nil {
		t.Fatalf("create through the factory-built manager: %v", err)
	}
	if len(rec.events) != 0 {
		t.Fatalf("a valid create emitted %d metrics, want none", len(rec.events))
	}

	// e2e_wide declares "note": {"type": "string"}. An integer violates the
	// schema, yet the text transformer stringifies it, so with strict update
	// validation off (the default) the write is accepted and reported —
	// exactly the #317 aggregate the emitter must receive.
	violate := func(manager forma.EntityManager, label string) {
		t.Helper()
		if _, err := manager.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: wide.Name, RowID: created.RowID},
			Type:             forma.OperationUpdate,
			Updates:          map[string]any{"note": 123},
		}); err != nil {
			t.Fatalf("%s: report-only update must be accepted: %v", label, err)
		}
	}

	violate(silent, "manager without an emitter")
	if len(rec.events) != 0 {
		t.Fatalf("a manager built without an emitter reached another manager's emitter: %+v", rec.events)
	}

	violate(loud, "manager with an emitter")
	got := rec.byName("entity_report_only_validation_violation_total")
	if len(got) != 1 {
		t.Fatalf("one accepted violation must emit exactly one increment, got %+v", rec.events)
	}
	want := forma.Metric{
		Name:   "entity_report_only_validation_violation_total",
		Kind:   forma.MetricKindCounter,
		Unit:   forma.MetricUnitCount,
		Labels: map[string]string{"schema_id": fmt.Sprintf("%d", wide.ID), "schema_name": wide.Name, "kind": "constraint"},
		Value:  1,
	}
	if got[0].Kind != want.Kind || got[0].Unit != want.Unit || got[0].Value != want.Value ||
		got[0].Labels["schema_id"] != want.Labels["schema_id"] ||
		got[0].Labels["schema_name"] != want.Labels["schema_name"] ||
		got[0].Labels["kind"] != want.Labels["kind"] || len(got[0].Labels) != 3 {
		t.Fatalf("report-only violation metric = %+v, want %+v", got[0], want)
	}

	// The federated engine the factory built shares the same emitter: one
	// federated query lands its stage latencies and source row counts there.
	// Flush first so the manifest lists a real object for the cold tier.
	mustFlush(ctx, t, env)
	before := len(rec.events)
	if _, err := factoryQuery(ctx, loud, wide, factoryQueryOpts{}); err != nil {
		t.Fatalf("federated query through the factory-built manager: %v", err)
	}
	if len(rec.byName("fed_query_latency_histogram")) == 0 {
		t.Fatalf("the factory-built engine emitted no fed_query_latency_histogram; events since the query: %+v", rec.events[before:])
	}
	if len(rec.byName("fed_query_row_count")) == 0 {
		t.Fatalf("the factory-built engine emitted no fed_query_row_count; events since the query: %+v", rec.events[before:])
	}
	for _, m := range rec.events {
		desc, ok := forma.LookupMetric(m.Name)
		if !ok {
			t.Fatalf("emitted metric %q is not in forma.MetricCatalogue", m.Name)
		}
		if desc.Kind != m.Kind || desc.Unit != m.Unit || !desc.LabelKeysMatch(m.Labels) {
			t.Fatalf("emitted metric %+v is off its catalogue contract %+v", m, desc)
		}
	}
}

// newFactoryManagerWithEmitter builds a manifest-on manager the way a server
// does, with the embedder's emitter on the config. nil is the unconfigured
// default.
func newFactoryManagerWithEmitter(ctx context.Context, t *testing.T, env *Env, emitter forma.MetricEmitter) forma.EntityManager {
	t.Helper()
	cfg := forma.DefaultConfig(env.Registry)
	cfg.Entity.SchemaDirectory = FixtureSchemasDir()
	cfg.Database.TableNames = forma.TableNames{
		SchemaRegistry: env.Tables.SchemaRegistry,
		EntityMain:     env.Tables.EntityMain,
		EAVData:        env.Tables.EAVData,
		ChangeLog:      env.Tables.ChangeLog,
	}
	duck := env.DuckCfg
	duck.MaxConnections = 1 // see newFactoryManager (#245)
	duck.S3Bucket = env.Cluster.Bucket
	duck.S3DataPrefix = env.S3Prefix
	duck.ManifestPrefix = env.CDC.ManifestPrefix
	duck.ManifestTemplate = env.CDC.ManifestTemplate
	cfg.DuckDB = duck
	cfg.Metrics.Emitter = emitter

	manager, err := factory.NewEntityManagerWithConfigContext(ctx, cfg, env.Pool)
	if err != nil {
		t.Fatalf("build entity manager via factory (emitter=%t): %v", emitter != nil, err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Errorf("close factory-built manager (emitter=%t): %v", emitter != nil, err)
		}
	})
	return manager
}
