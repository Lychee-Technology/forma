package reconcile

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/publication"
)

// TestDiagnosisIsReadOnly verifies that a run without --gc raises no horizon,
// retires no generation, deletes nothing, and reports the exact plan that the
// subsequent --gc pass executes.
func TestDiagnosisIsReadOnly(t *testing.T) {
	f := newFixture(t)
	g3 := collectable(t, f)
	rec := f.record(t)
	before := f.store.keys()
	var plan publication.Plan
	for range 2 {
		rep := f.pass(t, testOptions(false))
		require.NoError(t, rep.Err)
		require.Nil(t, rep.Collected)
		require.Empty(t, rep.Findings)
		require.Equal(t, rec.Horizon, rep.Horizon)
		require.Equal(t, []int64{1, 2}, rep.Plan.Forget)
		require.Len(t, rep.Plan.Manifests, 2)
		plan = rep.Plan
	}
	require.Equal(t, rec, f.record(t))
	require.Equal(t, before, f.store.keys())

	g4 := f.publish(t, f.stageFlush(t, g3, 1))
	rep := f.pass(t, testOptions(false))
	require.Equal(t, 2, rep.Retained)
	retained, err := publication.Retained(context.Background(), publication.DB(f.db), f.tables, testSchema, 0)
	require.NoError(t, err)
	require.Zero(t, retained[0].RetiredAt)

	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, plan.Objects, rep.Collected.Objects)
	require.Equal(t, plan.Manifests, rep.Collected.Manifests)
	require.Equal(t, plan.Forget, rep.Collected.Forgotten)
	require.Subset(t, f.store.keys(), f.keysOf(g4))
}

// TestDiagnosisReportsLossWithItsIdentityAndAction verifies that missing
// objects in both current and retained superseded generations are reported with
// schema, epoch, generation and safe operator action without blocking GC.
func TestDiagnosisReportsLossWithItsIdentityAndAction(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))
	g3 := f.publishCompact(t, g2)
	g4 := f.publish(t, f.stageFlush(t, g3, 1))
	supersededOnly := f.stagedKeys(staged{manifest: g2.manifest}, g1)[1]
	currentOnly := f.stagedKeys(staged{manifest: g4.manifest}, g3)[1]
	f.store.remove(supersededOnly)
	f.store.remove(currentOnly)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{LostObject, LostObject}, kinds(rep.Findings))
	byKey := map[string]Finding{}
	for _, finding := range rep.Findings {
		byKey[finding.Key] = finding
	}
	cur, old := byKey[currentOnly], byKey[supersededOnly]
	require.Equal(t, [3]any{int64(1), int64(4), true}, [3]any{cur.Epoch, cur.Generation, cur.Current})
	require.Equal(t, actionLostCurrent, cur.Action)
	require.Equal(t, [3]any{int64(1), int64(2), false}, [3]any{old.Epoch, old.Generation, old.Current})
	require.Equal(t, actionLostSuperseded, old.Action)
	require.Empty(t, rep.Refused)

	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	text := out.String()
	require.Contains(t, text, "schema 7: needs attention (PUBLISHED, epoch 1, generation 4)")
	require.Contains(t, text, fmt.Sprintf("lost_object: epoch 1 generation 4 (current), key %s", currentOnly))
	require.Contains(t, text, "action: re-initialize the schema with cdc-init")
}

// TestVerifyChecksumsReportsAnObjectWithOtherBytes verifies that checksum
// scrubbing detects silent corruption, deduplicates shared keys across
// generations, and is opt-in via VerifyChecksums.
func TestVerifyChecksumsReportsAnObjectWithOtherBytes(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 2)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))
	key := g1.manifest.Files[1].Key
	f.corrupt(t, key)

	require.Empty(t, f.pass(t, testOptions(false)).Findings, "nothing is hashed unless asked")

	hashCalls := map[string]int{}
	f.store.beforeHash = func(k string) { hashCalls[k]++ }
	opts := testOptions(false)
	opts.VerifyChecksums = true
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, 1, hashCalls[key], "shared key across g1 and g2 is hashed once")
	require.Equal(t, []FindingKind{CorruptObject, CorruptObject}, kinds(rep.Findings), "both referencing generations are reported")
	require.Equal(t, g2.rec.Generation, rep.Findings[1].Generation)
	require.True(t, rep.Findings[1].Current)
	require.Contains(t, rep.Findings[1].Detail, g1.manifest.Files[1].SHA256)
	require.Equal(t, actionCorrupt, rep.Findings[1].Action)
}

// TestSchemasEnumeratesRegisteredAndRecordedSchemas verifies enumeration
// across schema_registry, _publication, _publication_generation, and
// _publication_epoch.
func TestSchemasEnumeratesRegisteredAndRecordedSchemas(t *testing.T) {
	f := newFixture(t)
	registry := fmt.Sprintf("gc_registry_%d", time.Now().UnixNano())
	_, err := f.db.Exec("CREATE TABLE " + registry + " (schema_id SMALLINT PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = f.db.Exec("DROP TABLE IF EXISTS " + registry) })
	_, err = f.db.Exec("INSERT INTO " + registry + " VALUES (3), (7)")
	require.NoError(t, err)
	f.initialized(t)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Initialize(context.Background(), tx, f.tables, 9)
		return err
	}))

	s := &Schemas{DB: f.db, Registry: registry, Tables: f.tables}
	ids, err := s.SchemaIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int16{3, 7, 9}, ids)
	s.Only = 9
	ids, err = s.SchemaIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int16{9}, ids)
	s.Only = 4
	_, err = s.SchemaIDs(context.Background())
	require.ErrorContains(t, err, "schema 4 is neither registered")
}
