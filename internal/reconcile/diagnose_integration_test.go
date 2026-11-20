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

// TestDiagnosisIsReadOnly: a pass without GC raises no horizon, retires no
// generation and deletes nothing, and reports the plan a collection would
// run, which the next collection then runs exactly.
func TestDiagnosisIsReadOnly(t *testing.T) {
	f := newFixture(t)
	g3 := collectable(t, f)
	rec := f.record(t)
	before := f.store.keys()
	var plan publication.Plan
	for range 2 {
		rep := f.pass(t, testOptions(false))
		require.NoError(t, rep.Err)
		require.Nil(t, rep.Collected, "a diagnosis collects nothing")
		require.Empty(t, rep.Findings)
		require.Equal(t, rec.Horizon, rep.Horizon, "the plan is made against the record's horizon")
		require.Equal(t, []int64{1}, rep.Plan.Forget, "generation 2's row stays as the history's floor")
		require.Len(t, rep.Plan.Manifests, 2)
		plan = rep.Plan
	}
	require.Equal(t, rec, f.record(t), "the record is unchanged: no horizon was raised")
	require.Equal(t, before, f.store.keys(), "nothing was deleted")

	g4 := f.publish(t, f.stageFlush(t, g3, 1))
	rep := f.pass(t, testOptions(false))
	require.Equal(t, 2, rep.Retained, "the superseded generation is not retired by a diagnosis")
	retained, err := publication.Retained(context.Background(), publication.DB(f.db), f.tables, testSchema, 0)
	require.NoError(t, err)
	require.Zero(t, retained[0].RetiredAt)

	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, plan.Objects, rep.Collected.Objects, "the collection deletes the objects the diagnosis planned")
	require.Equal(t, plan.Manifests, rep.Collected.Manifests)
	require.Equal(t, plan.Forget, rep.Collected.Forgotten)
	require.Subset(t, f.store.keys(), f.keysOf(g4))
}

// TestDiagnosisReportsLossWithItsIdentityAndAction: an object the current
// generation lists and S3 lost, and one only a retained superseded
// generation lists, are each reported with schema, epoch, generation and the
// safe action, and neither stops the collection of what no reader scans.
func TestDiagnosisReportsLossWithItsIdentityAndAction(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))
	g3 := f.publishCompact(t, g2)
	g4 := f.publish(t, f.stageFlush(t, g3, 1))
	supersededOnly := f.stagedKeys(staged{manifest: g2.manifest}, g1)[1] // g2's delta
	currentOnly := f.stagedKeys(staged{manifest: g4.manifest}, g3)[1]    // g4's delta
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
	require.Empty(t, rep.Refused, "a lost object does not stop the collection of what no reader scans")

	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	text := out.String()
	require.Contains(t, text, "schema 7: needs attention (PUBLISHED, epoch 1, generation 4)")
	require.Contains(t, text, fmt.Sprintf("lost_object: epoch 1 generation 4 (current), key %s", currentOnly))
	require.Contains(t, text, "action: re-initialize the schema with cdc-init")
}

// TestVerifyChecksumsReportsAnObjectWithOtherBytes: the checksum pass hashes
// every object a retained generation lists, and reports one whose bytes are
// not those its manifest lists; without it nothing is hashed.
func TestVerifyChecksumsReportsAnObjectWithOtherBytes(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 2)
	key := g1.manifest.Files[1].Key
	f.corrupt(t, key)

	require.Empty(t, f.pass(t, testOptions(false)).Findings, "nothing is hashed unless asked")
	opts := testOptions(false)
	opts.VerifyChecksums = true
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{CorruptObject}, kinds(rep.Findings))
	finding := rep.Findings[0]
	require.Equal(t, key, finding.Key)
	require.True(t, finding.Current)
	require.Contains(t, finding.Detail, g1.manifest.Files[1].SHA256)
	require.Equal(t, actionCorrupt, finding.Action)
}

// TestSchemasEnumeratesRegisteredAndRecordedSchemas: a registered schema
// without a record and a dropped schema with only a record are both visited.
func TestSchemasEnumeratesRegisteredAndRecordedSchemas(t *testing.T) {
	f := newFixture(t)
	registry := fmt.Sprintf("gc_registry_%d", time.Now().UnixNano())
	_, err := f.db.Exec("CREATE TABLE " + registry + " (schema_id SMALLINT PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = f.db.Exec("DROP TABLE IF EXISTS " + registry) })
	_, err = f.db.Exec("INSERT INTO " + registry + " VALUES (3), (7)")
	require.NoError(t, err)
	f.initialized(t) // schema 7
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
