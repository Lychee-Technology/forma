package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of #696. An update that gives an attribute's single
// value an object or array, {} and [] included, used to pass report-only
// validation, and what it stored depended on the binding: ukaccount's bound
// tenant kept its value, an EAV attribute was removed, ["a"] stored an
// array element under a scalar, and ["b@x"] at an EAV key component was the
// key planner's redacted 500. Every update surface now refuses each shape
// before it writes, in report-only and strict modes alike, and the row, its
// key entries and its change_log stay exactly as they were.

// shapeRefusedValues are the values a single value's position refuses.
func shapeRefusedValues() map[string]any {
	return map[string]any{
		"empty object": map[string]any{}, "object": map[string]any{"x": "y"},
		"empty array": []any{}, "singleton array": []any{"b@x"}, "array": []any{"b@x", "c@x"},
		// No element reaches the conversion, so the array is judged as a whole
		// (#696 review R1-F6): before, the write met the null after the row
		// lock and aborted a Tx.
		"array of nulls": []any{nil},
	}
}

// shapeEnv is ukaccount's keyed environment with a manager and a transactor
// in one validation mode.
type shapeEnv struct {
	*txEnv
	em  forma.EntityManager
	txr forma.Transactor
	// other is a row an atomic batch writes beside the refused operation.
	other uuid.UUID
}

func newShapeEnv(t *testing.T, env *txEnv, strict bool) shapeEnv {
	t.Helper()
	catalog := env.ukCatalog(t)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(env.registry), ukSchemaDir)
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable, EntityMain: env.tables.EntityMain, EAVData: env.tables.EAVData, ChangeLog: env.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: ukSchemaDir, ValidateUpdatesStrict: strict},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	repo := NewDBPersistentRecordRepository(env.postgresPool, env.metadata, WithUniqueKeyCatalog(catalog))
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry), repo, nil, env.registry, config, validator,
		WithUniqueKeys(catalog))
	txr, err := NewTransactor(em)
	require.NoError(t, err)
	other, err := em.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("other", fmt.Sprintf("other-%v@x", strict), "")))
	require.NoError(t, err)
	return shapeEnv{txEnv: env, em: em, txr: txr, other: other.RowID}
}

// shapeSurface runs one update of the row whose note is note, and answers
// the refusal. The Tx surfaces read the row after the refusal, which an
// aborted transaction would refuse, and commit.
type shapeSurface struct {
	name string
	// keyed is false for a surface that refuses any write to a unique key
	// component before it judges values: Claim never writes one.
	keyed  bool
	update func(t *testing.T, e shapeEnv, rowID uuid.UUID, note, handle string, updates map[string]any) error
}

func (e shapeEnv) inTxThenRead(t *testing.T, rowID uuid.UUID, write func(ctx context.Context, tx forma.Tx) error) error {
	t.Helper()
	var refusal error
	require.NoError(t, e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		refusal = write(ctx, tx)
		_, err := tx.Get(ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &rowID})
		require.NoError(t, err, "the refusal must leave the Tx usable")
		return nil
	}), "the Tx commits after the refusal")
	return refusal
}

var shapeSurfaces = []shapeSurface{
	{"Update", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, _, _ string, updates map[string]any) error {
		_, err := e.em.Update(e.ctx, txUpdateOp(ukAccount, rowID, updates))
		return err
	}},
	{"atomic BatchUpdate", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, _, _ string, updates map[string]any) error {
		other := e.snapshot(t, ukAccount, e.other)
		_, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
			*txUpdateOp(ukAccount, e.other, map[string]any{"note": "changed"}), *txUpdateOp(ukAccount, rowID, updates),
		}})
		assert.Equal(t, other, e.snapshot(t, ukAccount, e.other), "the atomic batch writes neither operation")
		return err
	}},
	{"best-effort BatchUpdate", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, _, _ string, updates map[string]any) error {
		result, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{*txUpdateOp(ukAccount, rowID, updates)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1)
		return bestEffortFailure{failure: result.Failed[0]}
	}},
	{"Tx.Update", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, _, _ string, updates map[string]any) error {
		return e.inTxThenRead(t, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Update(ctx, txUpdateOp(ukAccount, rowID, updates))
			return err
		})
	}},
	{"Tx.UpdateWhere", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, note, _ string, updates map[string]any) error {
		return e.inTxThenRead(t, rowID, func(ctx context.Context, tx forma.Tx) error {
			req := ukHandleWhere(note)
			req.Updates, req.MaxAffected = updates, 10
			_, err := tx.UpdateWhere(ctx, req)
			return err
		})
	}},
	{"Tx.Claim", false, func(t *testing.T, e shapeEnv, rowID uuid.UUID, note, _ string, updates map[string]any) error {
		return e.inTxThenRead(t, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount, Condition: ukHandleWhere(note).Condition, Limit: 1, Updates: updates})
			return err
		})
	}},
	{"Tx.Upsert on the row ID", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, _, _ string, updates map[string]any) error {
		return e.inTxThenRead(t, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: *ukCreateOp(ukAccount, rowID, ukAccountData("x", "x@x", "")),
				Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, Updates: updates})
			return err
		})
	}},
	{"Tx.Upsert on a unique key", true, func(t *testing.T, e shapeEnv, rowID uuid.UUID, _, handle string, updates map[string]any) error {
		return e.inTxThenRead(t, rowID, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: *ukCreateOp(ukAccount, uuid.Nil, ukAccountData("x", "x@x", handle)),
				Target: forma.ConflictTarget{Key: "handle"}, Action: forma.UpsertUpdate, Updates: updates})
			return err
		})
	}},
}

// requireShapeRefusal asserts err refuses attr's shape as caller input with
// its published message. An atomic batch prefixes it with the operation.
func requireShapeRefusal(t *testing.T, err error, attr string, msgAndArgs ...any) {
	t.Helper()
	want := fmt.Sprintf("invalid value for attribute '%s'", attr)
	if failure, ok := err.(bestEffortFailure); ok {
		assert.Equal(t, "UPDATE_FAILED", failure.failure.Code, msgAndArgs...)
		assert.Contains(t, failure.failure.Error, want, msgAndArgs...)
		assert.Contains(t, failure.failure.Error, "cannot be stored where a single value belongs", msgAndArgs...)
		return
	}
	require.ErrorIs(t, err, forma.ErrInvalidInput, msgAndArgs...)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	assert.Contains(t, message, want, msgAndArgs...)
	assert.Contains(t, message, "takes a single value, got", msgAndArgs...)
	assert.Contains(t, message, "cannot be stored where a single value belongs", msgAndArgs...)
}

// TestUpdateRefusesAContainerAtASingleValueOnEverySurface is #696's
// acceptance on PostgreSQL: bound tenant, EAV key component email and EAV
// note, each sent every container shape on every update surface in both
// validation modes, keep the row, its key entries and its change_log
// exactly as they were, and a Tx that saw the refusal commits.
func TestUpdateRefusesAContainerAtASingleValueOnEverySurface(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	seq := 0
	for _, strict := range []bool{false, true} {
		e := newShapeEnv(t, env, strict)
		for _, attr := range []string{"tenant", "email", "note"} {
			for valueName, value := range shapeRefusedValues() {
				seq++
				note, handle := fmt.Sprintf("shape-%d", seq), fmt.Sprintf("h-%d", seq)
				data := ukAccountData(fmt.Sprintf("tenant-%d", seq), fmt.Sprintf("e%d@x", seq), handle)
				data["note"] = note
				created, err := e.em.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
				require.NoError(t, err)
				before, keys := e.snapshot(t, ukAccount, created.RowID), e.keyEntriesOfRow(t, created.RowID)

				for _, surface := range shapeSurfaces {
					if !surface.keyed && attr != "note" {
						continue
					}
					label := fmt.Sprintf("strict=%v %s %s %s", strict, surface.name, attr, valueName)
					err := surface.update(t, e, created.RowID, note, handle, map[string]any{attr: value})
					requireShapeRefusal(t, err, attr, label)
					assert.Equal(t, before, e.snapshot(t, ukAccount, created.RowID), "%s: entity_main, eav_data and change_log as they were", label)
					assert.ElementsMatch(t, keys, e.keyEntriesOfRow(t, created.RowID), "%s: key entries as they were", label)
				}
			}
		}
	}
	env.assertKeyTableMatchesRows(t)
}

// TestUpdateWithAContainerAtASingleValueAndNoRowToWriteSucceeds: an
// UpdateWhere or a Claim that takes no row, and an Upsert that creates its
// row, never write Updates, so they succeed as before #696 (#772's no-write
// branches).
func TestUpdateWithAContainerAtASingleValueAndNoRowToWriteSucceeds(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	e := newShapeEnv(t, env, false)
	// Claim never writes a unique key component, so only note is sent.
	for i, value := range []any{[]any{"a"}, []any{nil}} {
		updateWithNoRowToWrite(t, e, map[string]any{"note": value}, fmt.Sprintf("fresh-%d@x", i))
	}
	env.assertKeyTableMatchesRows(t)
}

// updateWithNoRowToWrite runs each no-write branch with updates; the
// Upsert creates a row with email.
func updateWithNoRowToWrite(t *testing.T, e shapeEnv, updates map[string]any, email string) {
	t.Helper()
	require.NoError(t, e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		req := ukHandleWhere("nobody")
		req.Updates, req.MaxAffected = updates, 10
		updated, err := tx.UpdateWhere(ctx, req)
		require.NoError(t, err)
		assert.Empty(t, updated.RowIDs)

		claimed, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount, Condition: ukHandleWhere("nobody").Condition, Limit: 1, Updates: updates})
		require.NoError(t, err)
		assert.Empty(t, claimed.Records)

		upserted, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: *ukCreateOp(ukAccount, uuid.Must(uuid.NewV7()), ukAccountData("t", email, "")),
			Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, Updates: updates})
		require.NoError(t, err)
		assert.Equal(t, forma.UpsertCreated, upserted.Outcome)
		return nil
	}))
}

// TestUpdateOfTheLeadSchemaRefusesAnExtraArrayAroundItsArrayOfObjects is
// the shipped-schema case an earlier attempt missed: on lead, whose
// propertyInterests is one array of objects, a report-only update sending
// it inside another array stored propertyInterests.propertyId at indices
// "0,0" and read it back as an array. It is refused, the row unchanged,
// while one array of objects is still written.
func TestUpdateOfTheLeadSchemaRefusesAnExtraArrayAroundItsArrayOfObjects(t *testing.T) {
	env := setupTxEnv(t)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(env.registry), shippedSchemaDir)
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable, EntityMain: env.tables.EntityMain, EAVData: env.tables.EAVData, ChangeLog: env.tables.ChangeLog,
		}},
		Query:  forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity: forma.EntityConfig{SchemaDirectory: shippedSchemaDir},
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry),
		NewDBPersistentRecordRepository(env.postgresPool, env.metadata), nil, env.registry, config, validator)
	created, err := em.Create(env.ctx, txCreateOp("lead", map[string]any{
		"id": uuid.NewString(), "tenantId": "tenant-1", "ownerUserId": "owner-1", "pipeline": "buy", "stage": "new", "status": "open",
		"contact":           map[string]any{"isAnonymous": false, "phones": []any{"1", "2"}},
		"propertyInterests": []any{map[string]any{"propertyId": "p1", "status": "favorited"}},
		"createdAt":         "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
	}))
	require.NoError(t, err)
	before := env.snapshot(t, "lead", created.RowID)

	for name, updates := range map[string]map[string]any{
		"extra array":            {"propertyInterests": []any{[]any{map[string]any{"propertyId": "p2", "status": "viewed"}}}},
		"array at a member":      {"propertyInterests": []any{map[string]any{"propertyId": []any{"p2"}}}},
		"empty object in phones": {"contact": map[string]any{"phones": []any{[]any{map[string]any{}}}}},
		"array at a scalar":      {"contact": map[string]any{"memo": []any{"a"}}},
	} {
		_, err := em.Update(env.ctx, txUpdateOp("lead", created.RowID, updates))
		require.ErrorIs(t, err, forma.ErrInvalidInput, name)
		assert.Equal(t, before, env.snapshot(t, "lead", created.RowID), "%s: the row as it was", name)
	}

	interests := []any{map[string]any{"propertyId": "p2", "status": "viewed"}, map[string]any{"propertyId": "p3", "status": "favorited"}}
	updated, err := em.Update(env.ctx, txUpdateOp("lead", created.RowID, map[string]any{"propertyInterests": interests}))
	require.NoError(t, err)
	assert.Equal(t, interests, updated.Attributes["propertyInterests"])
}

// TestTxUpdateKeepsTheNullRefusalsAndRefusesANullOnlyArray pins #696 review
// R1-F6 on PostgreSQL beside what it must not change: report-only
// Tx.Update refuses {"note": [null]} as a published shape refusal before it
// writes, and the Tx reads and commits after it; a direct null keeps its
// own refusal at its own stage, as before.
func TestTxUpdateKeepsTheNullRefusalsAndRefusesANullOnlyArray(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	e := newShapeEnv(t, env, false)
	data := ukAccountData("tenant-null", "null@x", "h-null")
	created, err := e.em.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	require.NoError(t, err)
	before := e.snapshot(t, ukAccount, created.RowID)

	refusal := e.inTxThenRead(t, created.RowID, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, txUpdateOp(ukAccount, created.RowID, map[string]any{"note": []any{nil}}))
		return err
	})
	requireShapeRefusal(t, refusal, "note")
	message, _ := forma.ResolvePublicMessage(refusal)
	assert.Contains(t, message, "declared type text takes a single value, got an array, inside arrays the schema does not declare there")
	assert.Equal(t, before, e.snapshot(t, ukAccount, created.RowID))

	_, err = e.em.Update(e.ctx, txUpdateOp(ukAccount, created.RowID, map[string]any{"note": nil}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, _ = forma.ResolvePublicMessage(err)
	assert.Contains(t, message, "attribute 'note' cannot be set to null", "a direct null keeps its refusal")
	assert.Equal(t, before, e.snapshot(t, ukAccount, created.RowID))
}

// TestBestEffortBatchRejectsOnlyTheRefusedShapeUpdate pins #696 review
// R1-F7 on PostgreSQL: a best-effort batch holding a valid update and an
// update that sends a container where a single value belongs commits the
// valid one, and fails the other alone, leaving its row, version, key
// entries and change_log exactly as they were, in both validation modes.
func TestBestEffortBatchRejectsOnlyTheRefusedShapeUpdate(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for _, strict := range []bool{false, true} {
		e := newShapeEnv(t, env, strict)
		rows := make([]uuid.UUID, 2)
		for i := range rows {
			created, err := e.em.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil,
				ukAccountData(fmt.Sprintf("batch-%v-%d", strict, i), fmt.Sprintf("batch-%v-%d@x", strict, i), fmt.Sprintf("hb-%v-%d", strict, i))))
			require.NoError(t, err)
			rows[i] = created.RowID
		}
		for _, attr := range []string{"tenant", "email", "note"} {
			label := fmt.Sprintf("strict=%v %s", strict, attr)
			valid, refused := e.snapshot(t, ukAccount, rows[0]), e.snapshot(t, ukAccount, rows[1])
			refusedKeys := e.keyEntriesOfRow(t, rows[1])
			note := "changed-" + attr
			result, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{
				*txUpdateOp(ukAccount, rows[0], map[string]any{"note": note}),
				*txUpdateOp(ukAccount, rows[1], map[string]any{attr: []any{"b@x"}}),
			}})
			require.NoError(t, err, label)
			require.Len(t, result.Successful, 1, label)
			require.Len(t, result.Failed, 1, label)
			assert.Equal(t, rows[0], result.Successful[0].RowID, label)
			assert.Equal(t, rows[1], result.Failed[0].Operation.RowID, label)
			requireShapeRefusal(t, bestEffortFailure{failure: result.Failed[0]}, attr, label)

			assert.NotEqual(t, valid, e.snapshot(t, ukAccount, rows[0]), "%s: the valid operation commits", label)
			got, err := e.em.Get(e.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &rows[0]})
			require.NoError(t, err)
			assert.Equal(t, note, got.Attributes["note"], label)
			assert.Equal(t, refused, e.snapshot(t, ukAccount, rows[1]), "%s: entity_main (version included), eav_data and change_log as they were", label)
			assert.ElementsMatch(t, refusedKeys, e.keyEntriesOfRow(t, rows[1]), "%s: key entries as they were", label)
		}
	}
	env.assertKeyTableMatchesRows(t)
}
