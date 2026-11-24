package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of #696 over ukaccount, the issue's schema: tenant is
// text bound to text_01 and a key component, email an EAV key component,
// note an EAV scalar. Before #696, report-only validation admitted {} and []
// at any of them, which kept the bound column and removed the EAV record,
// and ["a@x"] stored an element record, or failed the key planner with a
// redacted 500 at email. Every update surface now refuses each of these as
// invalid input before it writes, in every validation mode, and leaves the
// row's tables and key entries exactly as they were.

// shapeModes are the validation setups an update can run under: the default
// report-only update validation, strict update validation, and none.
var shapeModes = []struct {
	name     string
	validate bool
	strict   bool
}{
	{"report-only", true, false},
	{"strict", true, true},
	{"no validator", false, false},
}

// shapeValues are the containers refused at a scalar, with the kind the
// refusal names.
var shapeValues = []struct {
	kind  string
	value func(i int) any
}{
	{"an empty object", func(int) any { return map[string]any{} }},
	{"an empty array", func(int) any { return []any{} }},
	{"an array", func(i int) any { return []any{fmt.Sprintf("other-%d@x", i)} }},
	{"an object", func(int) any { return map[string]any{"x": "y"} }},
}

// shapeEnv is the keyed environment with a manager and transactor of the
// mode's own.
func shapeEnv(t *testing.T, validate, strict bool) *txEnv {
	t.Helper()
	env := setupUniqueKeyEnv(t)
	var validator *schemavalidate.Validator
	if validate {
		var err error
		validator, err = schemavalidate.New(SnapshotSchemaDocuments(env.registry), ukSchemaDir)
		require.NoError(t, err)
	}
	catalog := env.ukCatalog(t)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable, EntityMain: env.tables.EntityMain,
			EAVData: env.tables.EAVData, ChangeLog: env.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: ukSchemaDir, ValidateUpdatesStrict: strict},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	repo := NewDBPersistentRecordRepository(env.postgresPool, env.metadata, WithUniqueKeyCatalog(catalog))
	env.manager = mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry), repo, nil,
		env.registry, config, validator, WithUniqueKeys(catalog))
	txr, err := NewTransactor(env.manager)
	require.NoError(t, err)
	env.txr = txr.(*transactor)
	return env
}

// shapeRow is a created account and what it stores.
type shapeRow struct {
	id                  uuid.UUID
	tenant, email, note string
}

var shapeRowSeq atomic.Int64

func (e *txEnv) createShapeRow(t *testing.T) shapeRow {
	t.Helper()
	n := shapeRowSeq.Add(1)
	row := shapeRow{tenant: fmt.Sprintf("tenant-%d", n), email: fmt.Sprintf("e%d@x", n), note: fmt.Sprintf("note-%d", n)}
	data := ukAccountData(row.tenant, row.email, "")
	data["note"] = row.note
	created, err := e.manager.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	require.NoError(t, err)
	row.id = created.RowID
	return row
}

// shapeSurface runs updates against row on one update surface and answers
// the refusal. A Tx surface refuses inside a Tx that then writes another row
// and commits, so the refusal must leave the Tx usable and commit nothing.
type shapeSurface struct {
	name   string
	refuse func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error
}

func (e *txEnv) refuseInTx(t *testing.T, op func(ctx context.Context, tx forma.Tx) error) error {
	t.Helper()
	other := e.createShapeRow(t)
	var refusal error
	require.NoError(t, e.inTx(func(ctx context.Context, tx forma.Tx) error {
		refusal = op(ctx, tx)
		if !errors.Is(refusal, forma.ErrInvalidInput) {
			return nil
		}
		_, err := tx.Update(ctx, txUpdateOp(ukAccount, other.id, map[string]any{"note": "after-" + other.note}))
		return err
	}))
	require.Equal(t, "after-"+other.note, e.attrs(t, ukAccount, other.id)["note"], "the Tx stays usable after the refusal")
	return refusal
}

var shapeSurfaces = []shapeSurface{
	{"Update", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		_, err := e.manager.Update(e.ctx, txUpdateOp(ukAccount, row.id, updates))
		return err
	}},
	{"atomic BatchUpdate", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		_, err := e.manager.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{*txUpdateOp(ukAccount, row.id, updates)}})
		return err
	}},
	{"best-effort BatchUpdate", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		result, err := e.manager.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{*txUpdateOp(ukAccount, row.id, updates)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1)
		return bestEffortFailure{failure: result.Failed[0]}
	}},
	{"Tx.Update", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		return e.refuseInTx(t, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Update(ctx, txUpdateOp(ukAccount, row.id, updates))
			return err
		})
	}},
	{"Tx.UpdateWhere", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		return e.refuseInTx(t, func(ctx context.Context, tx forma.Tx) error {
			req := ukHandleWhere(row.note)
			req.Updates = updates
			_, err := tx.UpdateWhere(ctx, req)
			return err
		})
	}},
	{"Tx.Upsert by row ID", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		return e.refuseInTx(t, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: *ukCreateOp(ukAccount, row.id, ukAccountData(row.tenant, row.email, "")),
				Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, Updates: updates,
			})
			return err
		})
	}},
	{"Tx.Upsert by key", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
		return e.refuseInTx(t, func(ctx context.Context, tx forma.Tx) error {
			_, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: *ukCreateOp(ukAccount, uuid.Nil, ukAccountData(row.tenant, row.email, "")),
				Target: forma.ConflictTarget{Key: "tenant_email"}, Action: forma.UpsertUpdate, Updates: updates,
			})
			return err
		})
	}},
}

// claimSurface is Tx.Claim's mutation, which may not write a key component,
// so it is run at note only.
var claimSurface = shapeSurface{"Tx.Claim", func(t *testing.T, e *txEnv, row shapeRow, updates map[string]any) error {
	return e.refuseInTx(t, func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Claim(ctx, &forma.ClaimRequest{
			SchemaName: ukAccount, Condition: ukHandleWhere(row.note).Condition, Limit: 1, Updates: updates,
		})
		return err
	})
}}

func TestScalarShapeRefusedOnEveryUpdateSurfaceWithTheRowUnchanged(t *testing.T) {
	for _, mode := range shapeModes {
		t.Run(mode.name, func(t *testing.T) {
			env := shapeEnv(t, mode.validate, mode.strict)
			for _, surface := range append(shapeSurfaces, claimSurface) {
				for _, attr := range []string{"tenant", "email", "note"} {
					if surface.name == claimSurface.name && attr != "note" {
						continue
					}
					for i, value := range shapeValues {
						t.Run(surface.name+"/"+attr+"/"+value.kind, func(t *testing.T) {
							row := env.createShapeRow(t)
							before, keys := env.snapshot(t, ukAccount, row.id), env.keyEntriesOfRow(t, row.id)
							err := surface.refuse(t, env, row, map[string]any{attr: value.value(i)})
							assertShapeRefused(t, err, attr, "declared type text takes a single value, got "+value.kind)
							require.Equal(t, before, env.snapshot(t, ukAccount, row.id), "entity_main, eav_data and change_log after the refusal")
							require.Equal(t, keys, env.keyEntriesOfRow(t, row.id), "key entries after the refusal")
						})
					}
				}
			}
			env.assertKeyTableMatchesRows(t)
		})
	}
}

// An UpdateWhere or Claim that takes no row, and an Upsert that creates,
// write no row with Updates, so a refusal of their shape is not raised, as
// for every conversion refusal (#772). Text PostgreSQL cannot hold inside
// such a container is still refused on every route (#679).
func TestScalarShapeOnABranchThatWritesNoRowWithUpdates(t *testing.T) {
	env := shapeEnv(t, true, false)
	shape := map[string]any{"note": []any{"a"}}
	text := map[string]any{"note": []any{"a\x00b"}}
	for name, run := range map[string]func(ctx context.Context, tx forma.Tx, updates map[string]any) error{
		"UpdateWhere taking no row": func(ctx context.Context, tx forma.Tx, updates map[string]any) error {
			req := ukHandleWhere("no-such-note")
			req.Updates = updates
			result, err := tx.UpdateWhere(ctx, req)
			if err == nil {
				require.Empty(t, result.RowIDs)
			}
			return err
		},
		"Claim taking no row": func(ctx context.Context, tx forma.Tx, updates map[string]any) error {
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount, Condition: ukHandleWhere("no-such-note").Condition, Limit: 1, Updates: updates})
			return err
		},
		"Upsert that creates": func(ctx context.Context, tx forma.Tx, updates map[string]any) error {
			n := shapeRowSeq.Add(1)
			result, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: *ukCreateOp(ukAccount, uuid.Nil, ukAccountData(fmt.Sprintf("new-%d", n), fmt.Sprintf("new-%d@x", n), "")),
				Target: forma.ConflictTarget{Key: "tenant_email"}, Action: forma.UpsertUpdate, Updates: updates,
			})
			if err == nil {
				require.Equal(t, forma.UpsertCreated, result.Outcome)
			}
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error { return run(ctx, tx, shape) }))
			err := env.inTx(func(ctx context.Context, tx forma.Tx) error { return run(ctx, tx, text) })
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.ErrorContains(t, err, "text cannot contain a NUL character")
		})
	}
	env.assertKeyTableMatchesRows(t)
}

// An atomic batch holding one refused update writes none of its operations;
// a best-effort batch refuses only that one.
func TestScalarShapeInABatch(t *testing.T) {
	env := shapeEnv(t, true, false)
	valid, refused := env.createShapeRow(t), env.createShapeRow(t)
	before := map[uuid.UUID]rowSnapshot{valid.id: env.snapshot(t, ukAccount, valid.id), refused.id: env.snapshot(t, ukAccount, refused.id)}
	ops := []forma.EntityOperation{
		*txUpdateOp(ukAccount, valid.id, map[string]any{"note": "written"}),
		*txUpdateOp(ukAccount, refused.id, map[string]any{"tenant": map[string]any{}}),
	}
	_, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: ops})
	assertShapeRefused(t, err, "tenant", "got an empty object")
	for id, snapshot := range before {
		require.Equal(t, snapshot, env.snapshot(t, ukAccount, id), "an atomic batch with a refused operation writes nothing")
	}

	result, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Operations: ops})
	require.NoError(t, err)
	require.Len(t, result.Successful, 1)
	require.Len(t, result.Failed, 1)
	require.True(t, strings.Contains(result.Failed[0].Error, "invalid value for attribute 'tenant'"), result.Failed[0].Error)
	require.Equal(t, "written", env.attrs(t, ukAccount, valid.id)["note"])
	require.Equal(t, before[refused.id], env.snapshot(t, ukAccount, refused.id))
	env.assertKeyTableMatchesRows(t)
}

// Values the rule leaves alone are written as before: single values at the
// bound and EAV scalars, key components included, move the key, and an
// update that names neither key component keeps the bound column and its
// entry.
func TestScalarShapeLeavesSingleValuesAlone(t *testing.T) {
	for _, mode := range shapeModes {
		t.Run(mode.name, func(t *testing.T) {
			env := shapeEnv(t, mode.validate, mode.strict)
			row := env.createShapeRow(t)
			_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, row.id, map[string]any{"note": "changed"}))
			require.NoError(t, err)
			stored := env.attrs(t, ukAccount, row.id)
			require.Equal(t, row.tenant, stored["tenant"])
			require.Equal(t, row.email, stored["email"])
			require.Equal(t, "changed", stored["note"])

			_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, row.id, map[string]any{"tenant": "moved", "email": "moved@x"}))
			require.NoError(t, err)
			env.assertKeyTableMatchesRows(t)
			_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData(row.tenant, row.email, "")))
			require.NoError(t, err, "the old key value is free after the move")
			env.assertKeyTableMatchesRows(t)
		})
	}
}
