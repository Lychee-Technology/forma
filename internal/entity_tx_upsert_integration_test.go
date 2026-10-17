package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of Tx.Upsert (#651 part C) over ukidem, whose
// idempotency key is (instance_id, idempotency_key) and whose external_ref
// key is a second, non-target key, and ukhistory, which declares no key and
// is upserted on its row ID. Every Record is compared with a direct read
// after commit.

func idemCreate(instance, key, status, hash string) forma.EntityOperation {
	return forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: ukIdem},
		Data:             map[string]any{"instance_id": instance, "idempotency_key": key, "status": status, "request_hash": hash, "counter": 0},
	}
}

func onIdempotency(create forma.EntityOperation, action forma.UpsertAction) *forma.UpsertRequest {
	return &forma.UpsertRequest{Create: create, Target: forma.ConflictTarget{Key: "idempotency"}, Action: action}
}

// upsertInTx runs one Upsert in its own Tx and commits it.
func (e *txEnv) upsertInTx(t *testing.T, req *forma.UpsertRequest) (*forma.UpsertResult, error) {
	t.Helper()
	var result *forma.UpsertResult
	err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
		var err error
		result, err = tx.Upsert(ctx, req)
		return err
	})
	return result, err
}

// requireStored asserts that record is the row as stored, read after commit.
func (e *txEnv) requireStored(t *testing.T, record *forma.DataRecord) {
	t.Helper()
	require.NotNil(t, record)
	stored, err := e.manager.Get(e.ctx, &forma.QueryRequest{SchemaName: record.SchemaName, RowID: &record.RowID})
	require.NoError(t, err)
	assert.Equal(t, stored.Attributes, record.Attributes)
}

func TestUpsertOnKeyCreatesFindsAndUpdates(t *testing.T) {
	env := setupUniqueKeyEnv(t)

	res, err := env.upsertInTx(t, onIdempotency(idemCreate("i1", "k1", "running", "h1"), forma.UpsertDoNothing))
	require.NoError(t, err)
	assert.Equal(t, forma.UpsertCreated, res.Outcome)
	env.requireStored(t, res.Record)
	rowID := res.Record.RowID

	res, err = env.upsertInTx(t, onIdempotency(idemCreate("i1", "k1", "other", "h2"), forma.UpsertDoNothing))
	require.NoError(t, err)
	assert.Equal(t, forma.UpsertExisting, res.Outcome)
	assert.Equal(t, rowID, res.Record.RowID)
	assert.Equal(t, "running", res.Record.Attributes["status"], "do_nothing answers the row as stored")
	env.requireStored(t, res.Record)

	req := onIdempotency(idemCreate("i1", "k1", "ignored", "h3"), forma.UpsertUpdate)
	req.Preconditions = &forma.KvCondition{Attr: "status", Value: "equals:running"}
	req.Updates = map[string]any{"status": "done"}
	req.UpdateOps = []forma.UpdateOp{forma.Increment("counter", 1)}
	res, err = env.upsertInTx(t, req)
	require.NoError(t, err)
	assert.Equal(t, forma.UpsertUpdated, res.Outcome)
	assert.Equal(t, rowID, res.Record.RowID)
	assert.Equal(t, "done", res.Record.Attributes["status"])
	assert.Equal(t, "h1", res.Record.Attributes["request_hash"], "the create's data never reaches the existing row")
	env.requireStored(t, res.Record)
	assert.Equal(t, 1, env.mainRowCount(t, ukIdem))
	env.assertKeyTableMatchesRows(t)
}

func TestUpsertExpectedFailuresWriteNothingAndKeepTheTxUsable(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	holder := env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "running", "external_ref": "ref-1"})
	before, rows := env.snapshot(t, ukIdem, holder), env.schemaRowCount(t, ukIdem)

	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		req := onIdempotency(idemCreate("i1", "k1", "x", "h"), forma.UpsertUpdate)
		req.Preconditions = &forma.KvCondition{Attr: "status", Value: "equals:failed"}
		req.Updates = map[string]any{"status": "retried"}
		if _, err := tx.Upsert(ctx, req); !errors.Is(err, forma.ErrPreconditionFailed) {
			return fmt.Errorf("a failed precondition answered %v", err)
		}

		// The target is free, but the create takes external_ref, which
		// the holder has.
		create := idemCreate("i2", "k2", "running", "h")
		create.Data["external_ref"] = "ref-1"
		_, err := tx.Upsert(ctx, onIdempotency(create, forma.UpsertDoNothing))
		requireKeyConflict(t, err, ukIdem, "external_ref", "ref-1")

		// A row-ID create onto the holder with a key target.
		create = idemCreate("i3", "k3", "running", "h")
		create.RowID = holder
		_, err = tx.Upsert(ctx, onIdempotency(create, forma.UpsertDoNothing))
		require.ErrorIs(t, err, forma.ErrUniqueConflict)
		require.ErrorContains(t, err, "already has a row with ID")

		// A row-ID target that is free, with a create that takes the
		// holder's idempotency key.
		create = idemCreate("i1", "k1", "running", "h")
		create.RowID = uuid.New()
		_, err = tx.Upsert(ctx, &forma.UpsertRequest{Create: create, Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertDoNothing})
		requireKeyConflict(t, err, ukIdem, "idempotency", "k1")

		// A malformed precondition operand is refused before any SQL even
		// though a row holds the target, so it does not poison the Tx.
		req = onIdempotency(idemCreate("i1", "k1", "x", "h"), forma.UpsertUpdate)
		req.Preconditions = &forma.KvCondition{Attr: "counter", Value: "lt:NaN"}
		req.Updates = map[string]any{"status": "retried"}
		_, err = tx.Upsert(ctx, req)
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		return nil
	}), "none of the outcomes poisons the Tx, so it commits")
	assert.Equal(t, before, env.snapshot(t, ukIdem, holder), "the holder is unchanged")
	assert.Equal(t, rows, env.schemaRowCount(t, ukIdem), "no row was written")
	env.assertKeyTableMatchesRows(t)
}

func TestUpsertOnRowID(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID := uuid.New()
	event := func(n int) *forma.UpsertRequest {
		return &forma.UpsertRequest{
			Create: forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: ukHistory, RowID: rowID},
				Data:             map[string]any{"event": fmt.Sprintf("e%d", n), "dedup_count": 0},
			},
			Target: forma.ConflictTarget{RowID: true},
			Action: forma.UpsertDoNothing,
		}
	}
	res, err := env.upsertInTx(t, event(1))
	require.NoError(t, err)
	require.Equal(t, forma.UpsertCreated, res.Outcome)
	assert.Equal(t, rowID, res.Record.RowID)
	env.requireStored(t, res.Record)

	res, err = env.upsertInTx(t, event(2))
	require.NoError(t, err)
	require.Equal(t, forma.UpsertExisting, res.Outcome)
	assert.Equal(t, "e1", res.Record.Attributes["event"])

	// A deleted row ID is free: the upsert creates it again.
	require.NoError(t, env.manager.Delete(env.ctx, txDeleteOp(ukHistory, rowID)))
	res, err = env.upsertInTx(t, event(3))
	require.NoError(t, err)
	require.Equal(t, forma.UpsertCreated, res.Outcome)
	assert.Equal(t, "e3", res.Record.Attributes["event"])
	env.requireStored(t, res.Record)
}

func TestUpsertRefusesMalformedRequestsBeforeAnySQL(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	valid := func() *forma.UpsertRequest {
		return onIdempotency(idemCreate("i1", "k1", "running", "h"), forma.UpsertDoNothing)
	}
	cases := []struct {
		name string
		req  func() *forma.UpsertRequest
		want string
	}{
		{"nil request", func() *forma.UpsertRequest { return nil }, "upsert request cannot be nil"},
		{"no target", func() *forma.UpsertRequest { r := valid(); r.Target = forma.ConflictTarget{}; return r }, "needs a conflict target"},
		{"two targets", func() *forma.UpsertRequest { r := valid(); r.Target.RowID = true; return r }, "names two conflict targets"},
		{"row-ID target without a row ID", func() *forma.UpsertRequest {
			r := valid()
			r.Target = forma.ConflictTarget{RowID: true}
			return r
		}, "Create.RowID must be set"},
		{"undeclared key", func() *forma.UpsertRequest { r := valid(); r.Target.Key = "nosuch"; return r }, "unique key nosuch, which the schema does not declare"},
		{"create lacks a key attribute", func() *forma.UpsertRequest {
			r := valid()
			delete(r.Create.Data, "idempotency_key")
			return r
		}, "must set every attribute of it (instance_id, idempotency_key)"},
		{"unknown action", func() *forma.UpsertRequest { r := valid(); r.Action = "merge"; return r }, `Action must be "do_nothing" or "update", got "merge"`},
		{"missing action", func() *forma.UpsertRequest { r := valid(); r.Action = ""; return r }, `got ""`},
		{"updates with do_nothing", func() *forma.UpsertRequest { r := valid(); r.Updates = map[string]any{"status": "x"}; return r }, "takes no Preconditions, Updates or UpdateOps"},
		{"operators with do_nothing", func() *forma.UpsertRequest {
			r := valid()
			r.UpdateOps = []forma.UpdateOp{forma.Increment("counter", 1)}
			return r
		}, "takes no Preconditions, Updates or UpdateOps"},
		{"preconditions with do_nothing", func() *forma.UpsertRequest {
			r := valid()
			r.Preconditions = &forma.KvCondition{Attr: "status", Value: "equals:x"}
			return r
		}, "takes no Preconditions, Updates or UpdateOps"},
		{"update with nothing to apply", func() *forma.UpsertRequest { r := valid(); r.Action = forma.UpsertUpdate; return r }, "needs Updates or UpdateOps"},
		{"update operator on an unknown attribute", func() *forma.UpsertRequest {
			r := valid()
			r.Action = forma.UpsertUpdate
			r.UpdateOps = []forma.UpdateOp{forma.Increment("nosuch", 1)}
			return r
		}, "nosuch"},
		{"create of another type", func() *forma.UpsertRequest { r := valid(); r.Create.Type = forma.OperationUpdate; return r }, "Create.Type must be empty"},
		{"a precondition operand only rendering parses", func() *forma.UpsertRequest {
			r := valid()
			r.Action = forma.UpsertUpdate
			r.Updates = map[string]any{"status": "x"}
			r.Preconditions = &forma.KvCondition{Attr: "counter", Value: "lt:NaN"}
			return r
		}, "invalid numeric value for 'counter': NaN"},
		{"create with preconditions", func() *forma.UpsertRequest {
			r := valid()
			r.Create.Preconditions = &forma.KvCondition{Attr: "status", Value: "equals:x"}
			return r
		}, "create does not accept preconditions"},
	}
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		for _, tc := range cases {
			_, err := tx.Upsert(ctx, tc.req())
			require.ErrorIs(t, err, forma.ErrInvalidInput, tc.name)
			assert.Contains(t, err.Error(), tc.want, tc.name)
		}
		_, err := tx.Upsert(ctx, valid())
		return err
	}), "every refusal leaves the Tx usable")
	assert.Equal(t, 1, env.mainRowCount(t, ukIdem))
}
