package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Fixtures of the unset operator on the Tx surfaces that write through the
// single-row update path (#939): UpdateWhere and Claim call the CRUD
// service's Update for each row they take, and Upsert's update action calls
// its updateValidated. Each surface here applies one mutation to one row, so
// a test can hold the result against Tx.Update's for the same mutation.

// unsetMutation is one mutation of one row: what each surface needs to
// reach the row, and what it writes there.
type unsetMutation struct {
	schema string
	rowID  uuid.UUID
	// create is the Upsert's create, a valid document; the row ID target
	// finds the row, so the create never runs.
	create map[string]any
	// selector is UpdateWhere's and Claim's condition: it matches the row
	// and no other, and the mutation falsifies it, as a claim requires.
	selector forma.Condition
	updates  map[string]any
	ops      []forma.UpdateOp
}

// unsetTxSurface applies an unsetMutation through one Tx method. It fails
// unless the method wrote exactly the mutation's row.
type unsetTxSurface struct {
	name  string
	apply func(ctx context.Context, tx forma.Tx, m unsetMutation) error
}

var (
	unsetViaUpdate = unsetTxSurface{"Tx.Update", func(ctx context.Context, tx forma.Tx, m unsetMutation) error {
		_, err := tx.Update(ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: m.schema, RowID: m.rowID},
			Updates: m.updates, UpdateOps: m.ops})
		return err
	}}
	unsetViaUpdateWhere = unsetTxSurface{"Tx.UpdateWhere", func(ctx context.Context, tx forma.Tx, m unsetMutation) error {
		result, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: m.schema, Condition: m.selector,
			Updates: m.updates, UpdateOps: m.ops})
		if err != nil {
			return err
		}
		return requireOnlyRow("UpdateWhere", m.rowID, result.RowIDs)
	}}
	unsetViaClaim = unsetTxSurface{"Tx.Claim", func(ctx context.Context, tx forma.Tx, m unsetMutation) error {
		result, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: m.schema, Condition: m.selector, Limit: 1,
			Updates: m.updates, UpdateOps: m.ops})
		if err != nil {
			return err
		}
		return requireOnlyRow("Claim", m.rowID, rowIDsOf(result.Records))
	}}
	unsetViaUpsert = unsetTxSurface{"Tx.Upsert", func(ctx context.Context, tx forma.Tx, m unsetMutation) error {
		result, err := tx.Upsert(ctx, &forma.UpsertRequest{
			Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: m.schema, RowID: m.rowID}, Data: m.create},
			Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, Updates: m.updates, UpdateOps: m.ops})
		if err != nil {
			return err
		}
		if result.Outcome != forma.UpsertUpdated {
			return fmt.Errorf("upsert of %s answered %s, want %s", m.rowID, result.Outcome, forma.UpsertUpdated)
		}
		return requireOnlyRow("Upsert", m.rowID, []uuid.UUID{result.Record.RowID})
	}}
)

// unsetWriteSurfaces are the surfaces #939 covers.
var unsetWriteSurfaces = []unsetTxSurface{unsetViaUpdateWhere, unsetViaClaim, unsetViaUpsert}

func requireOnlyRow(method string, want uuid.UUID, got []uuid.UUID) error {
	if len(got) != 1 || got[0] != want {
		return fmt.Errorf("%s wrote rows %v, want only %s", method, got, want)
	}
	return nil
}

// applyIn runs the mutation through surface in a Tx of its own and commits
// it.
func (e *txEnv) applyIn(surface unsetTxSurface, m unsetMutation) error {
	return e.inTx(func(ctx context.Context, tx forma.Tx) error { return surface.apply(ctx, tx, m) })
}

// unsetTagged is a selector matching the rows whose attr equals value and
// that still hold present: an unset of present falsifies it.
func unsetTagged(attr, value, present string) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: attr, Value: "equals:" + value},
		&forma.KvCondition{Attr: present, Value: "is_not_null:true"},
	}}
}

// countStatements counts every statement the transactor's sessions send
// from now until the test ends.
func (e *txEnv) countStatements(t *testing.T) *atomic.Int64 {
	t.Helper()
	var sent atomic.Int64
	previous := e.txr.statementFault
	e.txr.statementFault = func(sql string) error {
		sent.Add(1)
		if previous != nil {
			return previous(sql)
		}
		return nil
	}
	t.Cleanup(func() { e.txr.statementFault = previous })
	return &sent
}

// storedImage is a row's entity_main columns and eav_data rows as
// PostgreSQL renders them, without what tells one row from another (its
// row ID) or one write from another (its stamps): two rows given the same
// document and the same mutation have the same image.
type storedImage struct {
	main map[string]any
	eav  []map[string]any
}

func (e *txEnv) storedImage(t *testing.T, schema string, rowID uuid.UUID) storedImage {
	t.Helper()
	snapshot := e.snapshot(t, schema, rowID)
	var image storedImage
	require.NoError(t, json.Unmarshal([]byte(snapshot.main), &image.main))
	require.NoError(t, json.Unmarshal([]byte(snapshot.eav), &image.eav))
	for _, column := range []string{"ltbase_row_id", "ltbase_created_at", "ltbase_updated_at"} {
		require.Contains(t, image.main, column)
		delete(image.main, column)
	}
	for _, row := range image.eav {
		delete(row, "row_id")
	}
	return image
}
