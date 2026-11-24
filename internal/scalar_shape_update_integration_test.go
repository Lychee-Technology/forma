package internal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of #696 over ukaccount: an update that gives a
// scalar attribute an object or array, empty or not. Report-only update
// validation, the default, admitted it, and what the transformer then stored
// depended on the binding: {} and [] left the column-bound tenant alone and
// removed the EAV note, ["a"] stored array elements under a scalar, and a
// key component sent ["b@x"] failed in the key planner with a redacted 500.
// Every update route now refuses it as invalid input, before it writes.

type shapeUpdate struct {
	name    string
	updates map[string]any
	want    string // the published message's prefix
}

func ukShapeRefusal(attr string, id int, got string) string {
	return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): declared type text takes a single value, got %s", attr, id, got)
}

// ukShapeUpdates are the shapes the issue lists at the bound key component
// tenant, the EAV key component email and the unkeyed EAV note, plus the
// nested and alternate spellings. keyed says whether the cases may touch a
// key component, which a Claim mutation may not.
func ukShapeUpdates(keyed bool) []shapeUpdate {
	attrs := []struct {
		name string
		id   int
	}{{"note", 5}}
	if keyed {
		attrs = append(attrs, struct {
			name string
			id   int
		}{"tenant", 1}, struct {
			name string
			id   int
		}{"email", 2})
	}
	var cases []shapeUpdate
	for _, attr := range attrs {
		for _, shape := range []struct {
			name  string
			value any
			got   string
		}{
			{"{}", map[string]any{}, "an empty object"},
			{"[]", []any{}, "an empty array"},
			{"[x]", []any{"b@x"}, "an array of 1 element"},
			{"{x: y}", map[string]any{"x": "y"}, "an object"},
			{"[[]]", []any{[]any{}}, "an array of 1 element"},
		} {
			cases = append(cases, shapeUpdate{attr.name + " " + shape.name, map[string]any{attr.name: shape.value},
				ukShapeRefusal(attr.name, attr.id, shape.got)})
		}
	}
	return cases
}

func requireShapeRefused(t *testing.T, err error, want string, msgAndArgs ...any) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput, msgAndArgs...)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	assert.Contains(t, message, want, msgAndArgs...)
}

// ukShapeState is a row's stored state: its three tables by direct SQL and
// its key-table entries.
type ukShapeState struct {
	row  rowSnapshot
	keys []keyEntry
}

func (e *txEnv) ukShapeState(t *testing.T, rowID uuid.UUID) ukShapeState {
	t.Helper()
	return ukShapeState{row: e.snapshot(t, ukAccount, rowID), keys: e.keyEntriesOfRow(t, rowID)}
}

// ukShapeRow creates an account whose every attribute is set, so a dropped
// value would show, and answers it with its handle and note.
func (e *txEnv) ukShapeRow(t *testing.T, i int) (uuid.UUID, string, string) {
	t.Helper()
	handle, note := fmt.Sprintf("h-%d", i), fmt.Sprintf("n-%d", i)
	data := ukAccountData(fmt.Sprintf("T-%d", i), fmt.Sprintf("e-%d@x", i), handle)
	data["note"], data["counter"], data["seq"] = note, i, i
	record, err := e.manager.Create(e.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
	require.NoError(t, err)
	return record.RowID, handle, note
}

// ukShapeTxRoutes are the Tx routes that write an existing row with Updates.
// Each is handed the row, its handle and note, and returns the route's error.
func ukShapeTxRoutes() map[string]func(context.Context, forma.Tx, uuid.UUID, string, string, map[string]any) error {
	upsert := func(target forma.ConflictTarget) func(context.Context, forma.Tx, uuid.UUID, string, string, map[string]any) error {
		return func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, handle, _ string, updates map[string]any) error {
			create := *ukCreateOp(ukAccount, rowID, ukAccountData("other", "other@x", handle))
			if target.Key != "" {
				create.RowID = uuid.Nil
			}
			result, err := tx.Upsert(ctx, &forma.UpsertRequest{Create: create, Target: target, Action: forma.UpsertUpdate, Updates: updates})
			if err == nil && result.Outcome != forma.UpsertUpdated {
				err = fmt.Errorf("upsert outcome %v", result.Outcome)
			}
			return err
		}
	}
	return map[string]func(context.Context, forma.Tx, uuid.UUID, string, string, map[string]any) error{
		"Tx.Update": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, _, _ string, updates map[string]any) error {
			_, err := tx.Update(ctx, txUpdateOp(ukAccount, rowID, updates))
			return err
		},
		"Tx.UpdateWhere": func(ctx context.Context, tx forma.Tx, _ uuid.UUID, _, note string, updates map[string]any) error {
			req := ukHandleWhere(note)
			req.Updates, req.MaxAffected = updates, 1
			_, err := tx.UpdateWhere(ctx, req)
			return err
		},
		"Tx.Upsert by row ID": upsert(forma.ConflictTarget{RowID: true}),
		"Tx.Upsert by key":    upsert(forma.ConflictTarget{Key: "handle"}),
	}
}

// TestScalarShapeUpdateRefusedOnEveryRoute is the issue's acceptance on the
// manager and Tx routes: each refuses the value as invalid input naming the
// attribute, the row (all three tables, its version and its key entries) is
// as it was, and a Tx that saw the refusal still commits the write after it.
func TestScalarShapeUpdateRefusedOnEveryRoute(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	i := 0
	next := func() (uuid.UUID, string, string) { i++; return env.ukShapeRow(t, i) }

	for _, tc := range ukShapeUpdates(true) {
		rowID, _, _ := next()
		before := env.ukShapeState(t, rowID)
		_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, tc.updates))
		requireShapeRefused(t, err, tc.want, "Update %s", tc.name)
		assert.Equal(t, before, env.ukShapeState(t, rowID), "Update %s: the row as it was", tc.name)

		other, _, _ := next()
		otherBefore := env.ukShapeState(t, other)
		_, err = env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Atomic: true, Operations: []forma.EntityOperation{
			*txUpdateOp(ukAccount, other, map[string]any{"note": "changed"}), *txUpdateOp(ukAccount, rowID, tc.updates)}})
		requireShapeRefused(t, err, "operation[1]: "+tc.want, "atomic BatchUpdate %s", tc.name)
		assert.Equal(t, before, env.ukShapeState(t, rowID), "atomic BatchUpdate %s: the row as it was", tc.name)
		assert.Equal(t, otherBefore, env.ukShapeState(t, other), "atomic BatchUpdate %s: nothing written", tc.name)

		result, err := env.manager.BatchUpdate(env.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{
			*txUpdateOp(ukAccount, other, map[string]any{"note": "changed"}), *txUpdateOp(ukAccount, rowID, tc.updates)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1, "best-effort BatchUpdate %s", tc.name)
		assert.Contains(t, result.Failed[0].Error, tc.want, "best-effort BatchUpdate %s", tc.name)
		require.Len(t, result.Successful, 1, "best-effort BatchUpdate %s writes the valid operation", tc.name)
		assert.Equal(t, before, env.ukShapeState(t, rowID), "best-effort BatchUpdate %s: the row as it was", tc.name)
		assert.Equal(t, "changed", env.attrs(t, ukAccount, other)["note"])
	}

	// Each Tx route: a transaction that only sees the refusal commits, and
	// the row (all three tables and its key entries) is as it was; one that
	// goes on after the refusal commits its write to another row.
	for route, write := range ukShapeTxRoutes() {
		for _, tc := range ukShapeUpdates(true) {
			rowID, handle, note := next()
			other, _, _ := next()
			before := env.ukShapeState(t, rowID)
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				requireShapeRefused(t, write(ctx, tx, rowID, handle, note, tc.updates), tc.want, "%s %s", route, tc.name)
				return nil
			}), "%s %s", route, tc.name)
			assert.Equal(t, before, env.ukShapeState(t, rowID), "%s %s: the committed row as it was", route, tc.name)

			err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
				requireShapeRefused(t, write(ctx, tx, rowID, handle, note, tc.updates), tc.want, "%s %s", route, tc.name)
				_, err := tx.Update(ctx, txUpdateOp(ukAccount, other, map[string]any{"counter": 1000}))
				return err
			})
			require.NoError(t, err, "%s %s: the refusal left the Tx usable", route, tc.name)
			assert.EqualValues(t, 1000, env.attrs(t, ukAccount, other)["counter"], "%s %s: the write after the refusal committed", route, tc.name)
			assert.Equal(t, before, env.ukShapeState(t, rowID), "%s %s: the refused row as it was", route, tc.name)
		}
	}

	for _, tc := range ukShapeUpdates(false) {
		rowID, _, note := next()
		before := env.ukShapeState(t, rowID)
		err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
			updates := map[string]any{"counter": -1}
			for k, v := range tc.updates {
				updates[k] = v
			}
			_, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount,
				Condition: &forma.KvCondition{Attr: "note", Value: "equals:" + note}, Limit: 1, Updates: updates})
			requireShapeRefused(t, err, tc.want, "Tx.Claim %s", tc.name)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, before, env.ukShapeState(t, rowID), "Tx.Claim %s: the row as it was", tc.name)
	}
	env.assertKeyTableMatchesRows(t)
}

// TestScalarShapeUpdateWithNoRowToWriteSucceeds: an UpdateWhere or Claim
// taking no row, and an Upsert that creates, never write Updates, and the
// shape refusal waits for a row as the conversion refusals of #772 do. Text
// PostgreSQL cannot hold inside such a container does not wait: intake has
// refused it on every route since #679, whatever the row, and still does.
func TestScalarShapeUpdateWithNoRowToWriteSucceeds(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	nobody := &forma.KvCondition{Attr: "note", Value: "equals:nobody"}
	// noRow runs tc on the routes that write no row: UpdateWhere and Upsert
	// when write, and Claim, whose mutation may not touch a key component,
	// when claim.
	noRow := func(tc shapeUpdate, j int, write, claim bool) map[string]error {
		results := map[string]error{}
		require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
			if write {
				updated, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: ukAccount, Condition: nobody, MaxAffected: 1, Updates: tc.updates})
				results["UpdateWhere matching no row"] = err
				if err == nil {
					assert.Empty(t, updated.RowIDs)
				}
				created, err := tx.Upsert(ctx, &forma.UpsertRequest{
					Create: *ukCreateOp(ukAccount, uuid.Nil, ukAccountData("T", fmt.Sprintf("new-%d@x", j), fmt.Sprintf("new-%d", j))),
					Target: forma.ConflictTarget{Key: "handle"}, Action: forma.UpsertUpdate, Updates: tc.updates,
				})
				results["Upsert creating its row"] = err
				if err == nil {
					assert.Equal(t, forma.UpsertCreated, created.Outcome)
				}
			}
			if claim {
				claimed, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: ukAccount, Condition: nobody, Limit: 1, Updates: tc.updates})
				results["Claim matching no row"] = err
				if err == nil {
					assert.Empty(t, claimed.Records)
				}
			}
			return nil
		}))
		return results
	}
	for j, tc := range ukShapeUpdates(true) {
		for route, err := range noRow(tc, j, true, false) {
			require.NoError(t, err, "%s, %s", route, tc.name)
		}
	}
	for j, tc := range ukShapeUpdates(false) {
		for route, err := range noRow(tc, 1000+j, false, true) {
			require.NoError(t, err, "%s, %s", route, tc.name)
		}
	}
	const nul = "invalid value for attribute 'note' (attrID=5): text cannot contain a NUL character (U+0000)"
	for j, updates := range []map[string]any{{"note": []any{"a\x00b"}}, {"note": []any{[]any{"a\x00b"}}}} {
		tc := shapeUpdate{name: fmt.Sprint(updates), updates: updates}
		results := noRow(tc, 2000+j, true, true)
		require.Len(t, results, 3)
		for route, err := range results {
			require.ErrorIs(t, err, forma.ErrInvalidInput, "%s, %s", route, tc.name)
			message, published := forma.ResolvePublicMessage(err)
			require.True(t, published, err)
			assert.Contains(t, message, nul, "%s, %s", route, tc.name)
		}
	}
	env.assertKeyTableMatchesRows(t)
}

// TestScalarShapeUpdateKeepsSparseColumnRetention: the refusal does not
// change what a valid update stores. One that names neither key component
// leaves the bound tenant's column and both components' key entry as they
// were (#682), and one that replaces a component moves its entry.
func TestScalarShapeUpdateKeepsSparseColumnRetention(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID, _, _ := env.ukShapeRow(t, 1)
	keys := env.keyEntriesOfRow(t, rowID)

	_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, map[string]any{"note": "other"}))
	require.NoError(t, err)
	assert.Equal(t, keys, env.keyEntriesOfRow(t, rowID))
	got := env.attrs(t, ukAccount, rowID)
	assert.Equal(t, "T-1", got["tenant"])
	assert.Equal(t, "e-1@x", got["email"])

	_, err = env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, map[string]any{"tenant": "T-2", "email": "e-2@x"}))
	require.NoError(t, err)
	assert.NotEqual(t, keys, env.keyEntriesOfRow(t, rowID))
	env.assertKeyTableMatchesRows(t)
	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("T-2", "e-2@x", "")))
	requireKeyConflict(t, err, ukAccount, "tenant_email", "e-2@x")
}

// TestScalarShapeUpdateOverHTTPIsA400: the update route answers 400 with
// the published message, where ["b@x"] at the EAV key component email used
// to fail in the key planner and answer a redacted 500.
func TestScalarShapeUpdateOverHTTPIsA400(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	handler := httpapi.NewServer(env.manager, httpapi.Options{}).Handler()
	for i, tc := range []struct{ body, want string }{
		{`{"email": ["b@x"]}`, ukShapeRefusal("email", 2, "an array of 1 element")},
		{`{"tenant": {}}`, ukShapeRefusal("tenant", 1, "an empty object")},
		{`{"note": []}`, ukShapeRefusal("note", 5, "an empty array")},
	} {
		rowID, _, _ := env.ukShapeRow(t, i)
		before := env.ukShapeState(t, rowID)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/"+ukAccount+"/"+rowID.String(), strings.NewReader(tc.body)))
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", tc.body, rec.Body.String())
		assert.Contains(t, rec.Body.String(), tc.want, tc.body)
		assert.Equal(t, before, env.ukShapeState(t, rowID), "%s: the row as it was", tc.body)
	}
}
