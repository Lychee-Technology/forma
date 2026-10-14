package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Predicate mutation (#652) against a pgxmock pool: what a request refuses
// before any SQL, the candidate order, and that the federated engine is
// never called. The mock fails on any statement a test did not queue.

func scheduledVisits() *forma.MutateWhereRequest {
	return &forma.MutateWhereRequest{SchemaName: "visit", Condition: &forma.KvCondition{Attr: "status", Value: "equals:scheduled"}}
}

func TestMutateWhereRefusesAnInvalidRequestBeforeAnySQL(t *testing.T) {
	update := func(edit func(*forma.MutateWhereRequest)) *forma.MutateWhereRequest {
		req := scheduledVisits()
		req.Updates = map[string]any{"status": "visited"}
		edit(req)
		return req
	}
	del := func(edit func(*forma.MutateWhereRequest)) *forma.MutateWhereRequest {
		req := scheduledVisits()
		edit(req)
		return req
	}
	cases := []struct {
		name    string
		delete  bool
		req     *forma.MutateWhereRequest
		message string
	}{
		{"a nil request", false, nil, "request cannot be nil"},
		{"no schema", false, update(func(r *forma.MutateWhereRequest) { r.SchemaName = "" }), "needs a schema name"},
		{"an unregistered schema", false, update(func(r *forma.MutateWhereRequest) { r.SchemaName = "no_such_schema" }), "not registered"},
		{"a nil condition", false, update(func(r *forma.MutateWhereRequest) { r.Condition = nil }), "no unconditioned bulk mutation"},
		{"a condition on an unknown attribute", false, update(func(r *forma.MutateWhereRequest) {
			r.Condition = &forma.KvCondition{Attr: "no_such_attr", Value: "equals:1"}
		}), "condition tests attribute 'no_such_attr'"},
		{"an empty and", true, del(func(r *forma.MutateWhereRequest) {
			r.Condition = &forma.CompositeCondition{Logic: forma.LogicAnd}
		}), "empty and group"},
		{"an empty and beside a leaf of an or", true, del(func(r *forma.MutateWhereRequest) {
			r.Condition = &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "status", Value: "equals:scheduled"}, &forma.CompositeCondition{Logic: forma.LogicAnd}}}
		}), "empty and group"},
		{"an empty or", false, update(func(r *forma.MutateWhereRequest) {
			r.Condition = &forma.CompositeCondition{Logic: forma.LogicOr}
		}), "empty or group"},
		{"a malformed operand", false, update(func(r *forma.MutateWhereRequest) {
			r.Condition = &forma.KvCondition{Attr: "scheduledStartAt", Value: "gt:not-a-date"}
		}), "scheduledStartAt"},
		{"an unknown sort attribute", false, update(func(r *forma.MutateWhereRequest) {
			r.Sort = []forma.OrderBy{{Attribute: "no_such_attr"}}
		}), "cannot sort by unknown attribute"},
		{"a negative limit", false, update(func(r *forma.MutateWhereRequest) { r.Limit = -1 }), "limit -1 is negative"},
		{"a negative MaxAffected", false, update(func(r *forma.MutateWhereRequest) { r.MaxAffected = -1 }), "MaxAffected -1 is out of range"},
		{"MaxAffected past the maximum", false, update(func(r *forma.MutateWhereRequest) { r.MaxAffected = 10001 }), "MaxAffected 10001 is out of range"},
		{"a limit past MaxAffected", false, update(func(r *forma.MutateWhereRequest) { r.Limit, r.MaxAffected = 5, 3 }), "limit 5 exceeds MaxAffected 3"},
		{"a limit past the default MaxAffected", false, update(func(r *forma.MutateWhereRequest) { r.Limit = 1001 }), "limit 1001 exceeds MaxAffected 1000"},
		{"an update with neither Updates nor UpdateOps", false, update(func(r *forma.MutateWhereRequest) { r.Updates = nil }), "needs Updates or UpdateOps"},
		{"an attribute in both Updates and UpdateOps", false, update(func(r *forma.MutateWhereRequest) {
			r.Updates = map[string]any{"updatedAt": "2026-10-01T00:00:00Z"}
			r.UpdateOps = []forma.UpdateOp{forma.ServerTime("updatedAt", 0)}
		}), "updatedAt"},
		{"one attribute with two operators", false, update(func(r *forma.MutateWhereRequest) {
			r.UpdateOps = []forma.UpdateOp{forma.ServerTime("updatedAt", 0), forma.ServerTime("updatedAt", 0)}
		}), "more than one update operator"},
		{"a delete with Updates", true, del(func(r *forma.MutateWhereRequest) { r.Updates = map[string]any{"status": "x"} }), "does not accept Updates or UpdateOps"},
		{"a delete with UpdateOps", true, del(func(r *forma.MutateWhereRequest) {
			r.UpdateOps = []forma.UpdateOp{forma.ServerTime("updatedAt", 0)}
		}), "does not accept Updates or UpdateOps"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txr, mock := newMockTransactorOver(t, nil, "visit")
			mock.ExpectBeginTx(readCommittedTx)
			mock.ExpectCommit()
			err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
				call := tx.UpdateWhere
				if tc.delete {
					call = tx.DeleteWhere
				}
				_, err := call(ctx, tc.req)
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				require.ErrorContains(t, err, tc.message)
				return nil
			})
			require.NoError(t, err, "a refused request leaves the Tx able to commit")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// expectCandidateSelect queues the candidate select of scheduledVisits
// (schema, the condition's attribute and value, limit, offset), which
// matches nothing, and checks its ORDER BY ends in the row ID tiebreak.
func expectCandidateSelect(mock pgxmock.PgxPoolIface, orderBy string) {
	mock.ExpectQuery(`(?s)^\s*WITH anchor AS .*ORDER BY\s+`+orderBy+`\s+LIMIT`).
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_row_id"}))
}

// The candidates are ordered by Sort with the row ID ascending as the last
// key: alone when Sort is empty, after every sort key otherwise. Sort names
// schema attributes only, so the row ID can never appear twice.
func TestMutateWhereOrdersCandidatesWithTheRowIDLast(t *testing.T) {
	cases := []struct {
		name    string
		sort    []forma.OrderBy
		orderBy string
	}{
		{"no sort", nil, `row_id`},
		{"one key", []forma.OrderBy{{Attribute: "scheduledStartAt", SortOrder: forma.SortOrderDesc}}, `k0 DESC NULLS LAST,\s+row_id`},
		{"two keys", []forma.OrderBy{{Attribute: "status"}, {Attribute: "scheduledStartAt", SortOrder: forma.SortOrderDesc}},
			`k0 ASC,\s+k1 DESC NULLS LAST,\s+row_id`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txr, mock := newMockTransactorOver(t, nil, "visit")
			mock.ExpectBeginTx(readCommittedTx)
			expectCandidateSelect(mock, tc.orderBy)
			mock.ExpectCommit()
			err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
				req := scheduledVisits()
				req.Sort = tc.sort
				result, err := tx.DeleteWhere(ctx, req)
				require.NoError(t, err)
				assert.Empty(t, result.RowIDs)
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Every predicate mutation selects on the transaction, on a manager wired
// with a federated engine: the engine is never called.
func TestMutateWhereNeverCallsTheFederatedEngine(t *testing.T) {
	engine := &countingEngine{}
	txr, mock := newMockTransactorOver(t, engine, "visit")
	mock.ExpectBeginTx(readCommittedTx)
	expectCandidateSelect(mock, `row_id`)
	expectCandidateSelect(mock, `row_id`)
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		req := scheduledVisits()
		req.Updates = map[string]any{"status": "visited"}
		if _, err := tx.UpdateWhere(ctx, req); err != nil {
			return err
		}
		_, err := tx.DeleteWhere(ctx, scheduledVisits())
		return err
	})
	require.NoError(t, err)
	assert.Zero(t, engine.calls)
	require.NoError(t, mock.ExpectationsWereMet())
}
