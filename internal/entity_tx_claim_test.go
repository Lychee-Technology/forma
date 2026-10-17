package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Claim (#653) against a pgxmock pool: what a request refuses before any
// SQL, the over-fetched candidate select, and that the federated engine is
// never called. The mock fails on any statement a test did not queue.

func scheduledVisitClaim() *forma.ClaimRequest {
	return &forma.ClaimRequest{SchemaName: "visit", Limit: 5,
		Condition: &forma.KvCondition{Attr: "status", Value: "equals:scheduled"},
		Updates:   map[string]any{"status": "visited"}}
}

func TestClaimRefusesAnInvalidRequestBeforeAnySQL(t *testing.T) {
	claim := func(edit func(*forma.ClaimRequest)) *forma.ClaimRequest {
		req := scheduledVisitClaim()
		edit(req)
		return req
	}
	cases := []struct {
		name    string
		req     *forma.ClaimRequest
		message string
	}{
		{"a nil request", nil, "Claim request cannot be nil"},
		{"no schema", claim(func(r *forma.ClaimRequest) { r.SchemaName = "" }), "Claim needs a schema name"},
		{"an unregistered schema", claim(func(r *forma.ClaimRequest) { r.SchemaName = "no_such_schema" }), "not registered"},
		{"a nil condition", claim(func(r *forma.ClaimRequest) { r.Condition = nil }), "needs a condition"},
		{"a condition on an unknown attribute", claim(func(r *forma.ClaimRequest) {
			r.Condition = &forma.KvCondition{Attr: "no_such_attr", Value: "equals:1"}
		}), "condition tests attribute 'no_such_attr'"},
		{"an empty and", claim(func(r *forma.ClaimRequest) { r.Condition = &forma.CompositeCondition{Logic: forma.LogicAnd} }), "empty and group"},
		{"a malformed operand", claim(func(r *forma.ClaimRequest) {
			r.Condition = &forma.KvCondition{Attr: "scheduledStartAt", Value: "gt:not-a-date"}
		}), "scheduledStartAt"},
		{"a zero limit", claim(func(r *forma.ClaimRequest) { r.Limit = 0 }), "Claim limit 0 is out of range: it must be 1 to 1000"},
		{"a negative limit", claim(func(r *forma.ClaimRequest) { r.Limit = -1 }), "Claim limit -1 is out of range"},
		{"a limit past the maximum", claim(func(r *forma.ClaimRequest) { r.Limit = 1001 }), "Claim limit 1001 is out of range"},
		{"an unknown sort attribute", claim(func(r *forma.ClaimRequest) {
			r.Sort = []forma.OrderBy{{Attribute: "no_such_attr"}}
		}), "cannot sort by unknown attribute"},
		{"neither Updates nor UpdateOps", claim(func(r *forma.ClaimRequest) { r.Updates = nil }), "needs Updates or UpdateOps"},
		{"empty Updates", claim(func(r *forma.ClaimRequest) { r.Updates = map[string]any{} }), "needs Updates or UpdateOps"},
		{"an attribute in both Updates and UpdateOps", claim(func(r *forma.ClaimRequest) {
			r.Updates = map[string]any{"updatedAt": "2026-10-01T00:00:00Z"}
			r.UpdateOps = []forma.UpdateOp{forma.ServerTime("updatedAt", 0)}
		}), "updatedAt"},
		{"one attribute with two operators", claim(func(r *forma.ClaimRequest) {
			r.UpdateOps = []forma.UpdateOp{forma.ServerTime("updatedAt", 0), forma.ServerTime("updatedAt", 0)}
		}), "more than one update operator"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txr, mock := newMockTransactorOver(t, nil, "visit")
			mock.ExpectBeginTx(readCommittedTx)
			mock.ExpectCommit()
			err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
				_, err := tx.Claim(ctx, tc.req)
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				require.ErrorContains(t, err, tc.message)
				return nil
			})
			require.NoError(t, err, "a refused request leaves the Tx able to commit")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// A claim selects twice its limit of candidates, ordered by Sort with the row
// ID last, on the transaction: a manager wired with a federated engine never
// calls it. No candidate means an empty result, not an error, and no lock.
func TestClaimSelectsOverFetchedCandidatesOnTheTx(t *testing.T) {
	engine := &countingEngine{}
	txr, mock := newMockTransactorOver(t, engine, "visit")
	mock.ExpectBeginTx(readCommittedTx)
	mock.ExpectQuery(`(?s)^\s*WITH anchor AS .*ORDER BY\s+k0 ASC,\s+row_id\s+LIMIT`).
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), 6, 0).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_row_id"}))
	mock.ExpectCommit()

	err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
		req := scheduledVisitClaim()
		req.Limit = 3
		req.Sort = []forma.OrderBy{{Attribute: "scheduledStartAt"}}
		result, err := tx.Claim(ctx, req)
		require.NoError(t, err)
		assert.Empty(t, result.Records)
		assert.NotNil(t, result.Records)
		return nil
	})
	require.NoError(t, err)
	assert.Zero(t, engine.calls)
	require.NoError(t, mock.ExpectationsWereMet())
}
