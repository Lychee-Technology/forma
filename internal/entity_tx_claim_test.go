package internal

import (
	"context"
	"strings"
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

func callsByUserClaim() *forma.ClaimRequest {
	return &forma.ClaimRequest{SchemaName: "activity", Limit: 5,
		Condition: &forma.KvCondition{Attr: "type", Value: "equals:call"},
		Updates:   map[string]any{"type": "called"},
		Group: &forma.ClaimGroup{Key: []string{"userId"}, Order: []forma.OrderBy{{Attribute: "at"}},
			Live: &forma.KvCondition{Attr: "direction", Value: "equals:in"}}}
}

// A Group is validated with the rest of the request: every refusal names
// the attribute or the part at fault and leaves the Tx usable.
func TestClaimRefusesAnInvalidGroupBeforeAnySQL(t *testing.T) {
	group := func(edit func(*forma.ClaimGroup)) *forma.ClaimRequest {
		req := callsByUserClaim()
		edit(req.Group)
		return req
	}
	claim := func(edit func(*forma.ClaimRequest)) *forma.ClaimRequest {
		req := callsByUserClaim()
		edit(req)
		return req
	}
	cases := []struct {
		name    string
		req     *forma.ClaimRequest
		message string
	}{
		{"no key", group(func(g *forma.ClaimGroup) { g.Key = nil }), "Claim group on activity has 0 key attributes: it must have 1 to 4"},
		{"five keys", group(func(g *forma.ClaimGroup) { g.Key = []string{"userId", "direction", "type", "summary", "id"} }), "has 5 key attributes"},
		{"an unknown key attribute", group(func(g *forma.ClaimGroup) { g.Key = []string{"nope"} }), "Claim group key names attribute 'nope', which schema 'activity' does not define"},
		{"an EAV key attribute", group(func(g *forma.ClaimGroup) { g.Key = []string{"userId", "leadId"} }),
			"Claim group key uses attribute 'leadId' of schema 'activity', which is stored in EAV"},
		{"a repeated key attribute", group(func(g *forma.ClaimGroup) { g.Key = []string{"userId", "userId"} }), "lists attribute 'userId' twice"},
		{"no order", group(func(g *forma.ClaimGroup) { g.Order = nil }), "Claim group on activity needs an order"},
		{"an unknown order attribute", group(func(g *forma.ClaimGroup) { g.Order = []forma.OrderBy{{Attribute: "nope"}} }), "cannot sort by unknown attribute 'nope'"},
		{"an EAV order attribute", group(func(g *forma.ClaimGroup) { g.Order = []forma.OrderBy{{Attribute: "at"}, {Attribute: "nextFollowUpAt"}} }),
			"Claim group order uses attribute 'nextFollowUpAt' of schema 'activity', which is stored in EAV"},
		{"an invalid order direction", group(func(g *forma.ClaimGroup) { g.Order = []forma.OrderBy{{Attribute: "at", SortOrder: "up"}} }), "invalid sort_order 'up'"},
		{"no live condition", group(func(g *forma.ClaimGroup) { g.Live = nil }), "Claim group on activity needs a live condition"},
		{"a live condition on an unknown attribute", group(func(g *forma.ClaimGroup) { g.Live = &forma.KvCondition{Attr: "nope", Value: "equals:x"} }),
			"group live condition tests attribute 'nope'"},
		{"a live condition on an EAV attribute", group(func(g *forma.ClaimGroup) {
			g.Live = &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "direction", Value: "equals:in"}, &forma.KvCondition{Attr: "leadId", Value: "equals:l1"}}}
		}), "Claim group live condition uses attribute 'leadId' of schema 'activity', which is stored in EAV"},
		{"an empty group in the live condition", group(func(g *forma.ClaimGroup) {
			g.Live = &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{&forma.CompositeCondition{Logic: forma.LogicOr}}}
		}), "empty or group in its live condition"},
		{"a malformed live operand", group(func(g *forma.ClaimGroup) { g.Live = &forma.KvCondition{Attr: "at", Value: "lt:not-a-date"} }),
			"not-a-date"},
		{"a malformed condition operand", claim(func(r *forma.ClaimRequest) { r.Condition = &forma.KvCondition{Attr: "at", Value: "lt:not-a-date"} }),
			"not-a-date"},
		{"a mutation that writes the group order", claim(func(r *forma.ClaimRequest) { r.Updates["at"] = "2026-01-02T00:00:00Z" }),
			"Claim on activity writes attribute 'at' of its group order: the claim's write would move a claimed row within its group"},
		{"an operator that writes the group order", claim(func(r *forma.ClaimRequest) { r.UpdateOps = []forma.UpdateOp{forma.ServerTime("at", 0)} }),
			"writes attribute 'at' of its group order"},
		{"a mutation that writes the group key", claim(func(r *forma.ClaimRequest) { r.Updates["userId"] = "u2" }),
			"Claim on activity writes attribute 'userId' of its group key"},
	}
	// The published message names the condition at fault.
	published := map[string]string{
		"a malformed live operand":      "Claim group live condition on activity: ",
		"a malformed condition operand": "Claim condition on activity: ",
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txr, mock := newMockTransactorOver(t, nil, "activity")
			mock.ExpectBeginTx(readCommittedTx)
			mock.ExpectCommit()
			err := txr.InTx(context.Background(), func(ctx context.Context, tx forma.Tx) error {
				_, err := tx.Claim(ctx, tc.req)
				require.ErrorIs(t, err, forma.ErrInvalidInput)
				require.ErrorContains(t, err, tc.message)
				if prefix, ok := published[tc.name]; ok {
					message, ok := forma.ResolvePublicMessage(err)
					require.True(t, ok)
					require.True(t, strings.HasPrefix(message, prefix), "published %q names the condition at fault", message)
				}
				return nil
			})
			require.NoError(t, err, "a refused request leaves the Tx able to commit")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// A key or order attribute bound to a column Forma stamps on every update
// would move a claimed row within its group on the claim's own write.
func TestClaimGroupRefusesAttributesTheServerRewrites(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"queue":   {AttributeName: "queue", ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"}},
		"at":      {AttributeName: "at", ValueType: forma.ValueTypeDate, ColumnBinding: &forma.MainColumnBinding{ColumnName: "bigint_01"}},
		"touched": {AttributeName: "touched", ValueType: forma.ValueTypeDate, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUpdatedAt}},
		"editor":  {AttributeName: "editor", ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUpdatedBy}},
		"state":   {AttributeName: "state", ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_02"}},
	}
	req := func(key, order string) *forma.ClaimRequest {
		return &forma.ClaimRequest{SchemaName: "jobs", Updates: map[string]any{"state": "held"},
			Group: &forma.ClaimGroup{Key: []string{key}, Order: []forma.OrderBy{{Attribute: order}},
				Live: &forma.KvCondition{Attr: "state", Value: "equals:ready"}}}
	}
	_, err := planClaimGroup(req("queue", "touched"), cache)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, "Claim group order uses attribute 'touched' of schema 'jobs', which is bound to ltbase_updated_at")
	_, err = planClaimGroup(req("editor", "at"), cache)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, "Claim group key uses attribute 'editor' of schema 'jobs', which is bound to ltbase_updated_by")
	head, err := planClaimGroup(req("queue", "at"), cache)
	require.NoError(t, err)
	require.Equal(t, []string{"text_01"}, head.KeyColumns)
}
