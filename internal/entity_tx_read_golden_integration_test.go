package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tx reads over the shipped schemas (#650): relation enrichment runs on the
// transaction, and on committed data Tx.Query answers what EntityManager.Query
// answers.

func txLead(contactName string) map[string]any {
	return map[string]any{
		"id": uuid.NewString(), "tenantId": "tenant-tx", "ownerUserId": "owner-tx",
		"pipeline": "buy", "stage": "new", "status": "open",
		"contact":   map[string]any{"isAnonymous": false, "name": contactName},
		"createdAt": "2026-10-01T00:00:00Z", "updatedAt": "2026-10-01T00:00:00Z",
	}
}

// A visit's contactSnapshot is enriched from its lead. Both rows are written
// in the Tx and never committed, so a parent lookup on the pool would find no
// lead: the snapshot can only come from a lookup on the transaction.
func TestTxReadsEnrichRelationsOnTheTransaction(t *testing.T) {
	env := setupTxEnv(t)
	bail := errors.New("roll back")
	err := env.inTx(func(ctx context.Context, tx forma.Tx) error {
		lead, err := tx.Create(ctx, txCreateOp("lead", txLead("Ada")))
		require.NoError(t, err)
		visitData := txVisit("scheduled")
		visitData["leadId"] = lead.Attributes["id"]
		visit, err := tx.Create(ctx, txCreateOp(txSchemaB, visitData))
		require.NoError(t, err)

		got, err := tx.Get(ctx, &forma.QueryRequest{SchemaName: txSchemaB, RowID: &visit.RowID})
		require.NoError(t, err)
		require.Equal(t, lead.Attributes["contact"], got.Attributes["contactSnapshot"],
			"Get enriched the visit from the uncommitted lead")

		listed, err := tx.Query(ctx, &forma.QueryRequest{SchemaName: txSchemaB, Page: 1, ItemsPerPage: 10})
		require.NoError(t, err)
		require.Len(t, listed.Data, 1)
		require.Equal(t, lead.Attributes["contact"], listed.Data[0].Attributes["contactSnapshot"],
			"Query enriched the visit from the uncommitted lead")
		return bail
	})
	require.ErrorIs(t, err, bail)
	assert.Zero(t, env.schemaRowCount(t, "lead"))
	assert.Zero(t, env.schemaRowCount(t, txSchemaB))
}

// goldenQueries are the requests the golden comparison runs. Each call builds
// a fresh request, because Query normalizes the one it is given.
func goldenQueries() map[string]func() *forma.QueryRequest {
	activity := func(cond forma.Condition) *forma.QueryRequest {
		return &forma.QueryRequest{SchemaName: txSchemaA, Condition: cond, Page: 1, ItemsPerPage: 50}
	}
	return map[string]func() *forma.QueryRequest{
		"every row":          func() *forma.QueryRequest { return activity(nil) },
		"main-column equals": func() *forma.QueryRequest { return activity(kv("type", "equals:call")) },
		"EAV in list":        func() *forma.QueryRequest { return activity(kv("summary", `in:["s0","s3","s5"]`)) },
		"and or composite": func() *forma.QueryRequest {
			return activity(and(kv("direction", "equals:inbound"), or(kv("type", "equals:email"), kv("summary", "starts_with:s1"))))
		},
		"datetime with $now": func() *forma.QueryRequest { return activity(kv("at", "lte:$now")) },
		"is_null":            func() *forma.QueryRequest { return activity(kv("nextFollowUpAt", "is_null:true")) },
		"sorted page two": func() *forma.QueryRequest {
			req := activity(nil)
			req.Sort = []forma.OrderBy{{Attribute: "summary", SortOrder: forma.SortOrderDesc}}
			req.Page, req.ItemsPerPage = 2, 3
			return req
		},
		"mixed sort": func() *forma.QueryRequest {
			req := activity(kv("type", "in:[\"call\",\"email\"]"))
			req.SortBy, req.SortOrder = []string{"direction", "summary"}, forma.SortOrderAsc
			return req
		},
		"projection": func() *forma.QueryRequest {
			req := activity(nil)
			req.Attrs = []string{"type", "summary"}
			return req
		},
		"past the last page": func() *forma.QueryRequest {
			req := activity(nil)
			req.Page, req.ItemsPerPage = 9, 5
			return req
		},
		"enriched visits": func() *forma.QueryRequest {
			return &forma.QueryRequest{SchemaName: txSchemaB, Page: 1, ItemsPerPage: 50}
		},
	}
}

func TestTxQueryMatchesEntityManagerQueryOnCommittedData(t *testing.T) {
	env := setupTxEnv(t)
	types, directions := []string{"call", "email", "note"}, []string{"inbound", "outbound"}
	for i := range 12 {
		data := txActivity("s" + string(rune('0'+i%10)))
		data["type"], data["direction"] = types[i%3], directions[i%2]
		if i%4 == 0 {
			data["nextFollowUpAt"] = "2026-10-05T10:00:00Z"
		}
		env.mustCreate(t, txSchemaA, data)
	}
	lead, err := env.manager.Create(env.ctx, txCreateOp("lead", txLead("Grace")))
	require.NoError(t, err)
	for range 3 {
		visit := txVisit("scheduled")
		visit["leadId"] = lead.Attributes["id"]
		env.mustCreate(t, txSchemaB, visit)
	}

	for name, build := range goldenQueries() {
		t.Run(name, func(t *testing.T) {
			want, err := env.manager.Query(env.ctx, build())
			require.NoError(t, err)
			var got *forma.QueryResult
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				got, err = tx.Query(ctx, build())
				return err
			}))
			want.ExecutionTime, got.ExecutionTime = 0, 0
			assert.Equal(t, want, got)
			if name != "past the last page" {
				assert.NotEmpty(t, got.Data, "the comparison is not between two empty pages")
			}
			if name == "enriched visits" {
				require.Len(t, got.Data, 3)
				assert.NotNil(t, got.Data[0].Attributes["contactSnapshot"], "the comparison covers enrichment")
			}
		})
	}
}
