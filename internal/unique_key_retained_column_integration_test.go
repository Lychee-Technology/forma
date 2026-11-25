package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL tests of an update that gives a key component an empty
// object (#651). Report-only update validation, the default, used to admit
// {"tenant": {}}: an object where ukaccount declares a string, for which the
// transformer stored nothing, so the column-bound tenant kept its value and
// the EAV email lost its value and its entry. Since #924 every surface
// refuses it before writing (scalar_shape.go), and the row keeps both values
// and both entries.

// ukDropAttribute replaces attr with an empty object.
func ukDropAttribute(attr string) map[string]any {
	return map[string]any{attr: map[string]any{}}
}

// ukBatchUpdateRefusal runs updates as one BatchUpdate and answers its
// refusal: the atomic batch's error, or the best-effort batch's one failure.
func (e *txEnv) ukBatchUpdateRefusal(t *testing.T, atomic bool, rowID uuid.UUID, updates map[string]any) error {
	t.Helper()
	op := *txUpdateOp(ukAccount, rowID, updates)
	result, err := e.manager.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{op}})
	if atomic {
		return err
	}
	require.NoError(t, err)
	require.Empty(t, result.Successful)
	require.Len(t, result.Failed, 1)
	return forma.InvalidInputf("%s", result.Failed[0].Error)
}

func TestUniqueKeyUpdateKeepsTheEntryOfABoundValueItLeavesStored(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	surfaces := []struct {
		name string
		// update applies ukDropAttribute("tenant") to the row and answers
		// the surface's refusal.
		update func(t *testing.T, rowID uuid.UUID, note string) error
	}{
		{"Update", func(t *testing.T, rowID uuid.UUID, _ string) error {
			_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, ukDropAttribute("tenant")))
			return err
		}},
		{"atomic BatchUpdate", func(t *testing.T, rowID uuid.UUID, _ string) error {
			return env.ukBatchUpdateRefusal(t, true, rowID, ukDropAttribute("tenant"))
		}},
		{"best-effort BatchUpdate", func(t *testing.T, rowID uuid.UUID, _ string) error {
			return env.ukBatchUpdateRefusal(t, false, rowID, ukDropAttribute("tenant"))
		}},
		{"Tx.Update", func(t *testing.T, rowID uuid.UUID, _ string) error {
			return env.inTx(func(ctx context.Context, tx forma.Tx) error {
				_, err := tx.Update(ctx, txUpdateOp(ukAccount, rowID, ukDropAttribute("tenant")))
				return err
			})
		}},
		{"Tx.UpdateWhere", func(t *testing.T, _ uuid.UUID, note string) error {
			return env.inTx(func(ctx context.Context, tx forma.Tx) error {
				req := ukHandleWhere(note)
				req.Updates, req.Returning = ukDropAttribute("tenant"), true
				_, err := tx.UpdateWhere(ctx, req)
				return err
			})
		}},
	}
	for i, surface := range surfaces {
		t.Run(surface.name, func(t *testing.T) {
			tenant, note := fmt.Sprintf("tenant-%d", i), fmt.Sprintf("note-%d", i)
			data := ukAccountData(tenant, "same@x", "")
			data["note"] = note
			created, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
			require.NoError(t, err)

			err = surface.update(t, created.RowID, note)
			requireScalarShapeRefusal(t, err, "tenant", forma.ValueTypeText, surface.name)
			stored, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &created.RowID})
			require.NoError(t, err)
			assert.Equal(t, tenant, stored.Attributes["tenant"], "the bound column keeps its stored value")
			env.assertKeyTableMatchesRows(t)

			_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData(tenant, "same@x", "")))
			requireKeyConflict(t, err, ukAccount, "tenant_email", "same@x")
		})
	}
}

func TestUniqueKeyUpdateKeepsTheEntryOfAnEAVValueItWouldDrop(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID := env.mustCreateAccount(t, "t1", "same@x", "")
	held := env.keyEntriesOfRow(t, rowID)
	require.NotEmpty(t, held)

	_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, ukDropAttribute("email")))
	requireScalarShapeRefusal(t, err, "email", forma.ValueTypeText, "Update")
	stored, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &rowID})
	require.NoError(t, err)
	assert.Equal(t, "same@x", stored.Attributes["email"], "the EAV value is kept")
	assert.Equal(t, held, env.keyEntriesOfRow(t, rowID))
	env.assertKeyTableMatchesRows(t)

	_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData("t1", "same@x", "")))
	requireKeyConflict(t, err, ukAccount, "tenant_email", "same@x")
}
