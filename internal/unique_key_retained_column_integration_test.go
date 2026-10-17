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

// Real-PostgreSQL tests of an update whose merged document drops a key
// component (#651). Report-only update validation, the default, admits
// {"tenant": {}}: an object where ukaccount declares a string, for which the
// transformer stores nothing. The main-table UPDATE assigns only the columns
// the merged record sets, so the column-bound tenant keeps its stored value,
// and the row must keep that value's entry. The EAV write replaces the row's
// attributes, so an EAV component dropped the same way is gone, and its
// entry with it.

// ukDropAttribute replaces attr with an empty object.
func ukDropAttribute(attr string) map[string]any {
	return map[string]any{attr: map[string]any{}}
}

// ukBatchUpdateAnswer runs updates as one BatchUpdate and answers the
// attributes of the one record it returns.
func (e *txEnv) ukBatchUpdateAnswer(t *testing.T, atomic bool, rowID uuid.UUID, updates map[string]any) map[string]any {
	t.Helper()
	op := *txUpdateOp(ukAccount, rowID, updates)
	result, err := e.manager.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: atomic, Operations: []forma.EntityOperation{op}})
	require.NoError(t, err)
	require.Empty(t, result.Failed)
	require.Len(t, result.Successful, 1)
	return result.Successful[0].Attributes
}

func TestUniqueKeyUpdateKeepsTheEntryOfABoundValueItLeavesStored(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	surfaces := []struct {
		name string
		// update applies ukDropAttribute("tenant") to the row and answers
		// the attributes the surface returns for it.
		update func(t *testing.T, rowID uuid.UUID, note string) map[string]any
	}{
		{"Update", func(t *testing.T, rowID uuid.UUID, _ string) map[string]any {
			record, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, ukDropAttribute("tenant")))
			require.NoError(t, err)
			return record.Attributes
		}},
		{"atomic BatchUpdate", func(t *testing.T, rowID uuid.UUID, _ string) map[string]any {
			return env.ukBatchUpdateAnswer(t, true, rowID, ukDropAttribute("tenant"))
		}},
		{"best-effort BatchUpdate", func(t *testing.T, rowID uuid.UUID, _ string) map[string]any {
			return env.ukBatchUpdateAnswer(t, false, rowID, ukDropAttribute("tenant"))
		}},
		{"Tx.Update", func(t *testing.T, rowID uuid.UUID, _ string) map[string]any {
			var record *forma.DataRecord
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				var err error
				record, err = tx.Update(ctx, txUpdateOp(ukAccount, rowID, ukDropAttribute("tenant")))
				return err
			}))
			return record.Attributes
		}},
		{"Tx.UpdateWhere", func(t *testing.T, _ uuid.UUID, note string) map[string]any {
			var result *forma.MutateWhereResult
			require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
				req := ukHandleWhere(note)
				req.Updates, req.Returning = ukDropAttribute("tenant"), true
				var err error
				result, err = tx.UpdateWhere(ctx, req)
				return err
			}))
			require.Len(t, result.Records, 1)
			return result.Records[0].Attributes
		}},
	}
	for i, surface := range surfaces {
		t.Run(surface.name, func(t *testing.T) {
			tenant, note := fmt.Sprintf("tenant-%d", i), fmt.Sprintf("note-%d", i)
			data := ukAccountData(tenant, "same@x", "")
			data["note"] = note
			created, err := env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, data))
			require.NoError(t, err)

			answered := surface.update(t, created.RowID, note)
			assert.Equal(t, tenant, answered["tenant"], "the update answers the value the row keeps")
			stored, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &created.RowID})
			require.NoError(t, err)
			assert.Equal(t, tenant, stored.Attributes["tenant"], "the bound column keeps its stored value")
			env.assertKeyTableMatchesRows(t)

			_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData(tenant, "same@x", "")))
			requireKeyConflict(t, err, ukAccount, "tenant_email", "same@x")
		})
	}
}

func TestUniqueKeyUpdateReleasesTheEntryOfAnEAVValueItDrops(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	rowID := env.mustCreateAccount(t, "t1", "same@x", "")

	_, err := env.manager.Update(env.ctx, txUpdateOp(ukAccount, rowID, ukDropAttribute("email")))
	require.NoError(t, err)
	stored, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &rowID})
	require.NoError(t, err)
	assert.NotContains(t, stored.Attributes, "email", "the EAV write removed the dropped value")
	assert.Empty(t, env.keyEntriesOfRow(t, rowID))
	env.assertKeyTableMatchesRows(t)

	env.mustCreateAccount(t, "t1", "same@x", "")
	env.assertKeyTableMatchesRows(t)
}
