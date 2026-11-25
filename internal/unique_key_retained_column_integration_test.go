package internal

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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
//
// The repository's invariant beneath that refusal still holds for any merged
// record that leaves a key component out, whatever produced it, and the
// tests at the end pin it at the merge boundary itself: the main-table UPDATE
// assigns only the columns the merged record sets, so an omitted bound
// column keeps its stored value and the row keeps that value's entry
// (retainStoredColumns); the EAV write replaces the row's attributes, so an
// omitted EAV component is gone, and its entry is released with it.

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

// ukMergeCores runs merge on rowID through each of the repository's merge
// cores, MergePersistentRecord and BatchMergePersistentRecords, on a
// repository enforcing the environment's unique keys.
func (e *txEnv) ukMergeCores(t *testing.T) map[string]func(rowID uuid.UUID, merge model.PersistentRecordMerge) error {
	t.Helper()
	repo := NewDBPersistentRecordRepository(e.postgresPool, e.metadata, WithUniqueKeyCatalog(e.ukCatalog(t)))
	schemaID := ukSchemaIDs[ukAccount]
	return map[string]func(uuid.UUID, model.PersistentRecordMerge) error{
		"MergePersistentRecord": func(rowID uuid.UUID, merge model.PersistentRecordMerge) error {
			_, err := repo.MergePersistentRecord(e.ctx, e.tables, schemaID, rowID, model.RowGuard{}, merge)
			return err
		},
		"BatchMergePersistentRecords": func(rowID uuid.UUID, merge model.PersistentRecordMerge) error {
			keys := []model.PersistentRecordKey{{SchemaID: schemaID, RowID: rowID}}
			_, err := repo.BatchMergePersistentRecords(e.ctx, e.tables, keys,
				func(ctx context.Context, _ int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
					return merge(ctx, existing, nil)
				})
			return err
		},
	}
}

// ukMergeOmitting answers a merge that rewrites note and stores the row
// without the main column column, or without the EAV records of attrID: a
// copy, so the merge base the repository retains from stays as read.
func ukMergeOmitting(column string, attrID int16, note string) model.PersistentRecordMerge {
	return func(_ context.Context, existing *model.PersistentRecord, _ model.TxClock) (*model.PersistentRecord, error) {
		if existing == nil {
			return nil, fmt.Errorf("the row is missing")
		}
		merged := *existing
		merged.TextItems = maps.Clone(existing.TextItems)
		delete(merged.TextItems, column)
		merged.OtherAttributes = slices.DeleteFunc(slices.Clone(existing.OtherAttributes), func(r model.EAVRecord) bool {
			return r.AttrID == attrID
		})
		for i := range merged.OtherAttributes {
			if merged.OtherAttributes[i].AttrID == ukNoteAttrID {
				merged.OtherAttributes[i].ValueText = &note
			}
		}
		return &merged, nil
	}
}

// The ukaccount attributes the merge tests omit (testdata/unique_keys).
const (
	ukTenantColumn       = "text_01"
	ukEmailAttrID  int16 = 2
	ukNoteAttrID   int16 = 5
)

// A merged record that leaves out the column-bound key component is stored
// with the column as it was, and the row keeps the component's entry: the
// merge succeeds, the row reads the stored tenant back, and a create of the
// same key still conflicts (#651).
func TestUniqueKeyMergeOmittingABoundColumnKeepsItsEntry(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for name, mergeCore := range env.ukMergeCores(t) {
		t.Run(name, func(t *testing.T) {
			tenant := "tenant-merge-" + name
			rowID := env.mustCreateAccount(t, tenant, "same@x", "")
			held := env.keyEntriesOfRow(t, rowID)
			require.NotEmpty(t, held)

			require.NoError(t, mergeCore(rowID, ukMergeOmitting(ukTenantColumn, 0, "merged")))
			stored, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &rowID})
			require.NoError(t, err)
			assert.Equal(t, "merged", stored.Attributes["note"], "the merge wrote")
			assert.Equal(t, tenant, stored.Attributes["tenant"], "the bound column keeps its stored value")
			assert.Equal(t, held, env.keyEntriesOfRow(t, rowID), "the row keeps the entry of the value it keeps")
			env.assertKeyTableMatchesRows(t)

			_, err = env.manager.Create(env.ctx, ukCreateOp(ukAccount, uuid.Nil, ukAccountData(tenant, "same@x", "")))
			requireKeyConflict(t, err, ukAccount, "tenant_email", "same@x")
		})
	}
}

// A merged record whose EAV set leaves out a key component stores the row
// without it, and the key's entry is released with it: the row reads back
// without email, holds no entry, and the key is free for another row.
func TestUniqueKeyMergeOmittingAnEAVComponentReleasesItsEntry(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for name, mergeCore := range env.ukMergeCores(t) {
		t.Run(name, func(t *testing.T) {
			tenant := "tenant-eav-" + name
			rowID := env.mustCreateAccount(t, tenant, "same@x", "")
			require.NotEmpty(t, env.keyEntriesOfRow(t, rowID))

			require.NoError(t, mergeCore(rowID, ukMergeOmitting("", ukEmailAttrID, "merged")))
			stored, err := env.manager.Get(env.ctx, &forma.QueryRequest{SchemaName: ukAccount, RowID: &rowID})
			require.NoError(t, err)
			assert.Equal(t, "merged", stored.Attributes["note"], "the merge wrote")
			assert.Equal(t, tenant, stored.Attributes["tenant"])
			assert.NotContains(t, stored.Attributes, "email", "the omitted EAV component is gone")
			assert.Empty(t, env.keyEntriesOfRow(t, rowID), "its entry is released")
			env.assertKeyTableMatchesRows(t)

			env.mustCreateAccount(t, tenant, "same@x", "")
			env.assertKeyTableMatchesRows(t)
		})
	}
}
