package internal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #457 pins: the update path must take the same (schema_id, row_id) advisory
// lock create and delete take, and read its merge base INSIDE the write
// transaction — the lock first, so a waiter re-reads post-lock state.

func TestMergePersistentRecordLocksThenReadsInsideTransaction(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("mock_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 11, ValueType: forma.ValueTypeText},
	}))
	repo := NewDBPersistentRecordRepository(mock, mc)
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	// What the in-transaction read returns: the merge base.
	baseValues := singleRowValues(map[string]any{
		"ltbase_schema_id":  int64(1),
		"ltbase_row_id":     rowID.String(),
		"ltbase_created_at": int64(111),
		"ltbase_updated_at": int64(222),
		"text_01":           "base",
	}, `[]`)

	text := "bar"
	merged := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		CreatedAt: 111,
		TextItems: map[string]string{"text_01": "merged"},
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "", ValueText: &text},
		},
	}

	expected := *merged
	expected.UpdatedAt = fixed.UnixMilli()
	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, &expected)
	require.NoError(t, err)
	_, eavArgs, err := buildAttributeValuesClause(merged.OtherAttributes)
	require.NoError(t, err)

	effectiveMillis := fixed.UnixMilli() + 5

	// READ COMMITTED is load-bearing, not incidental: a snapshot isolation
	// level would freeze this transaction's view at BEGIN — before the lock
	// below was granted — and hand the merge the stale base #457 is about.
	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`SELECT .* FROM "entity_main" m\s+LEFT JOIN LATERAL`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()).AddRow(baseValues...))
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(effectiveMillis))
	mock.ExpectExec(`^DELETE FROM "eav_table" WHERE schema_id = \$1 AND row_id = \$2 AND attr_id = ANY\(\$3\)$`).
		WithArgs(int16(1), rowID, []int16{11}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^INSERT INTO "eav_table"`).
		WithArgs(eavArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID, int64(0), effectiveMillis, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	var seen *model.PersistentRecord
	stored, err := repo.MergePersistentRecord(ctx, tables, 1, rowID,
		func(_ context.Context, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			seen = existing
			return merged, nil
		})
	require.NoError(t, err)

	require.NotNil(t, seen, "merge must receive the row read inside the transaction")
	assert.Equal(t, map[string]string{"text_01": "base"}, seen.TextItems)
	assert.Equal(t, int64(222), seen.UpdatedAt)
	require.NotNil(t, stored)
	assert.Equal(t, effectiveMillis, stored.UpdatedAt,
		"the stored record must carry the effective version PG computed (#274)")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMergePersistentRecordWhenRowMissingHandsNilAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`SELECT .* FROM "entity_main" m`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()))
	// No UPDATE, no EAV write, no changelog upsert — the transaction rolls back.
	mock.ExpectRollback()

	called := false
	stored, err := repo.MergePersistentRecord(ctx, tables, 1, rowID,
		func(_ context.Context, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			called = true
			assert.Nil(t, existing, "an absent row must reach merge as nil")
			return nil, forma.NotFoundf("entity not found: %s/%s", "mock_schema", rowID)
		})
	require.Error(t, err)
	require.True(t, called)
	assert.Nil(t, stored)
	require.ErrorIs(t, err, forma.ErrNotFound)
	// The repository adds its row context to the log line without touching the
	// published body: the wrap is plain, so the carrier's message still resolves.
	assert.ErrorContains(t, err, "merge record for "+rowID.String())
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "merge callback's published message must survive the repository wrap")
	assert.Equal(t, "entity not found: mock_schema/"+rowID.String(), msg)

	require.NoError(t, mock.ExpectationsWereMet())
}
