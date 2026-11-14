package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
)

const (
	labelQueryHotKeys = "internal/postgres_hot_leg.go:DBPersistentRecordRepository.QueryHotKeys"
	labelReadHotRows  = "internal/postgres_hot_leg.go:DBPersistentRecordRepository.ReadHotRows"
)

// TestDSQLCorpusHotLegReads exports the pgx hot leg's two reads in the pass's
// snapshot (design.md §6.1, pgx-hot-leg-plan.md step 4): the hot-keys query,
// with a condition over a bound column and an eav_data attribute and a sort
// key of each storage form, and the page read. The snapshot's BEGIN and
// COMMIT are internal/federated's TestDSQLCorpusChangeLogSnapshot's.
func TestDSQLCorpusHotLegReads(t *testing.T) {
	ctx := context.Background()
	mc := schemameta.NewMetadataCache()
	if err := mc.RegisterSchema(corpusKeyedSchema, corpusSchemaID, forma.SchemaAttributeCache{
		"code": {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}},
		"note": {AttributeID: 2, ValueType: forma.ValueTypeText},
	}); err != nil {
		t.Fatalf("register schema: %v", err)
	}
	repo := NewDBPersistentRecordRepository(nil, mc)

	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.MatchPgx)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(ctx) })
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(5)...).WillReturnRows(
		pgxmock.NewRows([]string{"ltbase_row_id", "ver_ts", "text_01", "note", "ltbase_created_at"}).
			AddRow(corpusRowA, corpusNowMillis, "a", []any{"x"}, corpusPriorVersion))
	// No row comes back: the page read's scan targets are the whole
	// entity_main projection, and the statement is what the corpus needs.
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(pgxmock.NewRows([]string{"ltbase_row_id"}))
	tx, err := mock.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	keys, err := repo.QueryHotKeys(ctx, tx, model.HotKeysQuery{
		Tables:        corpusTables,
		SchemaID:      corpusSchemaID,
		GraceCutoffMs: corpusNowMillis,
		Condition: &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "code", Value: "gt:B"},
			&forma.KvCondition{Attr: "note", Value: "equals:x"},
		}},
		SortColumns: []string{"code", "note", "created_at", "row_id"},
	})
	if err != nil {
		t.Fatalf("QueryHotKeys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("QueryHotKeys answered %d keys, want 1", len(keys))
	}
	if _, err := repo.ReadHotRows(ctx, tx, corpusTables, corpusSchemaID, []uuid.UUID{corpusRowA}); err == nil {
		t.Fatal("ReadHotRows answered a row the mock does not hold")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the hot leg's reads sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-hot-leg-reads", Engine: forma.EngineAuroraDSQL,
		Statements: rec.Take(t, labelQueryHotKeys, labelReadHotRows)})
}
