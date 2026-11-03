package cdc

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

const (
	originExportSnapshot  = "internal/cdc/export_pg_stage.go:stageInSnapshot"
	originExportChangeLog = "internal/cdc/export_sql_builder.go:buildChangeLogQuery"
	originExportMain      = "internal/cdc/export_sql_builder.go:buildMainEntityQuery"
	originExportEAV       = "internal/cdc/export_sql_builder.go:buildEAVQuery"
)

// corpusAttrCache is a representative schema: one attribute bound to an
// entity_main column, two stored in EAV.
var corpusAttrCache = forma.SchemaAttributeCache{
	"status": {AttributeName: "status", AttributeID: 10, ValueType: forma.ValueTypeText,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
	"score":     {AttributeName: "score", AttributeID: 11, ValueType: forma.ValueTypeInteger},
	"is_active": {AttributeName: "is_active", AttributeID: 12, ValueType: forma.ValueTypeBool},
}

// TestDSQLCorpusExport renders the PostgreSQL side of a CDC export for the
// DSQL lint gate (#740), as stageInSnapshot runs it: the BEGIN pgx writes
// for its REPEATABLE READ READ ONLY options, the stage reads in plan order,
// and the COMMIT. The delta export reads change_log, entity_main and
// eav_data; the base export reads live entity_main rows and eav_data. The
// projection and COPY that follow run in DuckDB, not on the database.
func TestDSQLCorpusExport(t *testing.T) {
	rows := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	delta, err := buildDeltaExportPlan(stagedTransport, "s3://bucket/delta/100/_tmp/x.parquet", CDCConfig{}, 100, 1_700_000_000_000, rows, corpusAttrCache)
	if err != nil {
		t.Fatalf("build delta export plan: %v", err)
	}
	base, err := buildBaseExportPlan(stagedTransport, "s3://bucket/base/100/_tmp/x.parquet", CDCConfig{}, 100, rows, corpusAttrCache)
	if err != nil {
		t.Fatalf("build base export plan: %v", err)
	}
	stageOrigins := map[string]string{"cl": originExportChangeLog, "m": originExportMain, "e_rows": originExportEAV}
	for name, plan := range map[string]exportSQLPlan{"internal-cdc-export-delta": delta, "internal-cdc-export-base": base} {
		begin, commit := snapshotBoundary(t)
		statements := []corpus.Statement{begin}
		for _, stage := range plan.stages {
			origin, ok := stageOrigins[stage.alias]
			if !ok {
				t.Fatalf("export stage %q has no origin: name its query builder so the DSQL lint gate can trace it", stage.alias)
			}
			statements = append(statements, corpus.Statement{Origin: origin, SQL: stage.query})
		}
		corpus.Export(t, corpus.Unit{Name: name, Statements: append(statements, commit)})
	}
}

// snapshotBoundary records the BEGIN and COMMIT stageInSnapshot sends
// around an export's reads, from a real pgx connection.
func snapshotBoundary(t *testing.T) (begin, commit corpus.Statement) {
	t.Helper()
	var rec corpus.Recorder
	if _, err := stageInSnapshot(context.Background(), corpus.WireConn(t, &rec), nil, nil, nil); err != nil {
		t.Fatalf("stageInSnapshot: %v", err)
	}
	statements := rec.Take(t, originExportSnapshot, originExportSnapshot)
	return statements[0], statements[1]
}

// TestDSQLCorpusFlushReads renders the change_log and entity_main reads
// cdc-flush and cdc-init plan their batches with.
func TestDSQLCorpusFlushReads(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	row := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"count", "min"}).AddRow(1, 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(sqlmock.NewRows([]string{"row_id", "changed_at"}).AddRow(row.String(), 1))
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(sqlmock.NewRows([]string{"ltbase_row_id"}).AddRow(row.String()))

	if _, err := getUnflushedSchemaIDs(ctx, db, "change_log"); err != nil {
		t.Fatalf("getUnflushedSchemaIDs: %v", err)
	}
	if _, _, err := GetChangeLogStats(ctx, db, "change_log", 100); err != nil {
		t.Fatalf("GetChangeLogStats: %v", err)
	}
	if _, _, _, err := SelectBatchRowIDs(ctx, db, "change_log", 100, 500); err != nil {
		t.Fatalf("SelectBatchRowIDs: %v", err)
	}
	for _, filter := range []int{0, 100} { // every schema, and one
		if _, err := getSchemaIDsToInit(ctx, db, "schema_registry", filter); err != nil {
			t.Fatalf("getSchemaIDsToInit(%d): %v", filter, err)
		}
	}
	if _, err := getEntityMainCount(ctx, db, "entity_main", 100); err != nil {
		t.Fatalf("getEntityMainCount: %v", err)
	}
	if _, err := selectEntityMainBatch(ctx, db, "entity_main", 100, &row, 500); err != nil {
		t.Fatalf("selectEntityMainBatch: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the flush reads sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-cdc-flush-reads", Statements: rec.Take(t,
		"internal/cdc/flusher.go:getUnflushedSchemaIDs",
		"internal/cdc/helpers.go:GetChangeLogStats",
		"internal/cdc/helpers.go:SelectBatchRowIDs",
		"internal/cdc/init.go:getSchemaIDsToInit",
		"internal/cdc/init.go:getSchemaIDsToInit",
		"internal/cdc/init_batch_select.go:getEntityMainCount",
		"internal/cdc/init_batch_select.go:selectEntityMainBatch",
	)})
}
