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
	labelExportSnapshot  = "internal/cdc/export_pg_stage.go:stageInSnapshot"
	labelExportChangeLog = "internal/cdc/export_sql_builder.go:buildChangeLogQuery"
	labelExportMain      = "internal/cdc/export_sql_builder.go:buildMainEntityQuery"
	labelExportEAV       = "internal/cdc/export_sql_builder.go:buildEAVQuery"
)

// corpusAttrCache is a representative schema: one attribute bound to an
// entity_main column, two stored in EAV.
var corpusAttrCache = derivedAncestry(forma.SchemaAttributeCache{
	"status": {AttributeName: "status", AttributeID: 10, ValueType: forma.ValueTypeText,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
	"score":     {AttributeName: "score", AttributeID: 11, ValueType: forma.ValueTypeInteger},
	"is_active": {AttributeName: "is_active", AttributeID: 12, ValueType: forma.ValueTypeBool},
})

// TestDSQLCorpusExport exports the PostgreSQL side of a CDC export for the
// DSQL lint gate (#740). The delta export runs as stageInSnapshot runs it:
// the BEGIN pgx writes for its REPEATABLE READ READ ONLY options, the
// change_log, entity_main and eav_data reads in plan order, and the COMMIT.
// The base export reads live entity_main rows and eav_data on the export
// snapshot internal/cdcv2's Writer.Init begins and ends (stageRowsOnTx), so
// its unit holds the reads only. The projection and COPY that follow run in
// DuckDB, not on the database.
func TestDSQLCorpusExport(t *testing.T) {
	rows := []uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000001")}
	delta, err := buildDeltaExportPlan(stagedTransport, "/tmp/export.parquet", CDCConfig{}, 100, 1_700_000_000_000, rows, corpusAttrCache)
	if err != nil {
		t.Fatalf("build delta export plan: %v", err)
	}
	base, err := buildBaseExportPlan(stagedTransport, "/tmp/export.parquet", CDCConfig{}, 100, rows, corpusAttrCache)
	if err != nil {
		t.Fatalf("build base export plan: %v", err)
	}
	begin, commit := snapshotBoundary(t)
	deltaStatements := append([]corpus.Statement{begin}, stageStatements(t, delta)...)
	corpus.Export(t, corpus.Unit{Name: "internal-cdc-export-delta", Engine: forma.EngineAuroraDSQL, Statements: append(deltaStatements, commit)})
	corpus.Export(t, corpus.Unit{Name: "internal-cdc-export-base", Engine: forma.EngineAuroraDSQL, Statements: stageStatements(t, base)})
}

// stageStatements renders plan's stage reads under their builders' labels.
func stageStatements(t *testing.T, plan exportSQLPlan) []corpus.Statement {
	t.Helper()
	stageLabels := map[string]string{"cl": labelExportChangeLog, "m": labelExportMain, "e_rows": labelExportEAV}
	var statements []corpus.Statement
	for _, stage := range plan.stages {
		label, ok := stageLabels[stage.alias]
		if !ok {
			t.Fatalf("export stage %q has no label: name its query builder so the DSQL lint gate can trace it", stage.alias)
		}
		statements = append(statements, corpus.Rendered(label, stage.query))
	}
	return statements
}

// snapshotBoundary records the BEGIN and COMMIT stageInSnapshot sends
// around an export's reads, from a real pgx connection.
func snapshotBoundary(t *testing.T) (begin, commit corpus.Statement) {
	t.Helper()
	var rec corpus.Recorder
	if _, err := stageInSnapshot(context.Background(), corpus.WireConn(t, &rec), nil, nil, nil); err != nil {
		t.Fatalf("stageInSnapshot: %v", err)
	}
	statements := rec.Take(t, labelExportSnapshot, labelExportSnapshot)
	return statements[0], statements[1]
}

// TestDSQLCorpusFlushReads exports the change_log and entity_main reads
// cdc-flush and cdc-init plan their runs with. The batch selection and the
// export snapshot's version read are internal/cdcv2's (its own corpus unit).
func TestDSQLCorpusFlushReads(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(rec.MatchSQL)))
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"count", "min"}).AddRow(1, 1))
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"schema_id"}).AddRow(100))
	mock.ExpectQuery("").WithArgs(anyArgs(1)...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	if _, err := getUnflushedSchemaIDs(ctx, db, "change_log"); err != nil {
		t.Fatalf("getUnflushedSchemaIDs: %v", err)
	}
	if _, _, err := GetChangeLogStats(ctx, db, "change_log", 100); err != nil {
		t.Fatalf("GetChangeLogStats: %v", err)
	}
	for _, filter := range []int{0, 100} { // every schema, and one
		if _, err := getSchemaIDsToInit(ctx, db, "schema_registry", filter); err != nil {
			t.Fatalf("getSchemaIDsToInit(%d): %v", filter, err)
		}
	}
	if _, err := getEntityMainCount(ctx, db, "entity_main", 100); err != nil {
		t.Fatalf("getEntityMainCount: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the flush reads sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-cdc-flush-reads", Engine: forma.EngineAuroraDSQL, Statements: rec.Take(t,
		"internal/cdc/flusher.go:getUnflushedSchemaIDs",
		"internal/cdc/helpers.go:GetChangeLogStats",
		"internal/cdc/init.go:getSchemaIDsToInit",
		"internal/cdc/init.go:getSchemaIDsToInit",
		"internal/cdc/init_batch_select.go:getEntityMainCount",
	)})
}
