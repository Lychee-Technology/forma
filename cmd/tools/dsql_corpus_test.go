package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/pashagolub/pgxmock/v4"
)

// initDBLabels names the generator of each statement tableTransactionDDL
// returns, keyed by the statement's failure label. A statement added to
// init-db without an entry here fails TestDSQLCorpus, so it cannot reach the
// DSQL lint gate (#740) unattributed or slip past it.
var initDBLabels = map[string]string{
	"ensure schema registry table":             "cmd/tools/init_db.go:schemaRegistryDDL",
	"ensure entity main table":                 "cmd/tools/init_db.go:entityMainDDL",
	"ensure eav table":                         "cmd/tools/init_db.go:eavTableDDL",
	"ensure change log table":                  "cmd/tools/init_db.go:changeLogDDL",
	"ensure unique key table":                  "internal/uniquekey/ddl.go:keyTableDDL",
	"ensure unique key state table":            "internal/uniquekey/ddl.go:stateTableDDL",
	"ensure row guard table":                   "internal/rowguard/ddl.go:tableDDL",
	"ensure schema lease table":                "internal/schemalease/ddl.go:tableDDL",
	"ensure active generation column":          "internal/schemaledger/ddl.go:pointerDDL",
	"ensure schema generation table":           "internal/schemaledger/ddl.go:generationDDL",
	"ensure schema generation attribute table": "internal/schemaledger/ddl.go:attributeDDL",
	"create numeric index":                     "cmd/tools/init_db.go:eavIndexDDL",
	"create text index":                        "cmd/tools/init_db.go:eavIndexDDL",
	"create change log flush index":            "cmd/tools/init_db.go:changeLogFlushIndexDDL",
	"create unique key row index":              "internal/uniquekey/ddl.go:keyRowIndexDDL",
}

// mainIndexFailurePrefix labels each main-column index statement.
const mainIndexFailurePrefix = "create main index for "

// TestDSQLCorpus exports init-db's SQL for the DSQL lint gate (#740): the
// table transaction's statements in the order ensureTables sends them, around
// a BEGIN and COMMIT the test spells for withTx, and the schema registration
// statements as sent. The tables are rendered for DSQL, without the
// COLLATE "C" clauses of the PostgreSQL form (#816), and so without the
// catalog check of those clauses, which runs on PostgreSQL only.
func TestDSQLCorpus(t *testing.T) {
	opts := initDBOptions{
		schemaTable: "schema_registry",
		eavTable:    "eav_data",
		entityMain:  "entity_main",
		changeLog:   "change_log",
		engine:      forma.EngineAuroraDSQL,
	}
	var body []corpus.Statement
	for _, ddl := range tableTransactionDDL(opts) {
		label, ok := initDBLabels[ddl.failure]
		if !ok && strings.HasPrefix(ddl.failure, mainIndexFailurePrefix) {
			label, ok = "cmd/tools/init_db.go:mainColumnIndexDDL", true
		}
		if !ok {
			t.Fatalf("init-db statement %q has no label in initDBLabels: name its generator so the DSQL lint gate can trace it", ddl.failure)
		}
		body = append(body, corpus.Rendered(label, ddl.statement))
	}
	corpus.Export(t, corpus.Unit{
		Name:       "cmd-tools-init-db-tables",
		Engine:     forma.EngineAuroraDSQL,
		Statements: corpus.Transaction("cmd/tools/init_db.go:withTx", body...),
	})
	corpus.Export(t, corpus.Unit{Name: "cmd-tools-init-db-register-schemas", Engine: forma.EngineAuroraDSQL, Statements: registerSchemasSQL(t, opts.schemaTable)})
}

// registerSchemasSQL records the statements registerSchemas sends to
// register one new schema that has no attributes file.
func registerSchemasSQL(t *testing.T, schemaTable string) []corpus.Statement {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "lead.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatalf("write schema file: %v", err)
	}
	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.MatchPgx)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(context.Background()) })
	mock.ExpectBegin()
	mock.ExpectQuery("").WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}))
	mock.ExpectExec("").WithArgs("lead", int16(firstSchemaID)).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	tx, err := mock.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var regErr error
	captureStdStreams(t, func() { regErr = registerSchemas(context.Background(), tx, schemaTable, dir) })
	if regErr != nil {
		t.Fatalf("registerSchemas: %v", regErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("registerSchemas sent other statements than recorded: %v", err)
	}
	return rec.Take(t, "cmd/tools/init_db.go:loadRegisteredSchemaIDs", "cmd/tools/init_db.go:registerSchemas")
}
