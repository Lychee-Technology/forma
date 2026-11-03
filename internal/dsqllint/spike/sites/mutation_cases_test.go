package sites

// EXPERIMENTAL mutation cases for the DSQL typed call-site spike. Every case
// is SYNTHETIC code injected into a disposable worktree of forma; none of it
// exists in forma.

import "strings"

type edit struct{ File, Old, New string }

type mutation struct {
	Name     string
	Group    string // adversarial | identity
	Files    map[string]string
	Edits    []edit
	Expect   string // new-site | finding | no-change | miss-documented | mains
	Describe string
}

const hdr = "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n"

func lit(name, sql string) mutation {
	h := hdr
	if strings.Contains(sql, "strings.") {
		h = "package cdc\n\nimport (\n\t\"context\"\n\t\"strings\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n"
	}
	return mutation{Name: name, Group: "adversarial", Expect: "new-site", Describe: "literal " + sql,
		Files: map[string]string{"internal/cdc/spike_" + name + ".go": h +
			"func spike_" + name + "(ctx context.Context, tx pgx.Tx) error {\n\t_, err := tx.Exec(ctx, " + sql + ")\n\treturn err\n}\n"}}
}

var mutations = []mutation{
	lit("a1_leading_comment", "\"/* c */ CREATE TABLE t (id SERIAL)\""),
	lit("a2_vertical_tab", "\"\\vCREATE TABLE t (id SERIAL)\""),
	lit("a3_lowercase", "\"create table t (id serial)\""),
	lit("a4_cte_columns", "\"WITH ids(id) AS (SELECT 1) SELECT id FROM ids\""),
	lit("a5_paren_select", "\"SELECT(1)\""),
	lit("a6_fragments", "strings.Join([]string{\"CREATE \", \"TABLE t (id SERIAL)\"}, \"\")"),
	{Name: "a7_dynamic_keyword", Group: "adversarial", Expect: "new-site", Describe: "keyword returned by a helper",
		Files: map[string]string{"internal/cdc/spike_a7.go": "package cdc\n\nimport (\n\t\"context\"\n\t\"fmt\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n" +
			"func spikeVerb(dsql bool) string {\n\tif dsql {\n\t\treturn \"CREATE\"\n\t}\n\treturn \"CREATE\"\n}\n\n" +
			"func spikeA7(ctx context.Context, tx pgx.Tx, t string) error {\n\t_, err := tx.Exec(ctx, fmt.Sprintf(\"%s INDEX i ON %s (a)\", spikeVerb(true), t))\n\treturn err\n}\n"}},
	{Name: "a8_const_other_package", Group: "adversarial", Expect: "new-site", Describe: "keyword constant from a new package",
		Files: map[string]string{
			"internal/spikesql/sql.go": "package spikesql\n\nconst Create = \"CREATE\"\n",
			"internal/cdc/spike_a8.go": "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n\t\"github.com/lychee-technology/forma/internal/spikesql\"\n)\n\n" +
				"func spikeA8(ctx context.Context, tx pgx.Tx) error {\n\t_, err := tx.Exec(ctx, spikesql.Create+\" TABLE t (id SERIAL)\")\n\treturn err\n}\n"}},
	{Name: "a9_exec_in_classified_function", Group: "adversarial", Expect: "new-site", Describe: "extra ExecContext inside leaseSchemaLocker.acquire",
		Edits: []edit{{"internal/cdc/schema_lease.go", "\tvar got uuid.UUID\n", "\tif _, err := l.db.ExecContext(ctx, \"SET default_transaction_isolation = 'serializable'\"); err != nil {\n\t\treturn false, err\n\t}\n\tvar got uuid.UUID\n"}}},
	{Name: "a10_embedded_template", Group: "adversarial", Expect: "new-site", Describe: "go:embed template rendered then executed",
		Files: map[string]string{
			"internal/cdc/spike_a10.sql.tmpl": "CREATE TABLE {{.}} (id SERIAL PRIMARY KEY)\n",
			"internal/cdc/spike_a10.go": "package cdc\n\nimport (\n\t\"context\"\n\t_ \"embed\"\n\t\"strings\"\n\t\"text/template\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n//go:embed spike_a10.sql.tmpl\nvar spikeA10Tmpl string\n\n" +
				"func spikeA10(ctx context.Context, tx pgx.Tx) error {\n\tvar b strings.Builder\n\tif err := template.Must(template.New(\"t\").Parse(spikeA10Tmpl)).Execute(&b, \"t\"); err != nil {\n\t\treturn err\n\t}\n\t_, err := tx.Exec(ctx, b.String())\n\treturn err\n}\n"}},
	{Name: "a11_unrelated_exec_method", Group: "adversarial", Expect: "no-change", Describe: "project type with its own Exec(ctx, string, ...any) (pgconn.CommandTag, error)",
		Files: map[string]string{"internal/cdc/spike_a11.go": "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5/pgconn\"\n)\n\ntype spikeRunner struct{}\n\n" +
			"func (spikeRunner) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) { return pgconn.CommandTag{}, nil }\n\n" +
			"func spikeA11(ctx context.Context) error {\n\t_, err := spikeRunner{}.Exec(ctx, \"CREATE TABLE x (id SERIAL)\")\n\treturn err\n}\n"}},
	{Name: "a12_forwarding_helper_two_callers", Group: "adversarial", Expect: "new-site", Describe: "verbatim helper; both callers must be sites",
		Files: map[string]string{"internal/cdc/spike_a12.go": hdr +
			"func spikeExec(ctx context.Context, tx pgx.Tx, sql string) error {\n\t_, err := tx.Exec(ctx, sql)\n\treturn err\n}\n\n" +
			"func spikeA12a(ctx context.Context, tx pgx.Tx) error { return spikeExec(ctx, tx, \"CREATE TABLE a (id SERIAL)\") }\n\n" +
			"func spikeA12b(ctx context.Context, tx pgx.Tx) error { return spikeExec(ctx, tx, \"CREATE TABLE b (id SERIAL)\") }\n"}},
	{Name: "a13_new_modifying_wrapper", Group: "adversarial", Expect: "new-site", Describe: "new wrapper appends FOR UPDATE; its inner call is a new site",
		Files: map[string]string{"internal/cdc/spike_a13.go": hdr +
			"func spikeLock(ctx context.Context, tx pgx.Tx, sql string) error {\n\t_, err := tx.Exec(ctx, sql+\" FOR UPDATE\")\n\treturn err\n}\n\n" +
			"func spikeA13(ctx context.Context, tx pgx.Tx) error { return spikeLock(ctx, tx, \"SELECT 1\") }\n"}},
	{Name: "a13b_new_caller_of_existing_modifying_helper", Group: "adversarial", Expect: "miss-documented", Describe: "new caller of cdc.SelectBatchRowIDs, which builds SQL from its table parameter",
		Files: map[string]string{"internal/cdc/spike_a13b.go": "package cdc\n\nimport (\n\t\"context\"\n\t\"database/sql\"\n)\n\n" +
			"func spikeA13b(ctx context.Context, db *sql.DB) error {\n\t_, _, _, err := SelectBatchRowIDs(ctx, db, \"other_log\", 1, 10)\n\treturn err\n}\n"}},
	{Name: "a14_method_value", Group: "adversarial", Expect: "finding", Describe: "f := tx.Exec",
		Files: map[string]string{"internal/cdc/spike_a14.go": hdr +
			"func spikeA14(ctx context.Context, tx pgx.Tx) error {\n\tf := tx.Exec\n\t_, err := f(ctx, \"CREATE TABLE t (id SERIAL)\")\n\treturn err\n}\n"}},
	{Name: "a15a_new_shipped_package", Group: "adversarial", Expect: "new-site", Describe: "new package with a sink, imported by cdc",
		Files: map[string]string{
			"internal/spikepkg/p.go":    "package spikepkg\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n// Run is synthetic.\nfunc Run(ctx context.Context, tx pgx.Tx) error {\n\t_, err := tx.Exec(ctx, \"CREATE TABLE t (id SERIAL)\")\n\treturn err\n}\n",
			"internal/cdc/spike_a15.go": "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n\t\"github.com/lychee-technology/forma/internal/spikepkg\"\n)\n\nfunc spikeA15(ctx context.Context, tx pgx.Tx) error { return spikepkg.Run(ctx, tx) }\n"}},
	{Name: "a15b_unimported_package", Group: "adversarial", Expect: "no-change", Describe: "new package with a sink that no shipped package imports (outside the claimed scope)",
		Files: map[string]string{"internal/spikeorphan/p.go": "package spikeorphan\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n// Run is synthetic.\nfunc Run(ctx context.Context, tx pgx.Tx) error {\n\t_, err := tx.Exec(ctx, \"SELECT 1\")\n\treturn err\n}\n"}},
	{Name: "a15c_new_main_package", Group: "adversarial", Expect: "mains", Describe: "new cmd/spikecmd main package",
		Files: map[string]string{"cmd/spikecmd/main.go": "package main\n\nfunc main() {}\n"}},
	{Name: "a16_project_interface", Group: "adversarial", Expect: "new-site", Describe: "call through a new project interface satisfied by pgx.Tx",
		Files: map[string]string{"internal/cdc/spike_a16.go": "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5/pgconn\"\n)\n\n" +
			"type spikeQ interface {\n\tExec(context.Context, string, ...any) (pgconn.CommandTag, error)\n}\n\n" +
			"func spikeA16(ctx context.Context, q spikeQ) error {\n\t_, err := q.Exec(ctx, \"CREATE TABLE t (id SERIAL)\")\n\treturn err\n}\n"}},
	{Name: "a17_nested_wrappers", Group: "adversarial", Expect: "new-site", Describe: "outer -> inner -> tx.Exec",
		Files: map[string]string{"internal/cdc/spike_a17.go": hdr +
			"func spikeInner(ctx context.Context, tx pgx.Tx, sql string) error {\n\t_, err := tx.Exec(ctx, sql)\n\treturn err\n}\n\n" +
			"func spikeOuter(ctx context.Context, tx pgx.Tx, sql string) error { return spikeInner(ctx, tx, sql) }\n\n" +
			"func spikeA17(ctx context.Context, tx pgx.Tx) error { return spikeOuter(ctx, tx, \"CREATE TABLE t (id SERIAL)\") }\n"}},
	{Name: "a18_wrapper_other_package", Group: "adversarial", Expect: "new-site", Describe: "wrapper declared in a new package, caller in cdc",
		Files: map[string]string{
			"internal/spikewrap/w.go":   "package spikewrap\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n// Exec is synthetic.\nfunc Exec(ctx context.Context, tx pgx.Tx, sql string) error {\n\t_, err := tx.Exec(ctx, sql)\n\treturn err\n}\n",
			"internal/cdc/spike_a18.go": "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5\"\n\t\"github.com/lychee-technology/forma/internal/spikewrap\"\n)\n\nfunc spikeA18(ctx context.Context, tx pgx.Tx) error {\n\treturn spikewrap.Exec(ctx, tx, \"CREATE TABLE t (id SERIAL)\")\n}\n"}},
	{Name: "a19_two_sinks_one_line", Group: "adversarial", Expect: "new-site", Describe: "two Exec calls on one source line",
		Files: map[string]string{"internal/cdc/spike_a19.go": hdr +
			"func spikeA19(ctx context.Context, tx pgx.Tx) { _, _ = tx.Exec(ctx, \"SELECT 1\"); _, _ = tx.Exec(ctx, \"SELECT 2\") }\n"}},
	{Name: "a20_batch", Group: "adversarial", Expect: "new-site", Describe: "Batch.Queue then SendBatch",
		Files: map[string]string{"internal/cdc/spike_a20.go": hdr +
			"func spikeA20(ctx context.Context, tx pgx.Tx) error {\n\tb := &pgx.Batch{}\n\tb.Queue(\"CREATE TABLE t (id SERIAL)\")\n\treturn tx.SendBatch(ctx, b).Close()\n}\n"}},
	{Name: "a21_func_value_call", Group: "adversarial", Expect: "finding", Describe: "call through a func-typed variable returning pgconn.CommandTag",
		Files: map[string]string{"internal/cdc/spike_a21.go": "package cdc\n\nimport (\n\t\"context\"\n\n\t\"github.com/jackc/pgx/v5/pgconn\"\n)\n\n" +
			"func spikeA21(ctx context.Context, exec func(context.Context, string, ...any) (pgconn.CommandTag, error)) error {\n\t_, err := exec(ctx, \"CREATE TABLE t (id SERIAL)\")\n\treturn err\n}\n"}},
	{Name: "a22_closure_begin_func", Group: "adversarial", Expect: "new-site", Describe: "pgx.BeginFunc with an Exec in the closure",
		Files: map[string]string{"internal/cdc/spike_a22.go": hdr +
			"func spikeA22(ctx context.Context, conn *pgx.Conn) error {\n\treturn pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {\n\t\t_, err := tx.Exec(ctx, \"CREATE TABLE t (id SERIAL)\")\n\t\treturn err\n\t})\n}\n"}},
	{Name: "a23_database_sql", Group: "adversarial", Expect: "new-site", Describe: "new db.QueryRowContext",
		Files: map[string]string{"internal/cdc/spike_a23.go": "package cdc\n\nimport (\n\t\"context\"\n\t\"database/sql\"\n)\n\n" +
			"func spikeA23(ctx context.Context, db *sql.DB) error {\n\tvar n int\n\treturn db.QueryRowContext(ctx, \"SELECT 1\").Scan(&n)\n}\n"}},
	{Name: "a24_interface_without_implementer", Group: "adversarial", Expect: "finding", Describe: "project interface with an Exec method nothing implements",
		Files: map[string]string{"internal/cdc/spike_a24.go": "package cdc\n\ntype spikeX interface{ Exec(string) error }\n\nfunc spikeA24(x spikeX) error { return x.Exec(\"CREATE TABLE t (id SERIAL)\") }\n"}},
	{Name: "a25_reflection", Group: "adversarial", Expect: "miss-documented", Describe: "reflect MethodByName(\"Exec\").Call",
		Files: map[string]string{"internal/cdc/spike_a25.go": "package cdc\n\nimport (\n\t\"context\"\n\t\"reflect\"\n\n\t\"github.com/jackc/pgx/v5\"\n)\n\n" +
			"func spikeA25(ctx context.Context, tx pgx.Tx) {\n\treflect.ValueOf(tx).MethodByName(\"Exec\").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(\"CREATE TABLE t (id SERIAL)\")})\n}\n"}},
	{Name: "a26_generic_type_param", Group: "adversarial", Expect: "new-site", Describe: "func[T pgx.Tx] calling t.Exec",
		Files: map[string]string{"internal/cdc/spike_a26.go": hdr +
			"func spikeA26[T pgx.Tx](ctx context.Context, t T) error {\n\t_, err := t.Exec(ctx, \"CREATE TABLE t (id SERIAL)\")\n\treturn err\n}\n"}},
	{Name: "a27_pgconn_direct", Group: "adversarial", Expect: "new-site", Describe: "tx.Conn().PgConn().Exec",
		Files: map[string]string{"internal/cdc/spike_a27.go": hdr +
			"func spikeA27(ctx context.Context, tx pgx.Tx) error {\n\treturn tx.Conn().PgConn().Exec(ctx, \"CREATE TABLE t (id SERIAL)\").Close()\n}\n"}},
	{Name: "a28_wrapper_as_value", Group: "adversarial", Expect: "finding", Describe: "a verbatim wrapper passed as a function value",
		Files: map[string]string{"internal/cdc/spike_a28.go": hdr +
			"func spikeExec28(ctx context.Context, tx pgx.Tx, sql string) error {\n\t_, err := tx.Exec(ctx, sql)\n\treturn err\n}\n\n" +
			"func spikeApply(f func(context.Context, pgx.Tx, string) error) {}\n\nfunc spikeA28() { spikeApply(spikeExec28) }\n"}},
	{Name: "a29_options_forwarded", Group: "adversarial", Expect: "new-site", Describe: "new BeginTx with non-default options",
		Files: map[string]string{"internal/cdc/spike_a29.go": hdr +
			"func spikeA29(ctx context.Context, conn *pgx.Conn) error {\n\ttx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})\n\tif err != nil {\n\t\treturn err\n\t}\n\treturn tx.Rollback(ctx)\n}\n"}},

	{Name: "a30_new_sql_via_struct_field_into_existing_site", Group: "adversarial", Expect: "miss-documented",
		Describe: "a fourth CDC export stage whose query (a new SQL text) reaches the existing appendStageRows site through exportStage.query",
		Edits: []edit{{"internal/cdc/duckdb_exporter.go", "\t\texportStage{alias: \"e_rows\", table: stageTableEAV, columns: eavStageColumns,",
			"\t\texportStage{alias: \"x\", table: stageTableEAV, columns: eavStageColumns, query: \"CREATE TABLE spike_x (id SERIAL)\"},\n\t\texportStage{alias: \"e_rows\", table: stageTableEAV, columns: eavStageColumns,"}}},
	{Name: "a31_new_branch_at_existing_site", Group: "adversarial", Expect: "miss-documented",
		Describe: "a new runtime branch changes the SQL text an existing site (GetChangeLogStats) sends",
		Edits:    []edit{{"internal/cdc/helpers.go", "\trow := db.QueryRowContext(ctx, query, schemaID)", "\tif schemaID < 0 {\n\t\tquery = \"CREATE TABLE spike_y (id SERIAL)\"\n\t}\n\trow := db.QueryRowContext(ctx, query, schemaID)"}}},

	{Name: "g1a_generator_new_wrapper_caller", Group: "adversarial", Expect: "new-site", Describe: "generator (no DB call) feeding a new caller of the existing verbatim wrapper scanRowPage",
		Files: map[string]string{"internal/spike_g1a_generator_new_wrapper_caller.go": "package internal\n\nimport (\n\t\"context\"\n\n\t\"github.com/google/uuid\"\n)\n\n" +
			"func spikeGen_g1a_generator_new_wrapper_caller() string { return \"SELECT ltbase_row_id FROM t WHERE schema_id = $1 AND ltbase_row_id > $2 LIMIT $3\" }\n\n" +
			"func spikeCaller_g1a_generator_new_wrapper_caller(ctx context.Context, db UniqueKeyStore) error {\n\t_, err := scanRowPage(ctx, db, spikeGen_g1a_generator_new_wrapper_caller(), 1, uuid.Nil)\n\treturn err\n}\n"}},
	{Name: "g1b_vtab_generator_new_wrapper_caller", Group: "adversarial", Expect: "new-site", Describe: "generator (no DB call) feeding a new caller of the existing verbatim wrapper scanRowPage",
		Files: map[string]string{"internal/spike_g1b_vtab_generator_new_wrapper_caller.go": "package internal\n\nimport (\n\t\"context\"\n\n\t\"github.com/google/uuid\"\n)\n\n" +
			"func spikeGen_g1b_vtab_generator_new_wrapper_caller() string { return \"\\vSELECT ltbase_row_id FROM t WHERE schema_id = $1 AND ltbase_row_id > $2 LIMIT $3\" }\n\n" +
			"func spikeCaller_g1b_vtab_generator_new_wrapper_caller(ctx context.Context, db UniqueKeyStore) error {\n\t_, err := scanRowPage(ctx, db, spikeGen_g1b_vtab_generator_new_wrapper_caller(), 1, uuid.Nil)\n\treturn err\n}\n"}},
	{Name: "g2a_existing_site_new_generator", Group: "adversarial", Expect: "runtime-only", Describe: "existing rendered site GetChangeLogStats switched to a new generator",
		Files: map[string]string{"internal/cdc/spike_g2a_existing_site_new_generator.go": "package cdc\n\nfunc spikeStatsSQL(table string) string { return \"CREATE TABLE spike_z (id SERIAL); SELECT 1 FROM \" + table }\n"},
		Edits: []edit{{"internal/cdc/helpers.go", "\tquery := fmt.Sprintf(\"SELECT COUNT(*), COALESCE(MIN(changed_at),0) FROM %s WHERE schema_id = $1 AND flushed_at = 0\", sanitizeIdentifier(table))", "\tquery := spikeStatsSQL(sanitizeIdentifier(table))"}}},
	{Name: "g2b_existing_site_vtab_generator", Group: "adversarial", Expect: "runtime-only", Describe: "existing rendered site GetChangeLogStats switched to a new generator",
		Files: map[string]string{"internal/cdc/spike_g2b_existing_site_vtab_generator.go": "package cdc\n\nfunc spikeStatsSQL(table string) string { return \"\\vCREATE TABLE spike_z (id SERIAL); SELECT 1 FROM \" + table }\n"},
		Edits: []edit{{"internal/cdc/helpers.go", "\tquery := fmt.Sprintf(\"SELECT COUNT(*), COALESCE(MIN(changed_at),0) FROM %s WHERE schema_id = $1 AND flushed_at = 0\", sanitizeIdentifier(table))", "\tquery := spikeStatsSQL(sanitizeIdentifier(table))"}}},

	// Identity: harmless or semantic edits to real code; the score is how
	// many pre-existing site IDs disappear under each scheme.
	{Name: "i1_unrelated_statement", Group: "identity", Describe: "insert `_ = holder` at the top of acquire",
		Edits: []edit{{"internal/cdc/schema_lease.go", "(bool, error) {\n\tquery := fmt.Sprintf(`INSERT", "(bool, error) {\n\t_ = holder\n\tquery := fmt.Sprintf(`INSERT"}}},
	{Name: "i2_reformat", Group: "identity", Describe: "split a call over lines and add blank lines at the top of the file",
		Edits: []edit{
			{"internal/cdc/schema_lease.go", "package cdc\n", "package cdc\n\n\n\n"},
			{"internal/cdc/schema_lease.go", "l.db.QueryRowContext(ctx, query, schemaID, holder, l.ttl.Milliseconds())", "l.db.QueryRowContext(\n\t\tctx,\n\t\tquery,\n\t\tschemaID, holder, l.ttl.Milliseconds(),\n\t)"}}},
	{Name: "i3_sql_call_before_same_callee", Group: "identity", Describe: "insert another QueryRowContext before the existing one in acquire",
		Edits: []edit{{"internal/cdc/schema_lease.go", "\tvar got uuid.UUID\n", "\t_ = l.db.QueryRowContext(ctx, \"SELECT 1\")\n\tvar got uuid.UUID\n"}}},
	{Name: "i4_rename_local", Group: "identity", Describe: "rename local `query` to `stmt` in acquire",
		Edits: []edit{
			{"internal/cdc/schema_lease.go", "(bool, error) {\n\tquery := fmt.Sprintf(`INSERT", "(bool, error) {\n\tstmt := fmt.Sprintf(`INSERT"},
			{"internal/cdc/schema_lease.go", "l.db.QueryRowContext(ctx, query, schemaID, holder", "l.db.QueryRowContext(ctx, stmt, schemaID, holder"}}},
	{Name: "i5_move_file", Group: "identity", Describe: "move GetChangeLogStats to a new file in the same package"},
	{Name: "i6_rename_function", Group: "identity", Describe: "rename leaseSchemaLocker.acquire to acquireLease",
		Edits: []edit{
			{"internal/cdc/schema_lease.go", "func (l *leaseSchemaLocker) acquire(", "func (l *leaseSchemaLocker) acquireLease("},
			{"internal/cdc/schema_lease.go", "l.acquire(", "l.acquireLease("}}},
}
