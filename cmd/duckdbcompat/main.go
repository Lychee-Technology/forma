// Command duckdbcompat checks that Parquet files written by one DuckDB
// release read back unchanged in another. scripts/duckdb_storage_compat.sh
// builds it twice, against the current driver (go.mod) and against the
// previous one (scripts/duckdbcompat/previous.mod), and runs each build on the
// other's files: the upgrade direction (the new engine reads files already in
// S3) and the rollback direction (a downgraded engine reads files the new one
// wrote).
//
// The files carry every column type Forma's CDC exporters and compaction
// write (see wideParquetTypes in
// internal/e2e_harness/production/full_type_parquet_e2e_test.go) with the
// value shapes Forma produces: NULL attributes, empty and NULL lists,
// integer-width extremes, non-ASCII text, tombstoned rows. They are written
// with the production COPY options from internal/parquetformat, so a change
// of those options changes what this probe checks.
//
// The program imports only the standard library, internal/parquetformat and
// the DuckDB driver, because it must also build under the previous driver's
// module graph.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma/internal/parquetformat"
)

const usage = `usage:
  duckdbcompat version
  duckdbcompat write  <out.parquet>
  duckdbcompat verify <in.parquet> <copies>
  duckdbcompat merge  <out.parquet> <in.parquet>...`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "duckdbcompat:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing command\n%s", usage)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open in-memory duckdb: %w", err)
	}
	defer db.Close()
	// One connection: the fixture table lives in the in-memory catalog of
	// the connection that created it.
	db.SetMaxOpenConns(1)

	version, err := engineVersion(db)
	if err != nil {
		return fmt.Errorf("identify engine: %w", err)
	}
	fmt.Printf("engine: duckdb %s\n", version)

	switch {
	case args[0] == "version" && len(args) == 1:
		return nil
	case args[0] == "write" && len(args) == 2:
		return write(db, args[1])
	case args[0] == "verify" && len(args) == 3:
		copies, err := strconv.Atoi(args[2])
		if err != nil || copies < 1 {
			return fmt.Errorf("copies must be a positive integer, got %q", args[2])
		}
		return verify(db, args[1], copies)
	case args[0] == "merge" && len(args) >= 3:
		return merge(db, args[1], args[2:])
	default:
		return fmt.Errorf("unknown command or wrong arguments: %q\n%s", strings.Join(args, " "), usage)
	}
}

func engineVersion(db *sql.DB) (string, error) {
	var v string
	if err := db.QueryRow("SELECT library_version FROM pragma_version()").Scan(&v); err != nil {
		return "", fmt.Errorf("read engine version: %w", err)
	}
	return v, nil
}

func write(db *sql.DB, out string) error {
	if _, err := db.Exec(fixtureSQL); err != nil {
		return fmt.Errorf("create fixture table: %w", err)
	}
	copySQL := fmt.Sprintf("COPY fixture TO %s (%s)", literal(out), parquetformat.DefaultCopyOptions())
	if _, err := db.Exec(copySQL); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}
	fmt.Printf("wrote %s (%d rows)\n", out, fixtureRows)
	return describeFile(db, out)
}

// merge rewrites its inputs into one file the way compaction does: a
// union_by_name read of every source and one COPY with the production
// options. Forma's LWW fold is left out on purpose; it is SQL over values
// that the single-engine suites already cover, while this probe is about
// what the files hold.
func merge(db *sql.DB, out string, inputs []string) error {
	quoted := make([]string, 0, len(inputs))
	for _, in := range inputs {
		quoted = append(quoted, literal(in))
	}
	copySQL := fmt.Sprintf(
		"COPY (SELECT * FROM read_parquet([%s], union_by_name=true)) TO %s (%s)",
		strings.Join(quoted, ", "), literal(out), parquetformat.DefaultCopyOptions(),
	)
	if _, err := db.Exec(copySQL); err != nil {
		return fmt.Errorf("merge into %s: %w", out, err)
	}
	fmt.Printf("merged %d files into %s\n", len(inputs), out)
	return describeFile(db, out)
}

// verify reads a file and requires it to hold exactly `copies` copies of
// the fixture, with the fixture's column names and types in order. Both
// EXCEPT ALL directions must be empty, so a dropped, duplicated or altered
// row fails, and NULL matches only NULL.
func verify(db *sql.DB, path string, copies int) error {
	if _, err := db.Exec(fixtureSQL); err != nil {
		return fmt.Errorf("create fixture table: %w", err)
	}
	if err := describeFile(db, path); err != nil {
		return fmt.Errorf("describe %s before verifying it: %w", path, err)
	}
	if err := compareColumns(db, path); err != nil {
		return fmt.Errorf("compare columns of %s with the fixture: %w", path, err)
	}
	parts := make([]string, copies)
	for i := range parts {
		parts[i] = "SELECT * FROM fixture"
	}
	expected := "SELECT * FROM (" + strings.Join(parts, " UNION ALL ") + ")"
	actual := fmt.Sprintf("SELECT * FROM read_parquet(%s)", literal(path))
	missing, err := countRows(db, fmt.Sprintf("SELECT count(*) FROM (%s EXCEPT ALL %s)", expected, actual))
	if err != nil {
		return fmt.Errorf("count fixture rows missing from %s: %w", path, err)
	}
	extra, err := countRows(db, fmt.Sprintf("SELECT count(*) FROM (%s EXCEPT ALL %s)", actual, expected))
	if err != nil {
		return fmt.Errorf("count unexpected rows in %s: %w", path, err)
	}
	if missing != 0 || extra != 0 {
		return fmt.Errorf("%s: %d expected rows missing or altered, %d unexpected rows (want %d copies of %d rows)",
			path, missing, extra, copies, fixtureRows)
	}
	fmt.Printf("verified %s: %d copies of %d rows, column names and types match\n", path, copies, fixtureRows)
	return nil
}

func compareColumns(db *sql.DB, path string) error {
	want, err := describe(db, "SELECT * FROM fixture")
	if err != nil {
		return fmt.Errorf("read fixture columns: %w", err)
	}
	got, err := describe(db, fmt.Sprintf("SELECT * FROM read_parquet(%s)", literal(path)))
	if err != nil {
		return fmt.Errorf("read columns of %s: %w", path, err)
	}
	if strings.Join(got, ", ") != strings.Join(want, ", ") {
		return fmt.Errorf("%s: columns read back as\n  %s\nwant\n  %s", path, strings.Join(got, ", "), strings.Join(want, ", "))
	}
	return nil
}

func describe(db *sql.DB, query string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE %s)", query))
	if err != nil {
		return nil, fmt.Errorf("describe %q: %w", query, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, fmt.Errorf("scan describe row: %w", err)
		}
		cols = append(cols, name+" "+typ)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate describe rows: %w", err)
	}
	return cols, nil
}

// describeFile prints what a reader needs to support to open the file: the
// writer, the row groups, and every column's codec and page encodings.
func describeFile(db *sql.DB, path string) error {
	var createdBy string
	var rowGroups int64
	q := fmt.Sprintf("SELECT created_by, num_row_groups FROM parquet_file_metadata(%s)", literal(path))
	if err := db.QueryRow(q).Scan(&createdBy, &rowGroups); err != nil {
		return fmt.Errorf("read file metadata of %s: %w", path, err)
	}
	fmt.Printf("file %s: created_by=%q row_groups=%d\n", path, createdBy, rowGroups)

	rows, err := db.Query(fmt.Sprintf(`SELECT path_in_schema,
       string_agg(DISTINCT compression, ',' ORDER BY compression),
       string_agg(DISTINCT encodings, ' | ' ORDER BY encodings)
FROM parquet_metadata(%s) GROUP BY path_in_schema ORDER BY path_in_schema`, literal(path)))
	if err != nil {
		return fmt.Errorf("read column metadata of %s: %w", path, err)
	}
	defer rows.Close()
	for rows.Next() {
		var col, codecs, encodings string
		if err := rows.Scan(&col, &codecs, &encodings); err != nil {
			return fmt.Errorf("scan column metadata of %s: %w", path, err)
		}
		fmt.Printf("  column %-20s codec=%s encodings=%s\n", col, codecs, encodings)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate column metadata of %s: %w", path, err)
	}
	return nil
}

func countRows(db *sql.DB, query string) (int64, error) {
	var n int64
	if err := db.QueryRow(query).Scan(&n); err != nil {
		return 0, fmt.Errorf("count %q: %w", query, err)
	}
	return n, nil
}

func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
