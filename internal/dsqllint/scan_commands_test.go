package dsqllint

import (
	"strings"
	"testing"
)

// postgresCommands holds one example of every command of PostgreSQL 17's
// SQL command reference (Part VI, "SQL Commands"), keyed by the command's
// reference page, so no statement family can drop out of the scan
// unnoticed (#740 review R8-F1). A command the scan once missed is added
// here, not to a separate list.
var postgresCommands = map[string]string{
	"ABORT":                              "ABORT",
	"ALTER AGGREGATE":                    "ALTER AGGREGATE a(int) RENAME TO b",
	"ALTER COLLATION":                    "ALTER COLLATION c REFRESH VERSION",
	"ALTER CONVERSION":                   "ALTER CONVERSION c RENAME TO d",
	"ALTER DATABASE":                     "ALTER DATABASE d SET work_mem = '1MB'",
	"ALTER DEFAULT PRIVILEGES":           "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO r",
	"ALTER DOMAIN":                       "ALTER DOMAIN d SET NOT NULL",
	"ALTER EVENT TRIGGER":                "ALTER EVENT TRIGGER e DISABLE",
	"ALTER EXTENSION":                    "ALTER EXTENSION e UPDATE",
	"ALTER FOREIGN DATA WRAPPER":         "ALTER FOREIGN DATA WRAPPER w NO VALIDATOR",
	"ALTER FOREIGN TABLE":                "ALTER FOREIGN TABLE f ADD COLUMN a int",
	"ALTER FUNCTION":                     "ALTER FUNCTION f() OWNER TO u",
	"ALTER GROUP":                        "ALTER GROUP g ADD USER u",
	"ALTER INDEX":                        "ALTER INDEX i RENAME TO j",
	"ALTER LANGUAGE":                     "ALTER LANGUAGE l OWNER TO u",
	"ALTER LARGE OBJECT":                 "ALTER LARGE OBJECT 1 OWNER TO u",
	"ALTER MATERIALIZED VIEW":            "ALTER MATERIALIZED VIEW v RENAME TO w",
	"ALTER OPERATOR":                     "ALTER OPERATOR + (int, int) OWNER TO u",
	"ALTER OPERATOR CLASS":               "ALTER OPERATOR CLASS c USING btree RENAME TO d",
	"ALTER OPERATOR FAMILY":              "ALTER OPERATOR FAMILY f USING btree RENAME TO g",
	"ALTER POLICY":                       "ALTER POLICY p ON t RENAME TO q",
	"ALTER PROCEDURE":                    "ALTER PROCEDURE p() OWNER TO u",
	"ALTER PUBLICATION":                  "ALTER PUBLICATION p ADD TABLE t",
	"ALTER ROLE":                         "ALTER ROLE r WITH LOGIN",
	"ALTER ROUTINE":                      "ALTER ROUTINE r() OWNER TO u",
	"ALTER RULE":                         "ALTER RULE r ON t RENAME TO s",
	"ALTER SCHEMA":                       "ALTER SCHEMA s RENAME TO t",
	"ALTER SEQUENCE":                     "ALTER SEQUENCE s RESTART",
	"ALTER SERVER":                       "ALTER SERVER s VERSION '2'",
	"ALTER STATISTICS":                   "ALTER STATISTICS s SET STATISTICS 10",
	"ALTER SUBSCRIPTION":                 "ALTER SUBSCRIPTION s DISABLE",
	"ALTER SYSTEM":                       "ALTER SYSTEM SET work_mem = '1MB'",
	"ALTER TABLE":                        "ALTER TABLE t ADD COLUMN a int",
	"ALTER TABLESPACE":                   "ALTER TABLESPACE t RENAME TO u",
	"ALTER TEXT SEARCH CONFIGURATION":    "ALTER TEXT SEARCH CONFIGURATION c OWNER TO u",
	"ALTER TEXT SEARCH DICTIONARY":       "ALTER TEXT SEARCH DICTIONARY d OWNER TO u",
	"ALTER TEXT SEARCH PARSER":           "ALTER TEXT SEARCH PARSER p RENAME TO q",
	"ALTER TEXT SEARCH TEMPLATE":         "ALTER TEXT SEARCH TEMPLATE t RENAME TO u",
	"ALTER TRIGGER":                      "ALTER TRIGGER tr ON t RENAME TO tr2",
	"ALTER TYPE":                         "ALTER TYPE e ADD VALUE 'x'",
	"ALTER USER":                         "ALTER USER u WITH PASSWORD NULL",
	"ALTER USER MAPPING":                 "ALTER USER MAPPING FOR u SERVER s OPTIONS (SET a 'b')",
	"ALTER VIEW":                         "ALTER VIEW v RENAME TO w",
	"ANALYZE":                            "ANALYZE t",
	"BEGIN":                              "BEGIN",
	"CALL":                               "CALL p()",
	"CHECKPOINT":                         "CHECKPOINT",
	"CLOSE":                              "CLOSE c",
	"CLUSTER":                            "CLUSTER t USING i",
	"COMMENT":                            "COMMENT ON TABLE t IS 'x'",
	"COMMIT":                             "COMMIT",
	"COMMIT PREPARED":                    "COMMIT PREPARED 'g'",
	"COPY":                               "COPY t FROM STDIN",
	"CREATE ACCESS METHOD":               "CREATE ACCESS METHOD m TYPE TABLE HANDLER h",
	"CREATE AGGREGATE":                   "CREATE AGGREGATE a(int) (SFUNC = f, STYPE = int)",
	"CREATE CAST":                        "CREATE CAST (int AS text) WITH INOUT",
	"CREATE COLLATION":                   "CREATE COLLATION c (locale = 'C')",
	"CREATE CONVERSION":                  "CREATE DEFAULT CONVERSION c FOR 'LATIN1' TO 'UTF8' FROM f",
	"CREATE DATABASE":                    "CREATE DATABASE d",
	"CREATE DOMAIN":                      "CREATE DOMAIN d AS int",
	"CREATE EVENT TRIGGER":               "CREATE EVENT TRIGGER e ON ddl_command_start EXECUTE FUNCTION f()",
	"CREATE EXTENSION":                   "CREATE EXTENSION IF NOT EXISTS pg_trgm",
	"CREATE FOREIGN DATA WRAPPER":        "CREATE FOREIGN DATA WRAPPER w",
	"CREATE FOREIGN TABLE":               "CREATE FOREIGN TABLE f (a int) SERVER s",
	"CREATE FUNCTION":                    "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
	"CREATE GROUP":                       "CREATE GROUP g",
	"CREATE INDEX":                       "CREATE INDEX ASYNC i ON t (a)",
	"CREATE LANGUAGE":                    "CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE l",
	"CREATE MATERIALIZED VIEW":           "CREATE MATERIALIZED VIEW v AS SELECT 1",
	"CREATE OPERATOR":                    "CREATE OPERATOR === (LEFTARG = int, RIGHTARG = int, FUNCTION = f)",
	"CREATE OPERATOR CLASS":              "CREATE OPERATOR CLASS c FOR TYPE int USING btree AS OPERATOR 1 <",
	"CREATE OPERATOR FAMILY":             "CREATE OPERATOR FAMILY f USING btree",
	"CREATE POLICY":                      "CREATE POLICY p ON t USING (true)",
	"CREATE PROCEDURE":                   "CREATE PROCEDURE p() AS $$ $$ LANGUAGE sql",
	"CREATE PUBLICATION":                 "CREATE PUBLICATION p FOR ALL TABLES",
	"CREATE ROLE":                        "CREATE ROLE r",
	"CREATE RULE":                        "CREATE RULE r AS ON INSERT TO t DO NOTHING",
	"CREATE SCHEMA":                      "CREATE SCHEMA s",
	"CREATE SEQUENCE":                    "CREATE UNLOGGED SEQUENCE s",
	"CREATE SERVER":                      "CREATE SERVER s FOREIGN DATA WRAPPER w",
	"CREATE STATISTICS":                  "CREATE STATISTICS s ON a, b FROM t",
	"CREATE SUBSCRIPTION":                "CREATE SUBSCRIPTION s CONNECTION 'c' PUBLICATION p",
	"CREATE TABLE":                       "CREATE GLOBAL TEMPORARY TABLE t (a int)",
	"CREATE TABLE AS":                    "CREATE TABLE t AS SELECT 1",
	"CREATE TABLESPACE":                  "CREATE TABLESPACE t LOCATION '/x'",
	"CREATE TEXT SEARCH CONFIGURATION":   "CREATE TEXT SEARCH CONFIGURATION c (PARSER = p)",
	"CREATE TEXT SEARCH DICTIONARY":      "CREATE TEXT SEARCH DICTIONARY d (TEMPLATE = t)",
	"CREATE TEXT SEARCH PARSER":          "CREATE TEXT SEARCH PARSER p (START = s, GETTOKEN = g, END = e, LEXTYPES = l)",
	"CREATE TEXT SEARCH TEMPLATE":        "CREATE TEXT SEARCH TEMPLATE t (LEXIZE = l)",
	"CREATE TRANSFORM":                   "CREATE TRANSFORM FOR int LANGUAGE l (FROM SQL WITH FUNCTION f)",
	"CREATE TRIGGER":                     "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
	"CREATE TYPE":                        "CREATE TYPE e AS ENUM ('a')",
	"CREATE USER":                        "CREATE USER u",
	"CREATE USER MAPPING":                "CREATE USER MAPPING FOR u SERVER s",
	"CREATE VIEW":                        "CREATE TEMP VIEW v AS SELECT 1",
	"DEALLOCATE":                         "DEALLOCATE PREPARE ALL",
	"DECLARE":                            "DECLARE c NO SCROLL CURSOR WITH HOLD FOR SELECT 1",
	"DELETE":                             "DELETE FROM t",
	"DISCARD":                            "DISCARD ALL",
	"DO":                                 "DO $$ BEGIN PERFORM 1; END $$",
	"DROP ACCESS METHOD":                 "DROP ACCESS METHOD m",
	"DROP AGGREGATE":                     "DROP AGGREGATE a(int)",
	"DROP CAST":                          "DROP CAST (int AS text)",
	"DROP COLLATION":                     "DROP COLLATION c",
	"DROP CONVERSION":                    "DROP CONVERSION c",
	"DROP DATABASE":                      "DROP DATABASE d",
	"DROP DOMAIN":                        "DROP DOMAIN d",
	"DROP EVENT TRIGGER":                 "DROP EVENT TRIGGER e",
	"DROP EXTENSION":                     "DROP EXTENSION e",
	"DROP FOREIGN DATA WRAPPER":          "DROP FOREIGN DATA WRAPPER w",
	"DROP FOREIGN TABLE":                 "DROP FOREIGN TABLE f",
	"DROP FUNCTION":                      "DROP FUNCTION f()",
	"DROP GROUP":                         "DROP GROUP g",
	"DROP INDEX":                         "DROP INDEX i",
	"DROP LANGUAGE":                      "DROP LANGUAGE l",
	"DROP MATERIALIZED VIEW":             "DROP MATERIALIZED VIEW v",
	"DROP OPERATOR":                      "DROP OPERATOR === (int, int)",
	"DROP OPERATOR CLASS":                "DROP OPERATOR CLASS c USING btree",
	"DROP OPERATOR FAMILY":               "DROP OPERATOR FAMILY f USING btree",
	"DROP OWNED":                         "DROP OWNED BY r",
	"DROP POLICY":                        "DROP POLICY p ON t",
	"DROP PROCEDURE":                     "DROP PROCEDURE p()",
	"DROP PUBLICATION":                   "DROP PUBLICATION p",
	"DROP ROLE":                          "DROP ROLE r",
	"DROP ROUTINE":                       "DROP ROUTINE r()",
	"DROP RULE":                          "DROP RULE r ON t",
	"DROP SCHEMA":                        "DROP SCHEMA s",
	"DROP SEQUENCE":                      "DROP SEQUENCE s",
	"DROP SERVER":                        "DROP SERVER s",
	"DROP STATISTICS":                    "DROP STATISTICS s",
	"DROP SUBSCRIPTION":                  "DROP SUBSCRIPTION s",
	"DROP TABLE":                         "DROP TABLE t",
	"DROP TABLESPACE":                    "DROP TABLESPACE t",
	"DROP TEXT SEARCH CONFIGURATION":     "DROP TEXT SEARCH CONFIGURATION c",
	"DROP TEXT SEARCH DICTIONARY":        "DROP TEXT SEARCH DICTIONARY d",
	"DROP TEXT SEARCH PARSER":            "DROP TEXT SEARCH PARSER p",
	"DROP TEXT SEARCH TEMPLATE":          "DROP TEXT SEARCH TEMPLATE t",
	"DROP TRANSFORM":                     "DROP TRANSFORM FOR int LANGUAGE l",
	"DROP TRIGGER":                       "DROP TRIGGER tr ON t",
	"DROP TYPE":                          "DROP TYPE e",
	"DROP USER":                          "DROP USER u",
	"DROP USER MAPPING":                  "DROP USER MAPPING FOR u SERVER s",
	"DROP VIEW":                          "DROP VIEW v",
	"END":                                "END",
	"EXECUTE":                            "EXECUTE q(1)",
	"EXPLAIN":                            "EXPLAIN SELECT 1",
	"FETCH":                              "FETCH FORWARD 5 FROM c",
	"GRANT":                              "GRANT app_role TO app_user",
	"IMPORT FOREIGN SCHEMA":              "IMPORT FOREIGN SCHEMA s FROM SERVER v INTO t",
	"INSERT":                             "INSERT INTO t VALUES (1)",
	"LISTEN":                             "LISTEN ch",
	"LOAD":                               "LOAD 'auto_explain'",
	"LOCK":                               "LOCK t IN ACCESS EXCLUSIVE MODE",
	"MERGE":                              "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
	"MOVE":                               "MOVE LAST IN c",
	"NOTIFY":                             "NOTIFY ch, 'payload'",
	"PREPARE":                            "PREPARE q(int) AS SELECT $1",
	"PREPARE TRANSACTION":                "PREPARE TRANSACTION 'g'",
	"REASSIGN OWNED":                     "REASSIGN OWNED BY a TO b",
	"REFRESH MATERIALIZED VIEW":          "REFRESH MATERIALIZED VIEW v",
	"REINDEX":                            "REINDEX (VERBOSE) TABLE t",
	"RELEASE SAVEPOINT":                  "RELEASE SAVEPOINT s",
	"RESET":                              "RESET ALL",
	"REVOKE":                             "REVOKE app_role FROM app_user",
	"ROLLBACK":                           "ROLLBACK",
	"ROLLBACK PREPARED":                  "ROLLBACK PREPARED 'g'",
	"ROLLBACK TO SAVEPOINT":              "ROLLBACK TO SAVEPOINT s",
	"SAVEPOINT":                          "SAVEPOINT s",
	"SECURITY LABEL":                     "SECURITY LABEL ON TABLE t IS 'x'",
	"SELECT":                             "SELECT 1",
	"SELECT INTO":                        "SELECT 1 INTO t",
	"SET":                                "SET search_path TO app",
	"SET CONSTRAINTS":                    "SET CONSTRAINTS ALL DEFERRED",
	"SET ROLE":                           "SET ROLE r",
	"SET SESSION AUTHORIZATION":          "SET SESSION AUTHORIZATION u",
	"SET TRANSACTION":                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
	"SHOW":                               "SHOW ALL",
	"START TRANSACTION":                  "START TRANSACTION",
	"TRUNCATE":                           "TRUNCATE t",
	"UNLISTEN":                           "UNLISTEN *",
	"UPDATE":                             "UPDATE t SET a = 1",
	"VACUUM":                             "VACUUM (FULL, ANALYZE) t",
	"VALUES":                             "VALUES (1)",
	"WITH (SELECT's WITH clause)":        "WITH x AS (SELECT 1) SELECT * FROM x",
	"TABLE (SELECT's TABLE form)":        "TABLE t",
	"ALTER TABLE ... (lower case)":       "alter table t add column a int",
	"GRANT on objects":                   "GRANT SELECT, INSERT ON TABLE t TO r",
	"GRANT ALL PRIVILEGES":               "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA s TO r",
	"REVOKE GRANT OPTION FOR":            "REVOKE GRANT OPTION FOR SELECT ON t FROM r",
	"REVOKE ADMIN OPTION FOR":            "REVOKE ADMIN OPTION FOR app_role FROM app_user",
	"COMMENT ON LARGE OBJECT":            "COMMENT ON LARGE OBJECT 1 IS 'x'",
	"COMMIT AND CHAIN":                   "COMMIT WORK AND CHAIN",
	"ABORT TRANSACTION":                  "abort transaction;",
	"END WORK":                           "END WORK",
	"RELEASE (no SAVEPOINT keyword)":     "RELEASE s",
	"ROLLBACK WORK TO (no SAVEPOINT)":    "ROLLBACK WORK TO s",
	"DO LANGUAGE":                        "DO LANGUAGE plpgsql $body$ BEGIN END $body$",
	"DO with a string body":              "DO 'BEGIN PERFORM 1; END'",
	"CALL (qualified)":                   `CALL "s".p(1, 2)`,
	"EXPLAIN (options)":                  "EXPLAIN (ANALYZE, BUFFERS) SELECT 1",
	"EXPLAIN ANALYZE VERBOSE":            "explain analyze verbose update t set a = 1",
	"VALUES (punctuated)":                "VALUES(1),(2)",
	"TABLE ONLY ... ORDER BY":            "TABLE ONLY t ORDER BY a",
	"(TABLE) UNION (TABLE)":              "(TABLE a) UNION (TABLE b)",
	"LOCK TABLE":                         "LOCK TABLE t",
	"LOCK (several, NOWAIT)":             "LOCK a, b NOWAIT",
	"FETCH (bare cursor)":                "FETCH c",
	"FETCH (count)":                      "FETCH 10 IN c",
	"MOVE (relative)":                    "MOVE RELATIVE -1 FROM c",
	"DECLARE (BINARY INSENSITIVE)":       "DECLARE c BINARY INSENSITIVE CURSOR FOR SELECT 1",
	"EXECUTE (no parameters)":            "EXECUTE q",
	"DEALLOCATE (one)":                   "DEALLOCATE q",
	"ANALYSE (British, VERBOSE)":         "ANALYSE VERBOSE t (a, b)",
	"VACUUM (bare)":                      "VACUUM",
	"CLUSTER (bare)":                     "CLUSTER",
	"REINDEX (CONCURRENTLY)":             "REINDEX INDEX CONCURRENTLY i",
	"NOTIFY (bare)":                      "NOTIFY ch",
	"COPY (query)":                       "COPY (SELECT 1) TO STDOUT",
	"SET LOCAL":                          "SET LOCAL statement_timeout = 0",
	"BEGIN ISOLATION LEVEL":              "BEGIN ISOLATION LEVEL SERIALIZABLE",
	"CREATE UNLOGGED TABLE (punctuated)": "CREATE UNLOGGED TABLE\"t\"(a int)",
}

// TestEveryPostgresCommand holds the scan to every command PostgreSQL has:
// each example must read as SQL, so a generator of any of them is a site.
func TestEveryPostgresCommand(t *testing.T) {
	for command, example := range postgresCommands {
		if !LooksLikeSQL(example) {
			t.Errorf("%s: LooksLikeSQL(%q) = false, want true", command, example)
		}
	}
}

// TestCommandKeywordsInProseAreNotSQL keeps the commands whose keyword is
// also an English word from making prose a site. A two-word text such as
// "move on" or "close c" is a whole statement (MOVE <cursor>, CLOSE
// <cursor>) and stays a site; classify such a symbol not-dsql.
func TestCommandKeywordsInProseAreNotSQL(t *testing.T) {
	for _, s := range []string{
		"do the work", "do not retry", "call the executor", "call back later", "values are compared",
		"values must match", "explain why", "explain the plan", "grant", "grant access", "revoke the token",
		"table", "table of contents", "table %s: %v", "close the connection", "close", "fetch the manifest object",
		"fetch", "end of input", "end the batch early", "abort the run", "analyze the plan", "vacuum the floor",
		"cluster health check", "checkpoint reached", "load the file", "listen on :8080", "notify the operator now",
		"prepare the batch", "execute the query", "deallocate the buffers now", "declare victory", "move on to the next",
		"release the lock early", "lock the mutex", "security of labels", "import foreign data later",
		"reassign the work", "end-of-input", "values", "do", "call",
	} {
		if LooksLikeSQL(s) {
			t.Errorf("LooksLikeSQL(%q) = true, want false", s)
		}
	}
}

// familyGo holds generators of statement families the scan once missed,
// each feeding the classified passthrough executor (#740 review R8-F1).
// dsql-lint refuses the DO block and the role GRANT (parse_error), so none
// may reach it unclassified.
const familyGo = `package p

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// exec is the classified passthrough executor.
func exec(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Exec(ctx, sql)
	return err
}

func doBlock() string { return "DO $$ BEGIN PERFORM 1; END $$" }

func run(ctx context.Context, tx pgx.Tx) error { return exec(ctx, tx, doBlock()) }

func call() string { return "CALL p()" }

func values() string { return "VALUES (1)" }

func explain() string { return "EXPLAIN SELECT 1" }

func grantRole() string { return "GRANT app_role TO app_user" }

func revokeRole() string { return "revoke app_role from app_user" }

func tableQuery() string { return "TABLE t" }

func vacuum() string { return "VACUUM (ANALYZE) t" }

func listen() string { return "LISTEN ch" }

func prepared() string { return "PREPARE q(int) AS SELECT $1" }

func cursor() string { return "DECLARE c CURSOR FOR SELECT 1" }

func release() string { return "RELEASE SAVEPOINT s" }

func dynamicCall(ctx context.Context, tx pgx.Tx, proc string) error {
	return exec(ctx, tx, "CALL "+proc+"()")
}

func dynamicGrant(role, user string) string {
	return strings.Join([]string{"GRANT ", role, " TO ", user}, "")
}

func dynamicExplain(query string) string { return "EXPLAIN " + query }

func sprintedDo(body string) string { return fmt.Sprintf("DO $body$ %s $body$", body) }

func label(name string) string { return "call " + name + " to explain the values" }
`

// TestStatementFamilyGeneratorsFeedingAClassifiedExecutorFail is the
// regression for a generator of a statement family the scan's prefixes
// once left out, such as DO, CALL, VALUES, EXPLAIN or a role GRANT: the
// scan recognises every PostgreSQL command, so the generator is a site the
// passthrough executor's entry does not cover, and the guard fails until
// it is classified itself.
func TestStatementFamilyGeneratorsFeedingAClassifiedExecutorFail(t *testing.T) {
	snap := scanFixture(t, map[string]string{"p/p.go": familyGo})
	want := map[string]string{
		"p/p.go:doBlock":        whyLiteral,
		"p/p.go:call":           whyLiteral,
		"p/p.go:values":         whyLiteral,
		"p/p.go:explain":        whyLiteral,
		"p/p.go:grantRole":      whyLiteral,
		"p/p.go:revokeRole":     whyLiteral,
		"p/p.go:tableQuery":     whyLiteral,
		"p/p.go:vacuum":         whyLiteral,
		"p/p.go:listen":         whyLiteral,
		"p/p.go:prepared":       whyLiteral,
		"p/p.go:cursor":         whyLiteral,
		"p/p.go:release":        whyLiteral,
		"p/p.go:dynamicCall":    whyFragments,
		"p/p.go:dynamicGrant":   whyFragments,
		"p/p.go:dynamicExplain": whyFragments,
		"p/p.go:sprintedDo":     whyLiteral,
	}
	for loc, why := range want {
		if got := snap.Sites[loc]; got.Why != why {
			t.Errorf("Sites[%s] = %+v, want a site that %s", loc, got, why)
		}
	}
	for _, loc := range []string{"p/p.go:run", "p/p.go:label"} {
		if site, ok := snap.Sites[loc]; ok {
			t.Errorf("%s holds no SQL of its own, but the scan reported %+v", loc, site)
		}
	}

	classified := []Entry{{Class: ClassPassthrough, Location: "p/p.go:exec", Line: 1}}
	joined := ""
	for _, p := range CheckInventory(classified, snap) {
		joined += p.Error() + "\n"
	}
	for loc, why := range want {
		if !strings.Contains(joined, loc+" "+why) || !strings.Contains(joined, "no inventory entry classifies it") {
			t.Errorf("CheckInventory did not refuse the unclassified generator %s:\n%s", loc, joined)
		}
	}

	line := 2
	for loc := range want {
		classified = append(classified, Entry{Class: ClassUncovered, Location: loc, Line: line, Gap: "later"})
		line++
	}
	if problems := CheckInventory(classified, snap); len(problems) != 0 {
		t.Errorf("CheckInventory with every generator classified = %v, want none", problems)
	}
}
