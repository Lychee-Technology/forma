package dsqllint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	// The DuckDB driver registers itself as "duckdb" for the empty-statement
	// check.
	_ "github.com/duckdb/duckdb-go/v2"
)

// duckDBEmptyQuery is the error text duckdb-go answers a query that holds no
// statement with. The driver does not export the error value;
// newEmptinessChecker proves the text still holds on every run.
const duckDBEmptyQuery = "empty query"

// emptinessChecker tells whether SQL text holds a statement. dsql-lint gives
// no signal for this: empty, whitespace-only and comment-only input all exit 0
// with no diagnostics, just as a valid statement does. A record holding such
// text would be counted and lint nothing.
//
// DuckDB already ships with Forma, and its parser separates "no statement"
// from everything else, so the gate needs no SQL lexer of its own. DuckDB
// cannot judge validity (it refuses CREATE INDEX ASYNC, for one), so only the
// empty-query answer counts; a parser error is left for dsql-lint to judge.
type emptinessChecker struct {
	db *sql.DB
}

// newEmptinessChecker opens an in-memory DuckDB and proves the check still
// tells empty from non-empty, so a driver upgrade that changes the answer fails
// the gate instead of turning the check off.
//
// The driver executes every statement but the last of a multi-statement query
// while preparing it, so external access is off: a record is only ever text to
// inspect, and must not reach a file or the network from here.
func newEmptinessChecker(ctx context.Context) (*emptinessChecker, error) {
	db, err := sql.Open("duckdb", "?enable_external_access=false")
	if err != nil {
		return nil, fmt.Errorf("open DuckDB for the empty-statement check: %w", err)
	}
	db.SetMaxOpenConns(1)
	c := &emptinessChecker{db: db}
	for text, want := range map[string]bool{"": true, "SELECT 1": false} {
		got, err := c.isEmpty(ctx, text)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("prove the empty-statement check: %w", err)
		}
		if got != want {
			_ = db.Close()
			return nil, fmt.Errorf("the empty-statement check answered %v for %q, want %v: did a duckdb-go upgrade change its %q error?", got, text, want, duckDBEmptyQuery)
		}
	}
	return c, nil
}

func (c *emptinessChecker) close() {
	_ = c.db.Close()
}

// isEmpty reports whether text holds no statement.
func (c *emptinessChecker) isEmpty(ctx context.Context, text string) (bool, error) {
	stmt, err := c.db.PrepareContext(ctx, text)
	if err == nil {
		_ = stmt.Close()
		return false, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, fmt.Errorf("check for an empty statement: %w", errors.Join(ctxErr, err))
	}
	return err.Error() == duckDBEmptyQuery, nil
}
