package federated

import (
	"context"
	"strings"
	"testing"
)

// captureDuckExecutor records every SQL string it is asked to run and returns
// an empty, immediately-exhausted row set, so the pure render string of each
// path-scan site can be asserted without a live DuckDB.
type captureDuckExecutor struct{ queries []string }

func (c *captureDuckExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	c.queries = append(c.queries, sql)
	return &verifyFakeRows{rowsLeft: 0}, nil
}

// TestExecutorParquetSitesEscapeQuotes pins #456 for the four executor-driven
// render sites: a path bearing a single quote must appear doubled in the SQL,
// never as a raw quote that could terminate the literal.
func TestExecutorParquetSitesEscapeQuotes(t *testing.T) {
	const evil = "s3://b/x.parquet') UNION ALL SELECT 1 --"
	ctx := context.Background()

	cases := []struct {
		name string
		run  func(duck DuckDBQueryExecutor)
	}{
		{"drainParquet", func(d DuckDBQueryExecutor) { _ = drainParquet(ctx, d, evil) }},
		{"drainGuardedParquet", func(d DuckDBQueryExecutor) { _ = drainGuardedParquet(ctx, d, evil) }},
		{"globParquetPaths", func(d DuckDBQueryExecutor) { _, _ = globParquetPaths(ctx, d, evil) }},
		{"describeParquetColumns", func(d DuckDBQueryExecutor) { _, _ = describeParquetColumns(ctx, d, evil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &captureDuckExecutor{}
			tc.run(cap)
			if len(cap.queries) == 0 {
				t.Fatalf("%s issued no query", tc.name)
			}
			sql := cap.queries[0]
			// Anchor on the payload prefix: an unescaped path renders
			// "parquet') UNION" (a lone quote closing the literal), a doubled
			// one renders "parquet'') UNION". A bare "') UNION" check cannot
			// discriminate, since "'') UNION" is a superstring of "') UNION".
			if strings.Contains(sql, "parquet') UNION") {
				t.Fatalf("%s left a raw quote (SQL breakout): %s", tc.name, sql)
			}
			if !strings.Contains(sql, "parquet'') UNION") {
				t.Fatalf("%s did not double the quote: %s", tc.name, sql)
			}
		})
	}
}
