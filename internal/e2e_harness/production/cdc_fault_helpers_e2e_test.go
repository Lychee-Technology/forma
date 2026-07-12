//go:build e2e

package production

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// seedRows creates n rows through the real EntityManager and returns the
// events (each create adds one dirty change_log row).
func seedRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, n int) []*Event {
	t.Helper()
	events := env.GenerateScript(ScriptSpec{Schema: schema, Creates: n})
	if err := env.ApplyEvents(ctx, events...); err != nil {
		t.Fatalf("apply %d creates: %v", n, err)
	}
	return events
}

// splitKeys partitions new S3 keys into final delta parquet files and _tmp
// leftovers; manifest JSON objects fall through both buckets.
func splitKeys(keys []string) (finals, tmps []string) {
	for _, k := range keys {
		switch {
		case strings.Contains(k, "/_tmp/"):
			tmps = append(tmps, k)
		case strings.HasSuffix(k, ".parquet"):
			finals = append(finals, k)
		}
	}
	return finals, tmps
}

// assertUntouched asserts a failed flush left no observable side effects: no
// new S3 objects, the dirty count unchanged, and no manifest for the schema.
func assertUntouched(t *testing.T, report *FlushReport, schema SchemaRef, wantUnflushed int64) {
	t.Helper()
	if len(report.NewObjects) != 0 {
		t.Errorf("expected no new S3 objects, got %v", report.NewObjects)
	}
	if report.UnflushedAfter != wantUnflushed {
		t.Errorf("unflushed count moved: %d -> %d, want %d", report.UnflushedBefore, report.UnflushedAfter, wantUnflushed)
	}
	if m := report.Manifests[schema.ID]; m != nil && len(m.Files) != 0 {
		t.Errorf("expected no manifest entries for schema %d, got %+v", schema.ID, m.Files)
	}
}

// assertManifestDeltaPaths asserts the schema manifest tracks exactly the
// given delta parquet keys (order-insensitive). Empty wantKeys asserts the
// manifest has no delta entries (or does not exist).
func assertManifestDeltaPaths(t *testing.T, manifests map[int16]*manifest.Manifest, schema SchemaRef, wantKeys []string) {
	t.Helper()
	var got []string
	if m := manifests[schema.ID]; m != nil {
		for _, f := range m.Files {
			if f.Tier == "delta" {
				got = append(got, f.Path)
			}
		}
	}
	want := append([]string(nil), wantKeys...)
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Errorf("manifest delta paths mismatch:\n got  %v\n want %v", got, want)
	}
}

// poisonMarkFlushed installs a trigger that fails any flushed_at update, so
// the flush pipeline breaks exactly at the mark-flushed step (step 5). Only
// the flusher ever updates flushed_at, so no other write path is affected.
func poisonMarkFlushed(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	env.ExecSQL(ctx, `CREATE OR REPLACE FUNCTION e2e_poison_mark_flushed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'e2e injected failure: mark-flushed poisoned';
END $$`)
	env.ExecSQL(ctx, `CREATE TRIGGER e2e_poison_flush
BEFORE UPDATE OF flushed_at ON change_log
FOR EACH ROW EXECUTE FUNCTION e2e_poison_mark_flushed()`)
}

// healMarkFlushed removes the mark-flushed poison trigger.
func healMarkFlushed(ctx context.Context, t *testing.T, env *Env) {
	t.Helper()
	env.ExecSQL(ctx, `DROP TRIGGER IF EXISTS e2e_poison_flush ON change_log`)
}

// assertNoRowExportedTwice reads every delta/base parquet under the schema's
// glob and fails if any row_id appears more than once. Valid only for
// create-only fixtures (a single version per row); update flows legitimately
// export the same row_id at different ver_ts.
func assertNoRowExportedTwice(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	glob := fmt.Sprintf("s3://%s/%s/%d/*.parquet", env.Cluster.Bucket, env.S3Prefix, schema.ID)
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT CAST(row_id AS VARCHAR), count(*) FROM read_parquet('%s') GROUP BY 1 HAVING count(*) > 1`, glob))
	if err != nil {
		t.Fatalf("scan parquet for duplicate row_ids: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rowID string
		var n int64
		if err := rows.Scan(&rowID, &n); err != nil {
			t.Fatalf("scan duplicate row: %v", err)
		}
		t.Errorf("row %s exported %d times across parquet files", rowID, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate duplicate rows: %v", err)
	}
}

// fetchChangeLogRowIDs partitions the schema's change_log rows into flushed
// (flushed_at != 0) and dirty (flushed_at = 0) row-id sets, so scenarios can
// assert exactly WHICH rows carry a flushed_at marker, not just how many.
func fetchChangeLogRowIDs(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) (flushed, dirty map[string]bool) {
	t.Helper()
	rows, err := env.Pool.Query(ctx,
		"SELECT row_id::text, flushed_at FROM change_log WHERE schema_id = $1", schema.ID)
	if err != nil {
		t.Fatalf("query change_log rows for schema %d: %v", schema.ID, err)
	}
	defer rows.Close()
	flushed, dirty = map[string]bool{}, map[string]bool{}
	for rows.Next() {
		var rowID string
		var flushedAt int64
		if err := rows.Scan(&rowID, &flushedAt); err != nil {
			t.Fatalf("scan change_log row: %v", err)
		}
		if flushedAt != 0 {
			flushed[rowID] = true
		} else {
			dirty[rowID] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate change_log rows: %v", err)
	}
	return flushed, dirty
}

// fetchParquetRowIDs reads the distinct row-id set contained in the given
// parquet object keys.
func fetchParquetRowIDs(ctx context.Context, t *testing.T, env *Env, keys []string) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	if len(keys) == 0 {
		return ids
	}
	paths := make([]string, len(keys))
	for i, k := range keys {
		paths[i] = fmt.Sprintf("'s3://%s/%s'", env.Cluster.Bucket, k)
	}
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		"SELECT DISTINCT CAST(row_id AS VARCHAR) FROM read_parquet([%s])", strings.Join(paths, ", ")))
	if err != nil {
		t.Fatalf("read parquet row_ids from %v: %v", keys, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var rowID string
		if err := rows.Scan(&rowID); err != nil {
			t.Fatalf("scan parquet row_id: %v", err)
		}
		ids[rowID] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate parquet row_ids: %v", err)
	}
	return ids
}

// assertSameRowIDs asserts two row-id sets are identical.
func assertSameRowIDs(t *testing.T, label string, got, want map[string]bool) {
	t.Helper()
	for id := range want {
		if !got[id] {
			t.Errorf("%s: row %s missing", label, id)
		}
	}
	for id := range got {
		if !want[id] {
			t.Errorf("%s: unexpected row %s", label, id)
		}
	}
}
