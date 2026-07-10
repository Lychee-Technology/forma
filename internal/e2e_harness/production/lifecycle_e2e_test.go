//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// TestEntityLifecycle proves the entity lifecycle is correct across CDC
// boundaries (#175). Each subtest drives one lifecycle chain through the
// real EntityManager, CDC flusher, and federated engine, checking every
// query against the independent oracle plus targeted physical (parquet) and
// change_log assertions. The headline risk is delete resurrection: a
// deletion must stay invisible before the flush (dirty-set anti-join), after
// the flush (parquet LWW over the exported tombstone), and across retries.
func TestEntityLifecycle(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// Scenario 1: create → flush → query. Exactly one visible row with
	// correct values, one live delta version exported.
	t.Run("create_flush_query", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, cluster)
		ctx := context.Background()

		creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
		if err := env.ApplyEvents(ctx, creates...); err != nil {
			t.Fatalf("apply create: %v", err)
		}

		env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})

		flush := mustFlush(ctx, t, env)
		if got := countTier(flush.Manifests[wide.ID], "delta"); got != 1 {
			t.Fatalf("manifest holds %d delta files, want 1", got)
		}

		result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
		if result != nil && !result.Plan.Routing.UseDuckDB {
			t.Errorf("post-flush query did not route to duckdb: %+v", result.Plan.Routing)
		}

		assertLiveParquetRow(ctx, t, env, soleParquetKey(t, flush), creates[0])
	})

	// Scenario 2: create → update → flush → query. The latest version wins
	// with no duplicate: the pre-flush change_log upsert coalesces both
	// mutations into the single slot-0 row, so exactly one (updated) version
	// reaches the delta parquet.
	t.Run("update_lww", func(t *testing.T) {
		t.Parallel()
		env := NewEnv(t, cluster)
		ctx := context.Background()

		script := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1, Updates: 1})
		if err := env.ApplyEvents(ctx, script...); err != nil {
			t.Fatalf("apply create+update: %v", err)
		}
		update := script[1]
		if update.Kind != EventUpdate {
			t.Fatalf("script[1] is %s, want update", update.Kind)
		}

		var slot0 int64
		if err := env.Pool.QueryRow(ctx,
			"SELECT COUNT(*) FROM change_log WHERE schema_id = $1 AND row_id = $2",
			wide.ID, update.RowID).Scan(&slot0); err != nil {
			t.Fatalf("count change_log rows: %v", err)
		}
		if slot0 != 1 {
			t.Fatalf("change_log holds %d rows for the mutated row, want 1 coalesced slot-0 row", slot0)
		}

		flush := mustFlush(ctx, t, env)

		// The oracle folds create+update by LWW into one row carrying the
		// update's values — a duplicate or stale engine row fails the diff.
		env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

		assertLiveParquetRow(ctx, t, env, soleParquetKey(t, flush), update)
	})
}

// mustFlush runs a real CDC flush and fails the test unless it drained the
// dirty set completely.
func mustFlush(ctx context.Context, t *testing.T, env *Env) *FlushReport {
	t.Helper()
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	return flush
}

// soleParquetKey returns the single parquet object a flush created, failing
// on any other count (manifest JSON objects are ignored).
func soleParquetKey(t *testing.T, flush *FlushReport) string {
	t.Helper()
	var keys []string
	for _, k := range flush.NewObjects {
		if strings.HasSuffix(k, ".parquet") {
			keys = append(keys, k)
		}
	}
	if len(keys) != 1 {
		t.Fatalf("flush created %d parquet objects %v, want exactly 1", len(keys), keys)
	}
	return keys[0]
}

// assertLiveParquetRow asserts one delta parquet file holds exactly one
// version of the event's row and that it is live: deleted_at NULL (the delta
// exporter emits raw cl.deleted_at), changed_at equal to the event's
// read-back change_log timestamp, and the pinned title/count attributes
// equal to the event payload.
func assertLiveParquetRow(ctx context.Context, t *testing.T, env *Env, key string, ev *Event) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT "title", "count", "changed_at", "deleted_at"
		 FROM read_parquet('%s') WHERE CAST(row_id AS VARCHAR) = ?`, path), ev.RowID.String())
	if err != nil {
		t.Fatalf("scan delta parquet %s: %v", key, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var title sql.NullString
		var count sql.NullInt32
		var changedAt, deletedAt sql.NullInt64
		if err := rows.Scan(&title, &count, &changedAt, &deletedAt); err != nil {
			t.Fatalf("scan delta parquet row: %v", err)
		}
		if deletedAt.Valid {
			t.Errorf("delta %s.deleted_at = %d, want NULL (live row)", ev.RowID, deletedAt.Int64)
		}
		if !changedAt.Valid || changedAt.Int64 != ev.ChangedAt {
			t.Errorf("delta %s.changed_at = %d (valid=%t), want %d", ev.RowID, changedAt.Int64, changedAt.Valid, ev.ChangedAt)
		}
		wantTitle := ev.Attrs["title"].(string)
		if !title.Valid || title.String != wantTitle {
			t.Errorf("delta %s.title = %q (valid=%t), want %q", ev.RowID, title.String, title.Valid, wantTitle)
		}
		wantCount := int32(ev.Attrs["count"].(float64))
		if !count.Valid || count.Int32 != wantCount {
			t.Errorf("delta %s.count = %d (valid=%t), want %d", ev.RowID, count.Int32, count.Valid, wantCount)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("delta parquet rows: %v", err)
	}
	if n != 1 {
		t.Errorf("delta parquet holds %d versions of row %s, want exactly 1", n, ev.RowID)
	}
}
