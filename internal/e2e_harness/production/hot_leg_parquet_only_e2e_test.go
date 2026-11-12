//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The parquet leg of the pgx pass (plan step 3, #854), timed beside the
// attach-path baseline (#851). For each shape the engine just ran, it reads
// the dirty set the attach statement's dirty_ids reads, over pgx in one
// snapshot, stages it through the Appender on the replayer's connection, and
// runs the parquet-only statement over the same parquet objects. The hot
// leg's own PostgreSQL reads (the hot keys and the page's hot rows, plan
// step 4) are not built yet, so this times the DuckDB side of a pgx pass and
// the dirty set's transport, not a whole pass.

// parquetOnlyTiming is one shape's medians.
type parquetOnlyTiming struct {
	dirtyIDs                   int
	snapshotRead, stage, query time.Duration
	parquetTotal               int64
}

// measureParquetOnly times baselineReps rounds of read, stage and statement
// after one warm-up round, and checks the statement's count against the
// engine's: the parquet leg answers the engine's rows less the hot ones, and
// the hot ones are dirty.
func (r *baselineReplayer) measureParquetOnly(ctx context.Context, t *testing.T, env *Env, first *QueryResult, stmt string) parquetOnlyTiming {
	t.Helper()
	cutoff, err := hotLegCutoff(stmt)
	if err != nil {
		t.Fatalf("read the statement's flush cutoff: %v", err)
	}
	fq := first.FQ
	// Every measured shape is a first page, whose window is [0, limit)
	// whatever the hot match count.
	window, err := sqlgen.OffsetPageParquetWindow(fq.Offset, fq.Limit, 0)
	if err != nil {
		t.Fatalf("parquet window: %v", err)
	}
	pstmt, pargs, err := env.Engine().RenderParquetOnlyStatement(ctx, env.Tables, fq, window)
	if err != nil {
		t.Fatalf("render the parquet-only statement: %v", err)
	}
	fetcher := fedengine.NewPostgresDirtyIDFetcher(env.Pool)
	req := fedengine.ChangeLogSnapshotRequest{Tables: env.Tables, SchemaID: fq.SchemaID, Form: fedengine.DirtySetHot, GraceCutoffMs: cutoff}

	var reads, stages, queries []time.Duration
	var out parquetOnlyTiming
	for rep := 0; rep <= baselineReps(t); rep++ {
		start := time.Now()
		snap, err := fetcher.ReadChangeLogSnapshot(ctx, req)
		if err != nil {
			t.Fatalf("read the dirty set: %v", err)
		}
		read := time.Now()
		if err := fedengine.StageDirtySet(ctx, r.conn, snap.DirtyIDs); err != nil {
			t.Fatalf("stage the dirty set: %v", err)
		}
		staged := time.Now()
		rows, err := r.conn.QueryContext(ctx, pstmt, pargs...)
		if err != nil {
			t.Fatalf("parquet-only statement: %v", err)
		}
		total, err := drainReplay(rows)
		if err != nil {
			t.Fatalf("parquet-only statement: %v", err)
		}
		done := time.Now()
		if rep > 0 { // the first round is the warm-up
			reads = append(reads, read.Sub(start))
			stages = append(stages, staged.Sub(read))
			queries = append(queries, done.Sub(staged))
		}
		out.dirtyIDs, out.parquetTotal = len(snap.DirtyIDs), total
	}
	if err := fedengine.DropStagedDirtySet(ctx, r.conn); err != nil {
		t.Fatalf("drop the staged dirty set: %v", err)
	}
	if hot := first.Total - out.parquetTotal; hot < 0 || hot > int64(out.dirtyIDs) {
		t.Fatalf("the parquet leg counts %d rows and the engine %d: the difference must be hot rows, at most the %d dirty ones",
			out.parquetTotal, first.Total, out.dirtyIDs)
	}
	out.snapshotRead, out.stage, out.query = median(reads), median(stages), median(queries)
	return out
}
