//go:build e2e

package production

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The parquet leg of the pgx pass (plan step 3, #854), timed beside the
// engine pass. For each shape the engine just ran, it reads the dirty set
// the pass read, at the pass's cutoff, over pgx in one snapshot, stages it
// through the Appender on a pinned connection of the Env's DuckDB instance,
// and runs the parquet-only statement over the same parquet objects. The
// hot leg's own PostgreSQL reads (the hot keys and the page's hot rows) are
// not timed here: the engine pass includes them.

// parquetOnlyTiming is one shape's medians.
type parquetOnlyTiming struct {
	dirtyIDs                   int
	snapshotRead, stage, query time.Duration
	parquetTotal               int64
}

// measureParquetOnly times baselineReps rounds of read, stage and statement
// on conn after one warm-up round, and checks the statement's count against
// the engine's: the parquet leg answers the engine's rows less the hot ones,
// and the hot ones are dirty.
func measureParquetOnly(ctx context.Context, t *testing.T, env *Env, conn *sql.Conn, first *QueryResult) parquetOnlyTiming {
	t.Helper()
	cutoff := passCutoff(t, first.Plan)
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
		if err := fedengine.StageDirtySet(ctx, conn, snap.DirtyIDs); err != nil {
			t.Fatalf("stage the dirty set: %v", err)
		}
		staged := time.Now()
		rows, err := conn.QueryContext(ctx, pstmt, pargs...)
		if err != nil {
			t.Fatalf("parquet-only statement: %v", err)
		}
		total, err := drainTotal(rows)
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
	if err := fedengine.DropStagedDirtySet(ctx, conn); err != nil {
		t.Fatalf("drop the staged dirty set: %v", err)
	}
	if hot := first.Total - out.parquetTotal; hot < 0 || hot > int64(out.dirtyIDs) {
		t.Fatalf("the parquet leg counts %d rows and the engine %d: the difference must be hot rows, at most the %d dirty ones",
			out.parquetTotal, first.Total, out.dirtyIDs)
	}
	out.snapshotRead, out.stage, out.query = median(reads), median(stages), median(queries)
	return out
}

// passCutoff returns the flush cutoff the pass read its dirty set at: the
// second parameter of the hot form's dirty-set query, which its execution
// plan records.
func passCutoff(t *testing.T, plan *model.ExecutionPlan) int64 {
	t.Helper()
	src, ok := planSource(plan, pgxDirtySetReason)
	if !ok || len(src.Params) != 2 {
		t.Fatalf("the plan records no hot-form dirty-set read with its cutoff: %+v", plan.Sources)
	}
	cutoff, err := strconv.ParseInt(src.Params[1], 10, 64)
	if err != nil {
		t.Fatalf("parse the dirty set's cutoff %q: %v", src.Params[1], err)
	}
	return cutoff
}
