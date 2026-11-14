//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// keysetWalk pages through q from start, limit rows at a time, comparing
// every page, and returns the walked row IDs. next builds the cursor after a
// page's last row; between, when set, runs after each page.
func (d *dualRun) keysetWalk(ctx context.Context, t *testing.T, label string, q Query, start *model.KeysetCursor, limit int,
	next func(*model.PersistentRecord) *model.KeysetCursor, between func(page int)) []string {
	t.Helper()
	var walked []string
	cursor := start
	for page := 0; page < 1000; page++ {
		q.Keyset, q.Limit = cursor, limit
		res := d.answer(ctx, t, fmt.Sprintf("%s, page %d", label, page), q)
		if len(res.Records) == 0 {
			return walked
		}
		walked = append(walked, pageRowIDs(res)...)
		cursor = next(res.Records[len(res.Records)-1])
		if between != nil {
			between(page)
		}
	}
	t.Fatalf("%s: the walk did not end", label)
	return nil
}

// TestDualRunTieRunWalks: one run of rows that tie on created_at and count,
// flushed, then every other row by row_id updated, so the run's hot and
// parquet rows interleave by row_id (tieRun). The walks of the step 5
// mixed-tier test, one and two rows per page, keyset under created_at DESC
// with both row_id directions and under count ASC, and offset pages in the
// default order and by count, must answer the same under both transports.
func TestDualRunTieRunWalks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	const rows = 7
	tieRun(ctx, t, env, wide, rows)
	d := newDualRun(env)
	for _, w := range tieWalks() {
		for _, limit := range []int{1, 2} {
			label := fmt.Sprintf("%s/limit=%d", w.name, limit)
			q := Query{Schema: wide, Sorts: w.sorts, PreferredTiers: allTiers}
			var walked []string
			if w.cursor == nil {
				walked = d.offsetWalk(ctx, t, label, q, limit)
			} else {
				start := &model.KeysetCursor{Columns: w.cursor, Values: w.start, Mode: model.KeysetCursorModeAfter}
				walked = d.keysetWalk(ctx, t, label, q, start, limit,
					func(rec *model.PersistentRecord) *model.KeysetCursor { return tieCursor(w.cursor, rec) }, nil)
			}
			if len(walked) != rows {
				t.Fatalf("%s: walked %d rows, want the run's %d once each", label, len(walked), rows)
			}
		}
	}
}

// TestDualRunDeletedTS: the step 2 fixture, live rows in every deleted_ts
// encoding (a pre-#274 delta row, a row exported since #274, a hot row),
// with the step 2 walks and boundary cases. The walks flush the hot rows
// between their second and third pages, under both transports at once.
func TestDualRunDeletedTS(t *testing.T) {
	t.Parallel()
	for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
		t.Run("walk "+string(dir), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			env := NewEnv(t, SharedCluster(t))
			f := seedDeletedTSFixture(ctx, t, env)
			start := uuid.Nil
			if dir == forma.SortOrderDesc {
				start = uuid.Max
			}
			walked := newDualRun(env).keysetWalk(ctx, t, "deleted_ts "+string(dir), Query{Schema: f.schema}, deletedTSCursor(dir, 0, start), 1,
				func(rec *model.PersistentRecord) *model.KeysetCursor { return deletedTSCursor(dir, 0, rec.RowID) },
				func(page int) {
					if page == 1 {
						mustFlush(ctx, t, env)
					}
				})
			if want := f.ids(dir == forma.SortOrderDesc); !equalStrings(walked, want) {
				t.Fatalf("walk %s = %v, want every row once in row_id order %v", dir, walked, want)
			}
		})
	}

	t.Run("boundaries and offset form", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		env := NewEnv(t, SharedCluster(t))
		f := seedDeletedTSFixture(ctx, t, env)
		d := newDualRun(env)
		all := f.ids(false)
		pivot := uuid.MustParse(all[len(all)/2])
		for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
			for _, boundary := range []int64{-1, 0, 1} {
				d.answer(ctx, t, fmt.Sprintf("%s at %d", dir, boundary), Query{Schema: f.schema, Limit: 100, Keyset: deletedTSCursor(dir, boundary, pivot)})
			}
		}
		ordered := &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderAsc},
				{Attribute: "deleted_ts", Direction: forma.SortOrderAsc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
			Values: []any{deletedTSCreatedAt - 1, int64(0), uuid.Nil},
			Mode:   model.KeysetCursorModeAfter,
		}
		if got := pageRowIDs(d.answer(ctx, t, "created_at, deleted_ts, row_id", Query{Schema: f.schema, Limit: 100, Keyset: ordered})); !equalStrings(got, all) {
			t.Fatalf("created_at, deleted_ts, row_id: page %v, want %v", got, all)
		}

		var walked []string
		for offset := 0; offset < 2*len(f.encoding); offset += 2 {
			if offset == 4 {
				mustFlush(ctx, t, env)
			}
			res := d.answer(ctx, t, fmt.Sprintf("offset %d", offset), Query{Schema: f.schema, Limit: 2, Offset: offset, PreferredTiers: allTiers})
			if len(res.Records) == 0 {
				break
			}
			walked = append(walked, pageRowIDs(res)...)
		}
		if !equalStrings(walked, all) {
			t.Fatalf("offset walk = %v, want every row once %v", walked, all)
		}
	})
}

// TestDualRunRacedFlush: a flush that appends and marks between path
// resolution and the snapshot, with the default flush-grace widening. Both
// transports resolve their paths, both pause, one flush runs, and both
// resume, so both answer over the same raced state: the raced rows must be
// on both pages, at their raced versions.
func TestDualRunRacedFlush(t *testing.T) {
	t.Parallel()
	for name, raced := range map[string]func(SchemaRef, []*Event) []*Event{"NewRows": racedCreates, "UpdatedRows": buildClosedUpdates} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			env := NewEnv(t, SharedCluster(t))
			wide := DefaultSchemaFixtures()[1]
			gen1 := buildOpenCreates(wide, 4)
			mustApplyEvents(ctx, t, env, "seed generation 1", gen1...)
			mustFlush(ctx, t, env)
			mustApplyEvents(ctx, t, env, "seed generation 2", raced(wide, gen1)...)

			// Set after seeding, which assembled the Env's own engine: only
			// the two engines below pause, each at its first path resolution.
			var pausers []*pausingParquetSource
			env.ParquetSourceWrap = func(src fedengine.ParquetSource) fedengine.ParquetSource {
				p := newPausingParquetSource()
				p.inner = src
				pausers = append(pausers, p)
				return p
			}
			d := newDualRun(env)
			attach, pgx := runRacedPair(ctx, t, env, d, pausers, Query{Schema: wide, Limit: 10})
			requireSamePage(t, env, wide, "raced flush", attach, pgx)
			want := map[string]int{"NewRows": 8, "UpdatedRows": 4}[name]
			if len(pgx.Records) != want {
				t.Fatalf("the raced page holds %d rows, want %d", len(pgx.Records), want)
			}
			if name != "UpdatedRows" {
				return
			}
			for _, rec := range pgx.Records {
				if title := recordTitle(t, env, wide, rec); !strings.HasPrefix(title, "closed-") {
					t.Errorf("row %s reads title %q, want its raced update closed-*", rec.RowID, title)
				}
			}
		})
	}
}

// runRacedPair runs q on both of d's engines at once, flushes once both
// have resolved their paths, and returns the two answers.
func runRacedPair(ctx context.Context, t *testing.T, env *Env, d *dualRun, pausers []*pausingParquetSource, q Query) (attach, pgx *QueryResult) {
	t.Helper()
	if len(pausers) != 2 {
		t.Fatalf("want one pausing source per transport, got %d", len(pausers))
	}
	type outcome struct {
		res *QueryResult
		err error
	}
	qctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
	defer cancel()
	outcomes := make([]chan outcome, 2)
	for i, engine := range []*fedengine.DBFederatedQueryEngine{d.attach, d.pgx} {
		outcomes[i] = make(chan outcome, 1)
		fq, err := env.buildFederatedQuery(q)
		if err != nil {
			t.Fatalf("build the raced query: %v", err)
		}
		go func() {
			opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
			page, err := engine.Query(qctx, env.Tables, fq, opts)
			if err != nil {
				outcomes[i] <- outcome{err: err}
				return
			}
			outcomes[i] <- outcome{res: &QueryResult{Spec: q, Records: page.Records, Total: page.TotalRecords, Plan: opts.ExecutionPlan, FQ: fq}}
		}()
	}
	defer func() {
		for _, p := range pausers {
			p.Release()
		}
	}()
	for _, p := range pausers {
		select {
		case <-p.Reached():
		case <-qctx.Done():
			t.Fatalf("a raced query did not reach path resolution: %v", qctx.Err())
		}
	}
	mustFlush(ctx, t, env)
	for _, p := range pausers {
		p.Release()
	}
	results := make([]*QueryResult, 2)
	for i, ch := range outcomes {
		out := <-ch
		if out.err != nil {
			t.Fatalf("raced query %d: %v", i, out.err)
		}
		if !out.res.Plan.Routing.UseDuckDB {
			t.Fatalf("raced query %d did not route to DuckDB: %+v", i, out.res.Plan.Routing)
		}
		results[i] = out.res
	}
	return results[0], results[1]
}
