package federated

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// What a successful pgx pass reports: the fed_query_* series and the
// execution plan's sources.

// hotLegPgxReason names the plan source of the hot leg: PostgreSQL evaluates
// its predicate.
const hotLegPgxReason = "hot-leg predicate (evaluated in PostgreSQL)"

// pgxOutcome is what one successful pgx pass measured.
type pgxOutcome struct {
	leg parquetLeg
	// dirtyRows is the size of the dirty set the snapshot read and the pass
	// staged; hotMatches the hot keys the page drew from (after the cursor);
	// hotOnPage the page's rows the hot leg served.
	dirtyRows, hotMatches, hotOnPage int
}

// report emits the pass's metrics and completes its execution plan. A pass
// that ran no DuckDB statement (#720) has no translation, execution or
// streaming stage and no duckdb row count, so those samples are not emitted.
func (p *pgxPass) report(ctx context.Context, o pgxOutcome) {
	m := p.e.metrics
	m.EmitLatency(ctx, "snapshot", p.snapshotMs)
	m.EmitRowCount(ctx, "pg", int64(o.dirtyRows))
	m.EmitRowCount(ctx, "hot", int64(o.hotMatches))
	if !p.noObjects {
		m.EmitLatency(ctx, "translation", o.leg.translateMs)
		m.EmitLatency(ctx, "execution", o.leg.executeMs)
		m.EmitLatency(ctx, "streaming", o.leg.streamMs)
		m.EmitRowCount(ctx, "duckdb", int64(len(o.leg.rows)))
	}
	p.recordPlan(o)
}

// recordPlan records the parquet leg's statement and outcome, then the hot
// leg as a PostgreSQL source whose predicate PostgreSQL evaluated.
func (p *pgxPass) recordPlan(o pgxOutcome) {
	c := p.planCtx
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	plan := c.opts.ExecutionPlan
	if !p.noObjects {
		c.recordTranslation(o.leg.sqlStr, o.leg.args, o.leg.translateMs, p.q)
		c.recordScanOutcome(duckDBScanOutcome{
			translateMs: o.leg.translateMs,
			executeMs:   o.leg.executeMs,
			streamMs:    o.leg.streamMs,
			rowCount:    int64(len(o.leg.rows)),
			dirtyRows:   int64(o.dirtyRows),
		})
	}
	if p.hasHot {
		plan.Sources = append(plan.Sources, model.DataSourcePlan{
			Tier:              model.DataTierHot,
			Engine:            "postgres",
			SQL:               fmt.Sprintf("hot-keys query over %s in the pass's snapshot", sqlutil.SanitizeIdentifier(p.tables.EntityMain)),
			PredicatePushdown: true,
			RowEstimate:       int64(o.hotMatches),
			ActualRows:        int64(o.hotOnPage),
			Reason:            hotLegPgxReason,
		})
	}
	plan.Timings["pg_snapshot"] = p.snapshotMs
	plan.Timings["total"] = c.millisSince(c.startTotal)
}

// recordSnapshotDirtySet records the dirty set the snapshot read: the
// widened set of the hot form at the pass's cutoff, or the strict set of the
// hot-excluded form, which the parquet leg anti-joins against.
func (c *duckDBExecutionPlanContext) recordSnapshotDirtySet(changeLog string, schemaID int16, form DirtySetForm, cutoffMs int64, n int) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	query, args := dirtySetQuery(ChangeLogSnapshotRequest{
		Tables:        model.StorageTables{ChangeLog: changeLog},
		SchemaID:      schemaID,
		Form:          form,
		GraceCutoffMs: cutoffMs,
	})
	reason := "dirty set read in the pass's snapshot and staged for the anti-join"
	if form == DirtySetHotExcluded {
		reason = "consistency barrier (dirty set read in the pass's snapshot, staged for the anti-join)"
	}
	c.opts.ExecutionPlan.Sources = append(c.opts.ExecutionPlan.Sources, model.DataSourcePlan{
		Tier:        model.DataTierHot,
		Engine:      "postgres",
		SQL:         query,
		Params:      formatPlanParams(args),
		RowEstimate: int64(n),
		Reason:      reason,
	})
}
