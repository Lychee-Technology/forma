//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The hot-leg baseline (#851) needs entity_main at sizes the EntityManager
// cannot write in a test's time: it creates about 34 rows a second on the
// harness. So the loader writes a few hundred template rows through the
// real EntityManager, then clones them in SQL under new row IDs, copying
// each template's entity_main row and its eav_data rows verbatim. Every
// stored image is one the write path produced. The clones' change_log
// entries are unflushed, so the real CDC flush exports them.
//
// Only the title and count columns vary per clone (text_01 and integer_01
// in e2e_wide_attributes.json): titles stay unique, so an equality filter
// on one selects one row, and count stays a unique sort key. Every other
// value repeats with the template period, which flatters parquet's
// dictionary encoding; the measurement compares passes at equal data, so
// that bias is shared by every number it reports.

// baselineCloneChunk bounds one clone transaction.
const baselineCloneChunk = 50_000

// baselineTemplateTable holds the template row IDs, numbered from 0, in the
// test's private database. No federated read touches it.
const baselineTemplateTable = "hot_leg_baseline_templates"

// seedBaselineTemplates writes the template rows through the EntityManager
// and records their IDs for cloneBaselineRows.
func seedBaselineTemplates(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: schema, Creates: baselineTemplates})...); err != nil {
		t.Fatalf("write template rows: %v", err)
	}
	env.ExecSQL(ctx, fmt.Sprintf(`CREATE TABLE %s AS
		SELECT ltbase_row_id AS src, (row_number() OVER (ORDER BY ltbase_row_id) - 1)::bigint AS ord
		FROM entity_main WHERE ltbase_schema_id = $1`, baselineTemplateTable), schema.ID)
}

// cloneBaselineRows adds n clones of the template rows, numbering them on
// from first. It returns the next unused clone number.
func cloneBaselineRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, first, n int64) int64 {
	t.Helper()
	insertMain := baselineCloneMainSQL(ctx, t, env)
	for done := int64(0); done < n; {
		size := min(baselineCloneChunk, n-done)
		from := first + done
		err := pgx.BeginFunc(ctx, env.Pool, func(tx pgx.Tx) error {
			return cloneBaselineChunk(ctx, tx, insertMain, schema.ID, from, from+size-1)
		})
		if err != nil {
			t.Fatalf("clone rows %d..%d: %v", from, from+size-1, err)
		}
		done += size
	}
	return first + n
}

// cloneBaselineChunk clones the rows numbered from..to in one transaction.
// Clone c copies template c mod baselineTemplates.
func cloneBaselineChunk(ctx context.Context, tx pgx.Tx, insertMain string, schemaID int16, from, to int64) error {
	stmts := []string{
		fmt.Sprintf(`CREATE TEMP TABLE clone_map ON COMMIT DROP AS
			SELECT gen_random_uuid() AS dst, tp.src, g / %d + 1 AS k
			FROM generate_series($1::bigint, $2::bigint) g
			JOIN %s tp ON tp.ord = g %% %d`, baselineTemplates, baselineTemplateTable, baselineTemplates),
		insertMain,
		`INSERT INTO eav_data (schema_id, row_id, attr_id, array_indices, value_text, value_numeric)
			SELECT e.schema_id, c.dst, e.attr_id, e.array_indices, e.value_text, e.value_numeric
			FROM clone_map c JOIN eav_data e ON e.schema_id = $1 AND e.row_id = c.src`,
		`INSERT INTO change_log (schema_id, row_id, changed_at, deleted_at, flushed_at)
			SELECT m.ltbase_schema_id, c.dst, m.ltbase_updated_at, NULL, 0
			FROM clone_map c JOIN entity_main m ON m.ltbase_schema_id = $1 AND m.ltbase_row_id = c.src`,
	}
	args := [][]any{{from, to}, {schemaID}, {schemaID}, {schemaID}}
	for i, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt, args[i]...); err != nil {
			return fmt.Errorf("clone statement %d: %w", i, err)
		}
	}
	return nil
}

// baselineCloneMainSQL renders the entity_main insert of a clone chunk over
// the table's actual columns, so a column the DDL gains is copied too.
func baselineCloneMainSQL(ctx context.Context, t *testing.T, env *Env) string {
	t.Helper()
	rows, err := env.Pool.Query(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'entity_main' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("list entity_main columns: %v", err)
	}
	cols, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("list entity_main columns: %v", err)
	}
	exprs := make([]string, len(cols))
	for i, col := range cols {
		switch col {
		case "ltbase_row_id":
			exprs[i] = "c.dst"
		case "text_01": // title
			exprs[i] = "m.text_01 || '-c' || c.k"
		case "integer_01": // count, below 10 * baselineTemplates in every template
			exprs[i] = fmt.Sprintf("m.integer_01 + c.k * %d", 10*baselineTemplates)
		default:
			exprs[i] = "m." + pgx.Identifier{col}.Sanitize()
		}
	}
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = pgx.Identifier{col}.Sanitize()
	}
	return fmt.Sprintf(`INSERT INTO entity_main (%s) SELECT %s
		FROM clone_map c JOIN entity_main m ON m.ltbase_schema_id = $1 AND m.ltbase_row_id = c.src`,
		strings.Join(quoted, ", "), strings.Join(exprs, ", "))
}

// baselineHotSet writes the fixed hot set through the EntityManager: half
// updates of flushed rows, which have parquet history the anti-join must
// discard, and half creates. The updated rows are drawn from the templates
// and their first clones, spread evenly.
func baselineHotSet(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, size int) {
	t.Helper()
	rows, err := env.Pool.Query(ctx, `SELECT ltbase_row_id FROM entity_main
		WHERE ltbase_schema_id = $1 ORDER BY ltbase_row_id LIMIT $2`, schema.ID, size/2)
	if err != nil {
		t.Fatalf("pick rows to update: %v", err)
	}
	targets, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		t.Fatalf("pick rows to update: %v", err)
	}
	creates := env.GenerateScript(ScriptSpec{Schema: schema, Creates: size - len(targets)})
	updates := env.GenerateScript(ScriptSpec{Schema: schema, Creates: len(targets)})
	for i, ev := range updates {
		updates[i] = UpdateEvent(schema, targets[i], ev.Attrs)
	}
	if err := env.ApplyEvents(ctx, append(creates, updates...)...); err != nil {
		t.Fatalf("write hot set: %v", err)
	}
}

// flushAndConsolidate flushes every unflushed row, then rewrites the schema's
// parquet as base files with a delta purge, so each size is read from the
// layout compaction leaves rather than from one delta object per flush batch.
func flushAndConsolidate(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	for {
		report, err := env.RunFlush(ctx)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		if report.UnflushedAfter == 0 {
			break
		}
		if report.UnflushedAfter >= report.UnflushedBefore {
			t.Fatalf("flush made no progress: %d unflushed rows before and %d after", report.UnflushedBefore, report.UnflushedAfter)
		}
	}
	cfg := env.CDC
	cfg.TargetFileSizeMB = 128
	if _, err := env.RunInitWith(ctx, schema, InitOverrides{Config: &cfg, ReplaceDelta: true}); err != nil {
		t.Fatalf("consolidate parquet into base: %v", err)
	}
}
