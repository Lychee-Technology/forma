//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
)

// TestHarnessCarriesDeclaredIndexes pins that a production-harness database
// carries the indexes its fixtures declare (#647): e2e_outbox's due, lease and
// fifo become three valid, non-partial btree indexes keyed
// (ltbase_schema_id, cols…, ltbase_row_id), so an index-dependent assertion
// can run against a harness Env at all. The expected keys are independent
// literals, not rebuilt from the planner.
func TestHarnessCarriesDeclaredIndexes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))

	indexes := entityMainIndexes(ctx, t, env)
	for declaration, key := range map[string]string{
		"e2e_outbox.due":   "ltbase_schema_id, text_01, bigint_01, ltbase_row_id",
		"e2e_outbox.lease": "ltbase_schema_id, text_01, bigint_02, ltbase_row_id",
		"e2e_outbox.fifo":  "ltbase_schema_id, text_02, bigint_03, ltbase_row_id",
	} {
		name := declaredIndexFor(t, env, declaration).Name
		got, ok := indexes[name]
		if !ok {
			t.Fatalf("%s: entity_main has no index %s; it has %v", declaration, name, indexes)
		}
		want := fmt.Sprintf("CREATE INDEX %s ON public.entity_main USING btree (%s)", name, key)
		if got.def != want || !got.valid {
			t.Fatalf("%s: index %s is %q (valid %v), want %q and valid", declaration, name, got.def, got.valid, want)
		}
	}
}

const (
	backlogRows = 12_000 // e2e_outbox rows: 240 pending, 240 leased, the rest done
	otherRows   = 10_000 // e2e_second rows that would match every outbox filter
	dayMillis   = int64(24 * time.Hour / time.Millisecond)
	// fifoCutoff splits partition p7's enqueue times: rows past it are 30.
	fifoCutoff = 6_000
)

// TestDeclaredIndexesServeOutboxQueries is the issue's EXPLAIN test: with
// ≥10k rows of the declaring schema and 10k of another schema in entity_main,
// and init-db's per-column indexes present as the alternatives, the hot-tier
// SQL Forma generates for the due, lease-expired and FIFO shapes reads through
// the declared index, constraining both declared columns. Each shape also
// returns exactly the rows the seed puts in it, so the explained SQL is the
// SQL that answered.
func TestDeclaredIndexesServeOutboxQueries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	outbox := SchemaRef{ID: SchemaIDOutbox, Name: "e2e_outbox"}

	createPerColumnIndexes(ctx, env, "text_01", "text_02", "bigint_01", "bigint_02", "bigint_03")
	base := time.Now().Add(-30 * 24 * time.Hour).Truncate(time.Second).UnixMilli()
	seedOutboxBacklog(ctx, env, base)

	cutoff := time.UnixMilli(base + fifoCutoff*1000).UTC().Format(time.RFC3339)
	shapes := []struct {
		name        string
		declaration string
		condition   forma.Condition
		sorts       []Sort
		columns     []string
		want        int
	}{
		{"due", "e2e_outbox.due",
			and(eq("status", "pending"), forma.Now("next_attempt_at", forma.FilterLessEq, 0)),
			nil, []string{"text_01", "bigint_01"}, 120},
		{"due over a status set", "e2e_outbox.due",
			and(forma.In("status", "pending", "retry"), forma.Now("next_attempt_at", forma.FilterLessEq, 0)),
			nil, []string{"text_01", "bigint_01"}, 120},
		{"lease expired", "e2e_outbox.lease",
			and(eq("status", "leased"), forma.Now("locked_until", forma.FilterLessEq, 0)),
			nil, []string{"text_01", "bigint_02"}, 120},
		{"fifo", "e2e_outbox.fifo",
			and(eq("partition_key", "p7"), &forma.KvCondition{Attr: "enqueued_at", Value: "gt:" + cutoff}),
			[]Sort{{Attr: "enqueued_at"}}, []string{"text_02", "bigint_03"}, 30},
	}

	recorder := &recordingPool{Pool: env.Pool}
	repo := internal.NewDBPersistentRecordRepository(recorder, env.Metadata)
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			orders, err := env.buildAttributeOrders(Query{Schema: outbox, Sorts: s.sorts})
			if err != nil {
				t.Fatalf("build sort: %v", err)
			}
			page, err := repo.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
				Tables: env.Tables, SchemaID: outbox.ID, Condition: s.condition, AttributeOrders: orders, Limit: 1000,
			})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			assertShapeRows(t, page, outbox.ID, s.want, len(s.sorts) > 0)

			ix := declaredIndexFor(t, env, s.declaration)
			plan := explain(ctx, t, env.Pool, recorder.takeAnchorQuery(t))
			scans := indexScansOf(plan, ix.Name)
			if len(scans) == 0 {
				t.Fatalf("the generated query does not read through %s (%s); plan:\n%s", ix.Name, s.declaration, describePlan(plan, 0))
			}
			for _, col := range s.columns {
				if !anyIndexCondMentions(scans, col) {
					t.Fatalf("no scan of %s constrains %s; plan:\n%s", ix.Name, col, describePlan(plan, 0))
				}
			}
		})
	}
}

func eq(attr, value string) *forma.KvCondition {
	return &forma.KvCondition{Attr: attr, Value: string(forma.FilterEquals) + ":" + value}
}

func and(conditions ...forma.Condition) forma.Condition {
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: conditions}
}

func anyIndexCondMentions(scans []planNode, col string) bool {
	for _, n := range scans {
		if strings.Contains(n.IndexCond, col) {
			return true
		}
	}
	return false
}

func assertShapeRows(t *testing.T, page *model.PersistentRecordPage, schemaID int16, want int, fifo bool) {
	t.Helper()
	if len(page.Records) != want || page.TotalRecords != int64(want) {
		t.Fatalf("query returned %d records (total %d), want %d", len(page.Records), page.TotalRecords, want)
	}
	for i, rec := range page.Records {
		if rec.SchemaID != schemaID {
			t.Fatalf("record %s belongs to schema %d, want %d", rec.RowID, rec.SchemaID, schemaID)
		}
		if fifo && i > 0 && page.Records[i-1].Int64Items["bigint_03"] > rec.Int64Items["bigint_03"] {
			t.Fatalf("fifo records out of enqueue order at %d", i)
		}
	}
}

// createPerColumnIndexes adds init-db's per-column indexes (cmd/tools
// mainColumnIndexDDL), which the harness DDL leaves out, so the declared
// index has to win against what a production entity_main already offers.
func createPerColumnIndexes(ctx context.Context, env *Env, cols ...string) {
	for _, col := range cols {
		env.ExecSQL(ctx, fmt.Sprintf(`CREATE INDEX entity_main_%s_idx ON entity_main (ltbase_schema_id, ltbase_row_id, %s)`, col, col))
	}
}

// seedOutboxBacklog writes an LTFlow-shaped backlog straight into the hot
// tier and analyzes it. In e2e_outbox, i%50 = 0 is pending and i%50 = 1
// leased, the rest done; alternate fifty-row blocks fall a day in the past or
// the future, so 120 pending rows are due and 120 leases expired. Partition
// p<i%200> enqueues at base + i seconds. e2e_second gets rows that would match
// every outbox filter if the schema key were ignored. Done rows are flushed;
// the rest still sit in change_log as unflushed, as they would in production.
func seedOutboxBacklog(ctx context.Context, env *Env, base int64) {
	now := time.Now().UnixMilli()
	env.ExecSQL(ctx, `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, text_01, text_02, bigint_01, bigint_02, bigint_03,
		                         ltbase_created_at, ltbase_updated_at)
		SELECT $1::smallint, gen_random_uuid(),
		       CASE i % 50 WHEN 0 THEN 'pending' WHEN 1 THEN 'leased' ELSE 'done' END,
		       'p' || (i % 200),
		       $2::bigint + CASE WHEN (i / 50) % 2 = 0 THEN -$3::bigint ELSE $3::bigint END,
		       CASE WHEN i % 50 = 1 THEN $2::bigint + CASE WHEN (i / 50) % 2 = 0 THEN -$3::bigint ELSE $3::bigint END END,
		       $4::bigint + i::bigint * 1000,
		       $2::bigint, $2::bigint
		FROM generate_series(1, $5::int) AS i`,
		SchemaIDOutbox, now, dayMillis, base, backlogRows)
	env.ExecSQL(ctx, `
		INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, text_01, text_02, bigint_01, bigint_02, bigint_03,
		                         ltbase_created_at, ltbase_updated_at)
		SELECT $1::smallint, gen_random_uuid(), CASE i % 2 WHEN 0 THEN 'pending' ELSE 'leased' END, 'p7',
		       $2::bigint - $3::bigint, $2::bigint - $3::bigint, $4::bigint + i::bigint * 1000, $2::bigint, $2::bigint
		FROM generate_series(1, $5::int) AS i`,
		SchemaIDSecond, now, dayMillis, base+fifoCutoff*1000, otherRows)
	env.ExecSQL(ctx, `
		INSERT INTO change_log (schema_id, row_id, changed_at, flushed_at)
		SELECT ltbase_schema_id, ltbase_row_id, ltbase_updated_at, CASE WHEN text_01 = 'done' THEN $1::bigint ELSE 0 END
		FROM entity_main`, now)
	env.ExecSQL(ctx, `ANALYZE entity_main`)
	env.ExecSQL(ctx, `ANALYZE change_log`)
}
