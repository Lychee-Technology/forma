//go:build e2e

package production

import (
	"context"
	"slices"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestTextSortFollowsTheColumnCollation records the observable change #816
// makes beyond ranges. #728 kept sort order out of its byte-order ruling: the
// Postgres route sorts with ORDER BY on the column, so it sorts under the
// column's collation, while the DuckDB route sorts VARCHAR by bytes. With
// Forma's columns under C both routes sort by bytes, column-bound (title) and
// eav_data (note) text alike, ascending and descending: B, Z, a, zebra, é. A
// table from before the contract still sorts under the database default on
// the Postgres route, which this test pins too, so the difference stays
// visible rather than assumed away. A keyset page runs on the DuckDB route
// whatever the preference, so it follows bytes either way.
func TestTextSortFollowsTheColumnCollation(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	requireNonCDatabaseDefault(ctx, t, env)
	seedByteOrderFiller(ctx, t, env, wide)
	rows := seedByteOrderRows(ctx, t, env, wide, byteOrderTextValues, "title", "note")

	byteOrder := []string{"B", "Z", "a", "zebra", "é"}
	for _, route := range byteOrderRoutes {
		for _, attr := range []string{"title", "note"} {
			assertSortedRows(ctx, t, env, "C/"+route.name, route.base(wide), attr, rows, byteOrder)
		}
	}
	assertTitlePageAfterZ(ctx, t, env, wide, rows)

	// A pre-#816 table: both columns follow the database default again.
	env.ExecSQL(ctx, `ALTER TABLE entity_main ALTER COLUMN text_01 TYPE text COLLATE "default"`)
	env.ExecSQL(ctx, `ALTER TABLE eav_data ALTER COLUMN value_text TYPE text COLLATE "default"`)
	// A pooled connection's prepared statements now fail with "cached plan
	// must not change result type" (0A000), as a running server's would after
	// a migration: docs/text-collation.md has the operator restart them.
	env.Pool.Reset()
	var linguistic []string
	if err := env.Pool.QueryRow(ctx, `SELECT array_agg(v ORDER BY v) FROM unnest($1::text[]) AS v`, byteOrderTextValues).Scan(&linguistic); err != nil {
		t.Fatalf("order the values under the database default: %v", err)
	}
	if slices.Equal(linguistic, byteOrder) {
		t.Fatalf("the database default orders %v as C does; this test needs one that does not", linguistic)
	}
	for _, route := range byteOrderRoutes {
		want := byteOrder
		if !route.useDuck {
			want = linguistic
		}
		for _, attr := range []string{"title", "note"} {
			assertSortedRows(ctx, t, env, "default/"+route.name, route.base(wide), attr, rows, want)
		}
	}
	assertTitlePageAfterZ(ctx, t, env, wide, rows)
}

// assertSortedRows sorts the rows holding attr by it, both ways, and checks
// the order of their stored values.
func assertSortedRows(ctx context.Context, t *testing.T, env *Env, label string, base Query, attr string,
	rows map[string]uuid.UUID, want []string) {
	t.Helper()
	value := make(map[uuid.UUID]string, len(rows))
	for v, id := range rows {
		value[id] = v
	}
	for _, desc := range []bool{false, true} {
		q := base
		q.Filters = []Filter{{Attr: attr, Op: "is_not_null", Value: "true"}}
		q.Sorts = []Sort{{Attr: attr, Desc: desc}}
		res, err := env.Query(ctx, q)
		if err != nil {
			t.Fatalf("%s: sort by %s (desc %v): %v", label, attr, desc, err)
		}
		got := make([]string, 0, len(res.Records))
		for _, r := range res.Records {
			got = append(got, value[r.RowID])
		}
		w := slices.Clone(want)
		if desc {
			slices.Reverse(w)
		}
		if !slices.Equal(got, w) {
			t.Errorf("%s: sort by %s (desc %v) = %q, want %q", label, attr, desc, got, w)
		}
	}
}

// assertTitlePageAfterZ reads the title page after "Z": a, zebra, é in byte order,
// on the hot rows, with the Postgres preference that a cursor overrides.
func assertTitlePageAfterZ(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, rows map[string]uuid.UUID) {
	t.Helper()
	res, err := env.Query(ctx, Query{
		Schema: wide, PreferredTiers: allTiers, Limit: 10,
		Sorts: []Sort{{Attr: "title"}},
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "title", Direction: forma.SortOrderAsc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
			Values: []any{"Z", rows["Z"].String()},
			Mode:   model.KeysetCursorModeAfter,
		},
	})
	if err != nil {
		t.Fatalf("keyset page after Z: %v", err)
	}
	var got []uuid.UUID
	for _, r := range res.Records {
		got = append(got, r.RowID)
	}
	if want := []uuid.UUID{rows["a"], rows["zebra"], rows["é"]}; !slices.Equal(got, want) {
		t.Errorf("keyset page after Z = %v, want a, zebra, é: %v", got, want)
	}
}
