package federated

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func TestValidateKeysetCursor(t *testing.T) {
	col := func(attr string) model.KeysetColumn {
		return model.KeysetColumn{Attribute: attr, Direction: forma.SortOrderAsc}
	}
	cursorOn := func(attrs ...string) *model.KeysetCursor {
		cols := make([]model.KeysetColumn, 0, len(attrs))
		vals := make([]interface{}, 0, len(attrs))
		for _, a := range attrs {
			cols = append(cols, col(a))
			vals = append(vals, "v")
		}
		return &model.KeysetCursor{Columns: cols, Values: vals, Mode: model.KeysetCursorModeAfter}
	}

	cases := []struct {
		name    string
		cursor  *model.KeysetCursor
		wantErr string // "" means accepted
	}{
		// The four system columns the visible CTE actually projects.
		{"row_id alone", cursorOn("row_id"), ""},
		{"created_at then row_id", cursorOn("created_at", "row_id"), ""},
		{"ver_ts then row_id", cursorOn("ver_ts", "row_id"), ""},
		{"deleted_ts then row_id", cursorOn("deleted_ts", "row_id"), ""},

		// Attribute columns: admitted, and the dotted form is the #260 case
		// the old code emitted verbatim against a folded CTE column.
		{"plain attribute", cursorOn("count", "row_id"), ""},
		{"dotted attribute folds to a safe identifier", cursorOn("contact.annualIncome", "row_id"), ""},
		{"spaced attribute folds to a safe identifier", cursorOn("annual income", "row_id"), ""},
		{"bracketed attribute folds to a safe identifier", cursorOn("a[0]", "row_id"), ""},

		// #381 amendment: the three names the old allowlist wrongly blessed are
		// ordinary attribute names now. They are ACCEPTED here and fail at
		// DuckDB's binder like any other unknown column — the false claim of
		// support is what this change removes, not the acceptance.
		{"updated_at is no longer blessed but is well-formed", cursorOn("updated_at", "row_id"), ""},
		{"schema_id is no longer blessed but is well-formed", cursorOn("schema_id", "row_id"), ""},

		// Dedup machinery: real columns of visible, never a caller's. The
		// lookup folds first, so the punctuation that ParquetAttrColumn
		// erases cannot smuggle a cursor onto the dedup rank.
		{"rn is rejected", cursorOn("rn", "row_id"), `keyset cursor column "rn"`},
		{"source_tier_priority is rejected", cursorOn("source_tier_priority", "row_id"),
			`keyset cursor column "source_tier_priority"`},
		{"dotted name folding onto source_tier_priority is rejected",
			cursorOn("source_tier.priority", "row_id"), `keyset cursor column "source_tier.priority"`},
		{"bracketed name folding onto rn is rejected", cursorOn("[rn]", "row_id"),
			`keyset cursor column "[rn]"`},

		// The ParquetAttrColumn fallback: every attribute the fold empties
		// lands on the literal "attr", which would silently retarget the
		// cursor at a real attribute of that name.
		{"empty attribute is rejected", cursorOn("", "row_id"), `folds to the placeholder "attr"`},
		{"bracket-only attribute is rejected", cursorOn("[]", "row_id"), `folds to the placeholder "attr"`},
		{"backtick-only attribute is rejected", cursorOn("`", "row_id"), `folds to the placeholder "attr"`},
		{"an attribute genuinely named attr is accepted", cursorOn("attr", "row_id"), ""},

		// Injection barrier (#381 item 2).
		{"quote is rejected", cursorOn(`a"b`, "row_id"), "is not a safe SQL identifier"},
		{"semicolon is rejected", cursorOn("a;DROP TABLE t", "row_id"), "is not a safe SQL identifier"},
		{"paren is rejected", cursorOn("count(*)", "row_id"), "is not a safe SQL identifier"},
		{"leading digit is rejected", cursorOn("1st", "row_id"), "is not a safe SQL identifier"},

		// Shape rules reach callers through this one entry point.
		{"missing trailing row_id", cursorOn("created_at"), `expected "row_id"`},
		{
			"misaligned values",
			&model.KeysetCursor{
				Columns: []model.KeysetColumn{col("created_at"), col("row_id")},
				Values:  []interface{}{"v"},
				Mode:    model.KeysetCursorModeAfter,
			},
			"carries 2 column(s) but 1 value(s)",
		},

		// The open first page carries no obligation.
		{"nil cursor", nil, ""},
		{"empty cursor", &model.KeysetCursor{Mode: model.KeysetCursorModeAfter}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateKeysetCursor(tc.cursor)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateKeysetCursor() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateKeysetCursor() = nil, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("validateKeysetCursor() = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
			if errors.Is(err, forma.ErrInvalidInput) {
				t.Error("keyset validation is a read-path error, never a write-path ErrInvalidInput carrier")
			}
		})
	}
}

// TestBothSeamsShareOneCursorValidator pins #381 item 1: the engine seam and
// the paginated seam must refuse exactly the same cursors. Before this change
// Query accepted arbitrary attribute columns that ExecuteFederatedPaginatedQuery
// rejected, and neither checked value alignment.
func TestBothSeamsShareOneCursorValidator(t *testing.T) {
	bad := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(5)}, // one value short
		Mode:   model.KeysetCursorModeAfter,
	}
	tables := model.StorageTables{EntityMain: "main", EAVData: "eav"}
	newQuery := func() *model.FederatedAttributeQuery {
		return &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			KeysetCursor:   bad,
		}
	}

	engine := NewDBFederatedQueryEngine(
		&fakePostgresFederatedSource{page: &model.PersistentRecordPage{}},
		nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	_, queryErr := engine.Query(context.Background(), tables, newQuery(), &model.FederatedQueryOptions{})
	require.Error(t, queryErr, "engine seam must refuse a misaligned cursor")
	require.Contains(t, queryErr.Error(), "carries 2 column(s) but 1 value(s)")

	_, _, pageErr := engine.ExecuteFederatedPaginatedQuery(
		context.Background(), tables, newQuery(), 10, 0, nil, &model.FederatedQueryOptions{})
	require.Error(t, pageErr, "paginated seam must refuse the same cursor")
	require.Contains(t, pageErr.Error(), "carries 2 column(s) but 1 value(s)")
}

// TestPaginatedQueryTakesKeysetPathOnCursorAlone pins #381 item 3. The keyset
// branch used to require opts.KeysetEnabled as well as a cursor; with the flag
// unset, control fell into the in-memory merge, where RunOptimizedQuery
// applies no cursor while the DuckDB leg does (the renderer keys off
// q.KeysetCursor, never off the flag). The merged page was then
// hot-rows-unfiltered union cold-rows-filtered — the same silent-wrong-answer
// family as #354. An active cursor alone now selects the keyset path.
func TestPaginatedQueryTakesKeysetPathOnCursorAlone(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
		KeysetCursor: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				// No trailing row_id: the keyset path validates and refuses,
				// while the in-memory merge path would have run happily. The
				// refusal is therefore proof of which branch was taken,
				// without needing a live DuckDB.
			},
			Values: []interface{}{int64(5)},
			Mode:   model.KeysetCursorModeAfter,
		},
	}

	_, _, err := engine.ExecuteFederatedPaginatedQuery(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		fq, 10, 0, nil, &model.FederatedQueryOptions{})

	require.Error(t, err, "an active cursor alone must select the keyset path")
	require.Contains(t, err.Error(), `expected "row_id"`)
	require.Zero(t, pg.runOptimizedCalls,
		"the keyset path runs, not the in-memory merge path which calls RunOptimizedQuery")
}
