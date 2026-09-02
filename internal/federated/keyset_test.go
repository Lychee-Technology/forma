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

// keysetCursorCase is one row of the cursor contract. The table is split
// across the functions below by RULE FAMILY rather than kept as one list:
// each family is a separate clause of validateKeysetCursor, and a failure
// then names the clause that broke.
type keysetCursorCase struct {
	name    string
	cursor  *model.KeysetCursor
	wantErr string // "" means accepted
}

func keysetCol(attr string) model.KeysetColumn {
	return model.KeysetColumn{Attribute: attr, Direction: forma.SortOrderAsc}
}

// keysetCursorOn builds an aligned, active cursor over the named columns, so
// a case exercises the column rules rather than the shape rules.
func keysetCursorOn(attrs ...string) *model.KeysetCursor {
	cols := make([]model.KeysetColumn, 0, len(attrs))
	vals := make([]interface{}, 0, len(attrs))
	for _, a := range attrs {
		cols = append(cols, keysetCol(a))
		vals = append(vals, "v")
	}
	return &model.KeysetCursor{Columns: cols, Values: vals, Mode: model.KeysetCursorModeAfter}
}

func runKeysetCursorCases(t *testing.T, cases []keysetCursorCase) {
	t.Helper()
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

// TestValidateKeysetCursorSystemColumns pins the system-column clause: the
// four columns the visible CTE actually projects, admitted under their own
// name in any casing and by no other route.
func TestValidateKeysetCursorSystemColumns(t *testing.T) {
	runKeysetCursorCases(t, []keysetCursorCase{
		{"row_id alone", keysetCursorOn("row_id"), ""},
		{"created_at then row_id", keysetCursorOn("created_at", "row_id"), ""},
		{"ver_ts then row_id", keysetCursorOn("ver_ts", "row_id"), ""},
		{"deleted_ts then row_id", keysetCursorOn("deleted_ts", "row_id"), ""},

		// DuckDB resolves an unquoted identifier case-insensitively, so
		// ROW_ID reaches the very column row_id does: it takes the
		// system-column branch, and it is the trailing tiebreak the shape
		// rule reaches first.
		{"upper-case ROW_ID is accepted as a system column", keysetCursorOn("ROW_ID", "row_id"), ""},
		{"upper-case ROW_ID is accepted as the trailing tiebreak", keysetCursorOn("created_at", "ROW_ID"), ""},
		{"upper-case ROW_ID alone is accepted", keysetCursorOn("ROW_ID"), ""},
		{"mixed-case Created_At is accepted as a system column", keysetCursorOn("Created_At", "row_id"), ""},

		// Case is the whole of that latitude. A name the fold TRANSFORMS onto
		// one of the four — "created.at" onto created_at — is refused: the
		// generator would emit the real system column and answer a page
		// ordered and filtered on a key the caller never named. Schema
		// registration cannot stand in for this check, because a cursor
		// column is an arbitrary string that was never registered.
		{"dotted name folding onto created_at is rejected", keysetCursorOn("created.at", "row_id"),
			`keyset cursor column "created.at"`},
		{"dotted name folding onto ver_ts is rejected", keysetCursorOn("ver.ts", "row_id"),
			`keyset cursor column "ver.ts"`},
		{"dotted name folding onto deleted_ts is rejected", keysetCursorOn("deleted.ts", "row_id"),
			`keyset cursor column "deleted.ts"`},
		{"bracketed name folding onto row_id is rejected", keysetCursorOn("[row_id]", "row_id"),
			`keyset cursor column "[row_id]"`},
		{"spaced name folding onto created_at is rejected", keysetCursorOn("created at", "row_id"),
			`keyset cursor column "created at"`},
	})
}

// TestValidateKeysetCursorAdmitsAttributes pins what an ordinary schema
// attribute may be, including the dotted form (#260) the old code emitted
// verbatim against a folded CTE column.
func TestValidateKeysetCursorAdmitsAttributes(t *testing.T) {
	runKeysetCursorCases(t, []keysetCursorCase{
		{"plain attribute", keysetCursorOn("count", "row_id"), ""},
		{"dotted attribute folds to a safe identifier", keysetCursorOn("contact.annualIncome", "row_id"), ""},
		{"spaced attribute folds to a safe identifier", keysetCursorOn("annual income", "row_id"), ""},
		{"bracketed attribute folds to a safe identifier", keysetCursorOn("a[0]", "row_id"), ""},

		// #381 amendment: the three names the old allowlist wrongly blessed are
		// ordinary attribute names now. They are ACCEPTED here and fail at
		// DuckDB's binder like any other unknown column — the false claim of
		// support is what this change removes, not the acceptance.
		{"updated_at is no longer blessed but is well-formed", keysetCursorOn("updated_at", "row_id"), ""},
		{"schema_id is no longer blessed but is well-formed", keysetCursorOn("schema_id", "row_id"), ""},
	})
}

// TestValidateKeysetCursorRejectsDedupMachinery pins the reject set: real
// columns of visible, never a caller's, on which a cursor would bind and then
// paginate over the dedup rank. The lookup folds and lower-cases first, so
// neither punctuation nor the shift key is a bypass.
func TestValidateKeysetCursorRejectsDedupMachinery(t *testing.T) {
	runKeysetCursorCases(t, []keysetCursorCase{
		{"rn is rejected", keysetCursorOn("rn", "row_id"), `keyset cursor column "rn"`},
		{"source_tier_priority is rejected", keysetCursorOn("source_tier_priority", "row_id"),
			`keyset cursor column "source_tier_priority"`},
		{"dotted name folding onto source_tier_priority is rejected",
			keysetCursorOn("source_tier.priority", "row_id"), `keyset cursor column "source_tier.priority"`},
		{"bracketed name folding onto rn is rejected", keysetCursorOn("[rn]", "row_id"),
			`keyset cursor column "[rn]"`},
		{"upper-case RN is rejected", keysetCursorOn("RN", "row_id"), `keyset cursor column "RN"`},
		{"mixed-case Source_Tier_Priority is rejected", keysetCursorOn("Source_Tier_Priority", "row_id"),
			`keyset cursor column "Source_Tier_Priority"`},
		{"bracketed upper-case name folding onto rn is rejected", keysetCursorOn("[RN]", "row_id"),
			`keyset cursor column "[RN]"`},
	})
}

// TestValidateKeysetCursorPlaceholder pins the ParquetAttrColumn placeholder
// rule: an attribute the fold empties lands on the literal "attr", and so does
// one the fold merely strips down onto it — either would silently retarget the
// cursor at a real attribute of that name. The exemption for an attribute
// genuinely named attr is folded and case-insensitive on both sides.
func TestValidateKeysetCursorPlaceholder(t *testing.T) {
	runKeysetCursorCases(t, []keysetCursorCase{
		{"empty attribute is rejected", keysetCursorOn("", "row_id"), `folds onto the placeholder "attr"`},
		{"bracket-only attribute is rejected", keysetCursorOn("[]", "row_id"), `folds onto the placeholder "attr"`},
		{"backtick-only attribute is rejected", keysetCursorOn("`", "row_id"), `folds onto the placeholder "attr"`},
		{"a name stripping down onto the placeholder is rejected", keysetCursorOn("[attr]", "row_id"),
			`folds onto the placeholder "attr"`},
		{"an attribute genuinely named attr is accepted", keysetCursorOn("attr", "row_id"), ""},

		{"bracketed mixed-case name folding onto the placeholder is rejected",
			keysetCursorOn("[Attr]", "row_id"), `folds onto the placeholder "Attr"`},
		{"bracketed upper-case name folding onto the placeholder is rejected",
			keysetCursorOn("[ATTR]", "row_id"), `folds onto the placeholder "ATTR"`},
		{"an attribute genuinely named Attr is accepted", keysetCursorOn("Attr", "row_id"), ""},
		{"an attribute genuinely named ATTR is accepted", keysetCursorOn("ATTR", "row_id"), ""},
	})
}

// TestValidateKeysetCursorIdentifierAndShape pins the injection barrier
// (#381 item 2) and the model-level shape rules, which reach callers through
// this one entry point.
func TestValidateKeysetCursorIdentifierAndShape(t *testing.T) {
	runKeysetCursorCases(t, []keysetCursorCase{
		{"quote is rejected", keysetCursorOn(`a"b`, "row_id"), "is not a safe SQL identifier"},
		{"semicolon is rejected", keysetCursorOn("a;DROP TABLE t", "row_id"), "is not a safe SQL identifier"},
		{"paren is rejected", keysetCursorOn("count(*)", "row_id"), "is not a safe SQL identifier"},
		{"leading digit is rejected", keysetCursorOn("1st", "row_id"), "is not a safe SQL identifier"},

		{"missing trailing row_id", keysetCursorOn("created_at"), `expected "row_id"`},
		{
			"misaligned values",
			&model.KeysetCursor{
				Columns: []model.KeysetColumn{keysetCol("created_at"), keysetCol("row_id")},
				Values:  []interface{}{"v"},
				Mode:    model.KeysetCursorModeAfter,
			},
			"carries 2 column(s) but 1 value(s)",
		},

		// The open first page carries no obligation.
		{"nil cursor", nil, ""},
		{"empty cursor", &model.KeysetCursor{Mode: model.KeysetCursorModeAfter}, ""},
	})
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
