package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The validation matrix of the unset operator (#938, Q4): an unset is
// validated by the paths every update takes, on the merged document, and by
// nothing new. The AttributeMetadata required policies reject in every mode;
// the JSON Schema follows ValidateUpdatesStrict. Each verdict is compared
// with the verdict of another update reaching the same document: an update
// of a seeded row that already lacks the attribute.

// unsetVerdictCase is one cell of the matrix: ops (with updates) applied to
// base, and whether each mode accepts the result.
type unsetVerdictCase struct {
	name    string
	base    map[string]any
	updates map[string]any
	ops     []forma.UpdateOp
	// accept is the verdict per mode; want is the refusal text otherwise.
	accept map[unsetMode]bool
	want   string
	// reportOnlyKind is the violation kind report-only mode counts, "" for
	// none.
	reportOnlyKind string
}

func unsetVerdictCases() []unsetVerdictCase {
	soleOwner := unsetBaseDoc()
	soleOwner["owner"] = map[string]any{"id": "o1"}
	none := map[unsetMode]bool{}
	all := map[unsetMode]bool{unsetStrict: true, unsetReportOnly: true, unsetNoValidator: true}
	relaxed := map[unsetMode]bool{unsetReportOnly: true, unsetNoValidator: true}
	return []unsetVerdictCase{
		{name: "required_always", base: unsetBaseDoc(), ops: []forma.UpdateOp{forma.Unset("name")},
			accept: none, want: "missing required attribute 'name'"},
		{name: "required_if_parent_present with the parent kept", base: unsetBaseDoc(), ops: []forma.UpdateOp{forma.Unset("owner.id")},
			accept: none, want: "missing required attribute 'owner.id'"},
		{name: "required_if_parent_present with the parent pruned", base: soleOwner, ops: []forma.UpdateOp{forma.Unset("owner.id")},
			accept: all},
		{name: "JSON Schema only required", base: unsetBaseDoc(), ops: []forma.UpdateOp{forma.Unset("memo")},
			accept: relaxed, want: "memo", reportOnlyKind: "required"},
		{name: "unrelated representable violation", base: unsetBaseDoc(), updates: map[string]any{"short": "toolong"},
			ops: []forma.UpdateOp{forma.Unset("note")}, accept: relaxed, want: "short", reportOnlyKind: "constraint"},
		{name: "scalar shape refusal (#924)", base: unsetBaseDoc(), updates: map[string]any{"note": map[string]any{"x": "y"}},
			ops: []forma.UpdateOp{forma.Unset("label")}, accept: none, want: "invalid value for attribute 'note'"},
	}
}

func TestUnsetValidationMatrix(t *testing.T) {
	for _, tc := range unsetVerdictCases() {
		for _, mode := range unsetModes {
			t.Run(tc.name+"/"+string(mode), func(t *testing.T) {
				h := newUnsetHarness(t, mode)
				rowID := h.seed(t, tc.base)
				before := h.record(rowID)
				_, err := h.update(rowID, tc.updates, tc.ops...)
				requireUnsetVerdict(t, err, tc.accept[mode], tc.want)
				if !tc.accept[mode] {
					assert.Same(t, before, h.record(rowID), "a refused update stores nothing")
				}
				var counted []string
				for _, event := range h.metrics.events {
					counted = append(counted, event.Labels["kind"])
				}
				if mode == unsetReportOnly && tc.reportOnlyKind != "" {
					assert.Equal(t, []string{tc.reportOnlyKind}, counted, "report-only counts the accepted violation as today")
				} else {
					assert.Empty(t, counted)
				}

				otherErr := reachWithoutUnset(t, mode, tc)
				assert.Equal(t, err == nil, otherErr == nil, "another mutation reaching the same document gets the same verdict: %v / %v", err, otherErr)
			})
		}
	}
}

// reachWithoutUnset answers the verdict on tc's resulting document reached
// without the operator: an update of a row that already lacks what the
// unset removes. A document the transformer refuses to store cannot be such
// a row, and the refusal is the verdict every write reaching it gets.
func reachWithoutUnset(t *testing.T, mode unsetMode, tc unsetVerdictCase) error {
	t.Helper()
	h := newUnsetHarness(t, mode)
	reached := tc.base
	for _, op := range tc.ops {
		reached = withoutPath(reached, op.Attribute)
	}
	rowID := uuid.New()
	record, err := h.tr.ToPersistentRecord(h.ctx, h.schemaID, rowID, reached)
	if err != nil {
		require.ErrorIs(t, err, forma.ErrInvalidInput)
		return err
	}
	h.repo.storeRecord(record)
	updates := tc.updates
	if updates == nil {
		updates = map[string]any{"counter": int64(3)}
	}
	_, err = h.update(rowID, updates)
	return err
}

func requireUnsetVerdict(t *testing.T, err error, accept bool, want string) {
	t.Helper()
	if accept {
		require.NoError(t, err)
		return
	}
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, "the refusal publishes its message: %v", err)
	assert.Contains(t, message, want)
}

// TestExplicitNullIsStillRefused pins Q3: unset is the only clear, and an
// explicit null stays refused on Create and Update, nested and as a list
// element, in every mode.
func TestExplicitNullIsStillRefused(t *testing.T) {
	nulls := map[string]map[string]any{
		"top-level":    {"note": nil},
		"bound":        {"label": nil},
		"nested":       {"profile": map[string]any{"nick": nil}},
		"list element": {"tags": []any{"a", nil}},
	}
	for _, mode := range unsetModes {
		for name, updates := range nulls {
			t.Run(string(mode)+"/"+name, func(t *testing.T) {
				h := newUnsetHarness(t, mode)
				data := unsetBaseDoc()
				for key, value := range updates {
					data[key] = value
				}
				create := unsetOp(uuid.Nil, nil)
				create.Data = data
				_, err := h.em.Create(h.ctx, create)
				require.ErrorIs(t, err, forma.ErrInvalidInput, "create")

				rowID := h.create(t, unsetBaseDoc())
				before := h.record(rowID)
				_, err = h.update(rowID, updates)
				require.ErrorIs(t, err, forma.ErrInvalidInput, "update")
				assert.Same(t, before, h.record(rowID), "the refused update stores nothing")
			})
		}
	}
}

// TestTypedNilIsNoClear pins that no typed nil clears an attribute: each is
// refused, or leaves the attribute present. A nil list is the empty list,
// whose marker the row keeps, as []any{} is.
func TestTypedNilIsNoClear(t *testing.T) {
	var nilText *string
	var nilCount *int64
	cases := []struct {
		name    string
		updates map[string]any
		attr    string
	}{
		{"nil list", map[string]any{"tags": []any(nil)}, "tags"},
		{"empty list", map[string]any{"tags": []any{}}, "tags"},
		{"nil nested list", map[string]any{"profile": map[string]any{"aliases": []any(nil)}}, "profile.aliases"},
		{"nil object", map[string]any{"profile": map[string]any(nil)}, "profile.nick"},
		{"nil text pointer", map[string]any{"note": nilText}, "note"},
		{"nil integer pointer", map[string]any{"counter": nilCount}, "counter"},
	}
	for _, mode := range unsetModes {
		for _, tc := range cases {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				h := newUnsetHarness(t, mode)
				rowID := h.create(t, unsetBaseDoc())
				before := h.record(rowID)
				_, err := h.update(rowID, tc.updates)
				if err != nil {
					require.ErrorIs(t, err, forma.ErrInvalidInput)
					assert.Same(t, before, h.record(rowID), "the refused update stores nothing")
					return
				}
				assert.NotNil(t, getValueAtPath(h.stored(t, rowID), tc.attr), "a typed nil leaves '%s' present", tc.attr)
			})
		}
	}
}

// TestEmptyListAndUnsetReadBackDifferently pins that the empty list is a
// value and unset the clear: the one keeps its marker, the other removes it.
func TestEmptyListAndUnsetReadBackDifferently(t *testing.T) {
	h := newUnsetHarness(t, unsetStrict)
	emptied := h.create(t, unsetBaseDoc())
	cleared := h.create(t, unsetBaseDoc())

	_, err := h.update(emptied, map[string]any{"tags": []any{}})
	require.NoError(t, err)
	_, err = h.update(cleared, nil, forma.Unset("tags"))
	require.NoError(t, err)

	assert.Equal(t, []any{}, h.stored(t, emptied)["tags"])
	assert.NotContains(t, h.stored(t, cleared), "tags")

	// Unset of a list holding only its marker removes the marker.
	_, err = h.update(emptied, nil, forma.Unset("tags"))
	require.NoError(t, err)
	assert.NotContains(t, h.stored(t, emptied), "tags")
	for _, record := range h.record(emptied).OtherAttributes {
		assert.NotEqual(t, int16(5), record.AttrID, "no tags record, the marker included, is left")
	}
}
