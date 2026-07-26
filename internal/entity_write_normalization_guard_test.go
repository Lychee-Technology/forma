package internal

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

// mapPointer is the identity of a map value: two map variables alias the same
// map exactly when their headers match. reflect is the only way to read it.
func mapPointer(m map[string]any) uintptr {
	return reflect.ValueOf(m).Pointer()
}

// TestCreateWritesTheCallersOwnMap is the guard on the one invariant nothing
// else in this package enforces: NormalizeDottedKeys' output feeds the validator
// and must never reach the writer.
//
// The assertion is identity, not equality, and that is deliberate. Normalizing
// merges the two spellings of a dotted attribute into one document, which
// destroys the provenance transform's dedupe resolves precedence with (#312) —
// but for most payload shapes both documents flatten to the same records, so an
// equality assertion passes while the bug is live. An earlier tripwire built
// that way came out green. Only the map header distinguishes them.
func TestCreateWritesTheCallersOwnMap(t *testing.T) {
	manager, _, spy := newValidationHarness(t, false, true)

	data := map[string]any{"name": "open", "person.name": "ada"}
	_, err := manager.Create(context.Background(), createOp(data))
	require.NoError(t, err)

	require.Len(t, spy.seen, 1, "create must transform exactly once")
	require.Equal(t, mapPointer(data), spy.seen[0].pointer,
		"ToPersistentRecord must be handed the caller's own map, not the normalized document")
}

// TestUpdateWritesTheUnnormalizedMerge is the same guard at the update seam.
//
// mergeMaps allocates its result inside Update, so no pointer the test holds can
// name it. The discriminator is instead the merged map's own shape: it is
// key-literal, so a dotted update key is still spelled "person.name" there,
// while the normalized document has expanded it into a nested "person" object.
func TestUpdateWritesTheUnnormalizedMerge(t *testing.T) {
	manager, _, spy := newValidationHarness(t, true, true)

	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)
	spy.seen = nil

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"person.name": "ada"}))
	require.NoError(t, err)

	require.Len(t, spy.seen, 1, "update must transform exactly once")
	require.Contains(t, spy.seen[0].keys, "person.name",
		"the merged map is key-literal; the writer must still see the caller's spelling")
	require.NotContains(t, spy.seen[0].keys, "person",
		"a nested \"person\" key means the normalized document reached the writer")
}

// TestUpdateStoresTheSameRowsWithAndWithoutValidation is the behavioural backstop
// under the identity guard: turning validation on must not move a single stored
// EAV row. The payload carries both spellings of person.name precisely because
// that is the case whose outcome dedupe decides from the spelling tags.
//
// It is a net, not the tripwire. Measured: with the normalized document wired
// into ToPersistentRecord this test still passes, because the normalizer and
// dedupe agree on scalars by construction — the two identity assertions above
// are the only things in this package that fail.
func TestUpdateStoresTheSameRowsWithAndWithoutValidation(t *testing.T) {
	updates := map[string]any{
		"person":      map[string]any{"name": "grace"},
		"person.name": "ada",
	}

	validated := storedRowsAfterUpdate(t, true, updates)
	baseline := storedRowsAfterUpdate(t, false, updates)

	require.Equal(t, baseline, validated,
		"enabling schema validation changed the stored EAV rows")
}

// storedRowsAfterUpdate creates a record, updates it, and renders the stored EAV
// rows canonically. validate selects whether the manager has a validator at all.
func storedRowsAfterUpdate(t *testing.T, validate bool, updates map[string]any) []string {
	t.Helper()
	manager, repo, _ := newValidationHarness(t, false, validate)

	ctx := context.Background()
	created, err := manager.Create(ctx, createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)

	_, err = manager.Update(ctx, updateOp(created.RowID, updates))
	require.NoError(t, err)

	stored := repo.records[100][created.RowID]
	require.NotNil(t, stored, "the updated record must be in storage")
	return renderEAVRows(stored.OtherAttributes)
}

// renderEAVRows renders EAV rows in a row-id-independent, order-independent form
// so two runs of the same payload are directly comparable.
func renderEAVRows(rows []model.EAVRecord) []string {
	rendered := make([]string, 0, len(rows))
	for _, row := range rows {
		rendered = append(rendered, fmt.Sprintf("attr=%d indices=%q text=%s numeric=%s",
			row.AttrID, row.ArrayIndices, renderText(row.ValueText), renderNumeric(row.ValueNumeric)))
	}
	sort.Strings(rendered)
	return rendered
}

func renderText(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%q", *value)
}

func renderNumeric(value *float64) string {
	if value == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", *value)
}
