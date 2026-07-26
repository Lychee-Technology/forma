package internal

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The guards in this file protect the one invariant nothing else in the codebase
// enforces: NormalizeDottedKeys' output feeds the validator and must never reach
// the writer.
//
// The assertions are identity, not equality, and that is deliberate. Normalizing
// merges the two spellings of a dotted attribute into one document, which
// destroys the provenance transform's dedupe resolves precedence with (#312) —
// but the two documents flatten to the same records for every payload shape the
// normalizer was designed to handle, so an equality assertion passes while the
// bug is live. An earlier tripwire built that way came out green, and so did a
// stored-EAV-rows comparison. Only the map header distinguishes them.
//
// All four production ToPersistentRecord sites are covered. The atomic batch pair
// is not redundant with the CRUD pair: httpapi routes POST /api/v1/<schema>
// through BatchCreate with Atomic true (internal/httpapi/server.go), so
// production create traffic goes through the batch seam and never through
// crud.Create.

// mapPointer is the identity of a map value: two map variables alias the same
// map exactly when their headers match. reflect is the only way to read it.
func mapPointer(m map[string]any) uintptr {
	return reflect.ValueOf(m).Pointer()
}

// TestCreateWritesTheCallersOwnMap covers the crud.Create seam.
//
// The pointer is expected to be *exactly* the caller's only because schema "test"
// has no relations, so StripComputedFields early-returns the map it was given. A
// schema that does have relations gets a rebuilt map from stripping, and this
// assertion would then fail against a correct implementation — so do not re-point
// this test at a schema like "visit".
func TestCreateWritesTheCallersOwnMap(t *testing.T) {
	manager, _, spy := newValidationHarness(t, false)

	data := map[string]any{"name": "open", "person.name": "ada"}
	_, err := manager.Create(context.Background(), createOp(data))
	require.NoError(t, err)

	require.Len(t, spy.seen, 1, "create must transform exactly once")
	require.Equal(t, mapPointer(data), spy.seen[0].pointer,
		"ToPersistentRecord must be handed the caller's own map, not the normalized document")
}

// TestBatchCreateAtomicWritesTheCallersOwnMap covers the seam production create
// traffic actually uses. The same caveat about schema "test" having no relations
// applies.
func TestBatchCreateAtomicWritesTheCallersOwnMap(t *testing.T) {
	manager, _, spy := newValidationHarness(t, false)

	data := map[string]any{"name": "open", "person.name": "ada"}
	_, err := manager.BatchCreate(context.Background(), &forma.BatchOperation{
		Atomic:     true,
		Operations: []forma.EntityOperation{*createOp(data)},
	})
	require.NoError(t, err)

	require.Len(t, spy.seen, 1, "atomic batch create must transform exactly once")
	require.Equal(t, mapPointer(data), spy.seen[0].pointer,
		"ToPersistentRecord must be handed the caller's own map, not the normalized document")
}

// TestUpdateWritesTheUnnormalizedMerge covers the crud.Update seam.
//
// mergeMaps allocates its result inside Update, so no pointer the test holds can
// name it. The discriminator is instead the merged map's own shape: it is
// key-literal, so a dotted update key is still spelled "person.name" there, while
// the normalized document has expanded it into a nested "person" object.
func TestUpdateWritesTheUnnormalizedMerge(t *testing.T) {
	manager, _, spy := newValidationHarness(t, true)

	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)
	spy.seen = nil

	_, err = manager.Update(context.Background(), updateOp(created.RowID, map[string]any{"person.name": "ada"}))
	require.NoError(t, err)

	requireUnnormalizedMerge(t, spy)
}

// TestBatchUpdateAtomicWritesTheUnnormalizedMerge covers the fourth seam.
func TestBatchUpdateAtomicWritesTheUnnormalizedMerge(t *testing.T) {
	manager, _, spy := newValidationHarness(t, true)

	created, err := manager.Create(context.Background(), createOp(map[string]any{"name": "open"}))
	require.NoError(t, err)
	spy.seen = nil

	_, err = manager.BatchUpdate(context.Background(), &forma.BatchOperation{
		Atomic:     true,
		Operations: []forma.EntityOperation{*updateOp(created.RowID, map[string]any{"person.name": "ada"})},
	})
	require.NoError(t, err)

	requireUnnormalizedMerge(t, spy)
}

func requireUnnormalizedMerge(t *testing.T, spy *writeSpy) {
	t.Helper()
	require.Len(t, spy.seen, 1, "update must transform exactly once")
	require.Contains(t, spy.seen[0].keys, "person.name",
		"the merged map is key-literal; the writer must still see the caller's spelling")
	require.NotContains(t, spy.seen[0].keys, "person",
		"a nested \"person\" key means the normalized document reached the writer")
}

// TestCreateLeavesTheCallersMapUntouched closes the vector pointer identity is
// blind to. A NormalizeDottedKeys that expanded dotted keys *in place* would hand
// the writer the caller's own map header — passing every assertion above — while
// its contents had already become the normalized document.
//
// This replaces a stored-EAV-rows comparison that detected neither vector:
// measured, it stayed green both with the normalized document wired into
// ToPersistentRecord and with an in-place normalizer, because the normalizer and
// dedupe agree on the records for the payload shapes the normalizer handles.
//
// Comparing marshalled JSON rather than the map catches a change at any depth and
// is order-independent, because encoding/json sorts object keys.
func TestCreateLeavesTheCallersMapUntouched(t *testing.T) {
	seams := []struct {
		name  string
		write func(forma.EntityManager, map[string]any) error
	}{
		{"crud", func(m forma.EntityManager, data map[string]any) error {
			_, err := m.Create(context.Background(), createOp(data))
			return err
		}},
		{"batchAtomic", func(m forma.EntityManager, data map[string]any) error {
			_, err := m.BatchCreate(context.Background(), &forma.BatchOperation{
				Atomic:     true,
				Operations: []forma.EntityOperation{*createOp(data)},
			})
			return err
		}},
	}

	for _, seam := range seams {
		t.Run(seam.name, func(t *testing.T) {
			manager, _, _ := newValidationHarness(t, false)
			data := map[string]any{
				"name":        "open",
				"person.name": "ada",
				"person":      map[string]any{"name": "grace"},
			}

			before, err := json.Marshal(data)
			require.NoError(t, err)

			require.NoError(t, seam.write(manager, data))

			after, err := json.Marshal(data)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after),
				"validation must not mutate the caller's document")
		})
	}
}
