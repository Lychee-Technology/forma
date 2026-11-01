package internal

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// textImageTxRoutes are the Tx routes whose merged write follows a row lock,
// each writing updates to one row of the cond649 schema in tx. The row's
// label selects it, and every route writes label "done", which also makes
// the Claim falsify its condition.
func textImageTxRoutes() map[string]func(context.Context, forma.Tx, uuid.UUID, map[string]any) error {
	return map[string]func(context.Context, forma.Tx, uuid.UUID, map[string]any) error{
		"Tx.Update": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, updates map[string]any) error {
			_, err := tx.Update(ctx, condUpdate(rowID, kv("label", "equals:"+rowID.String()), updates))
			return err
		},
		"Tx.UpdateWhere": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, updates map[string]any) error {
			_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: condSchemaName,
				Condition: kv("label", "equals:"+rowID.String()), MaxAffected: 10, Updates: updates})
			return err
		},
		"Tx.Claim": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, updates map[string]any) error {
			result, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: condSchemaName,
				Condition: kv("label", "equals:"+rowID.String()), Limit: 1, Updates: updates})
			if err == nil && len(result.Records) != 1 {
				err = assert.AnError
			}
			return err
		},
		"Tx.Upsert": func(ctx context.Context, tx forma.Tx, rowID uuid.UUID, updates map[string]any) error {
			result, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create:  forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID}, Data: map[string]any{"name": "created"}},
				Target:  forma.ConflictTarget{RowID: true},
				Action:  forma.UpsertUpdate,
				Updates: updates,
			})
			if err == nil && result.Outcome != forma.UpsertUpdated {
				err = assert.AnError
			}
			return err
		},
	}
}

// TestChangingTextInATxIsStoredAsChecked is #773's reproduction: a Go
// caller's fmt.Stringer that answers valid text on its first call and NUL on
// later ones passed the update intake, and the merged write, after the row
// lock, took its image again and was refused, poisoning the Tx. Every route
// now stores the image intake checked, so the Tx stays usable and commits
// what each route was handed first; a Stringer that changes between valid
// strings is stored as the first too.
func TestChangingTextInATxIsStoredAsChecked(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	images := map[string][]string{
		"valid, then NUL":           {"first", "a\x00b"},
		"valid, then invalid UTF-8": {"first", "a\xffb"},
		"changing between valid":    {"one", "two", "three"},
	}
	type write struct {
		route, variant string
		rowID          uuid.UUID
		// one value per position: name, stats.note and a tags element
		values [3]changingText
	}
	// The atomic BatchUpdate is no Tx route, but its merges also run after
	// every row is locked; it runs after the Tx.
	const batch = "atomic BatchUpdate"
	routes := textImageTxRoutes()
	var writes []write
	for _, route := range append(slices.Collect(maps.Keys(routes)), batch) {
		for variant, texts := range images {
			rowID := uuid.Must(uuid.NewV7())
			_, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID},
				Data: map[string]any{"name": "stored", "label": rowID.String()}})
			require.NoError(t, err)
			writes = append(writes, write{route, variant, rowID,
				[3]changingText{newChangingText(texts...), newChangingText(texts...), newChangingText(texts...)}})
		}
	}
	updatesOf := func(w write) map[string]any {
		return map[string]any{"label": "done", "name": w.values[0], "stats": map[string]any{"note": w.values[1]}, "tags": []any{w.values[2]}}
	}

	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		for _, w := range writes {
			if w.route == batch {
				continue
			}
			require.NoError(t, routes[w.route](ctx, tx, w.rowID, updatesOf(w)), "%s, %s", w.route, w.variant)
		}
		_, err := tx.Update(ctx, condUpdate(writes[0].rowID, nil, map[string]any{"small": 1}))
		return err
	})
	require.NoError(t, err, "no merged write refused the text after the lock")

	for _, w := range writes {
		if w.route != batch {
			continue
		}
		_, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true,
			Operations: []forma.EntityOperation{textFitUpdateOp(w.rowID, updatesOf(w))}})
		require.NoError(t, err, "%s, %s", w.route, w.variant)
	}

	for _, w := range writes {
		got := e.get(t, w.rowID)
		first := w.values[0].images[0]
		assert.Equal(t, first, got["name"], "%s, %s: the checked image is stored", w.route, w.variant)
		assert.Equal(t, map[string]any{"note": first}, got["stats"], "%s, %s", w.route, w.variant)
		assert.Equal(t, []any{first}, got["tags"], "%s, %s", w.route, w.variant)
		assert.Equal(t, "done", got["label"], "%s, %s", w.route, w.variant)
		for _, value := range w.values {
			assert.Equal(t, 1, *value.calls, "%s, %s: String() is called once", w.route, w.variant)
		}
	}
}
