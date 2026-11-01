package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An update's value the write cannot convert (#772): a width, UUID,
// integrality or date-range refusal of a value in Updates used to come from
// the merged write, after the row lock and read, so inside a Tx it poisoned
// the Tx although the same value refused by Create did not.

type unconvertibleUpdate struct {
	name    string
	updates map[string]any
	want    string // the published message's prefix
}

func unconvertibleUpdateCases() []unconvertibleUpdate {
	refused := func(attr string, id int) string {
		return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): ", attr, id)
	}
	return []unconvertibleUpdate{
		{"integer past its width", map[string]any{"counter": json.Number("2147483648")}, refused("counter", 2)},
		{"integer, fractional", map[string]any{"counter": 1.5}, refused("counter", 2)},
		{"bigint, fractional past 2^52", map[string]any{"big": json.Number("9007199254740991.5")}, refused("big", 4)},
		{"smallint past its bound column", map[string]any{"level": 40000}, refused("level", 20)},
		{"nested integer, fractional", map[string]any{"stats": map[string]any{"attempts": 0.5}}, refused("stats.attempts", 11)},
		{"array-of-objects element, fractional", map[string]any{"items": []any{map[string]any{"qty": 1}, map[string]any{"qty": 1.5}}}, refused("items.qty", 13)},
		{"uuid, malformed", map[string]any{"ref": "not-a-uuid"}, refused("ref", 19)},
		{"bound uuid, malformed", map[string]any{"token": "not-a-uuid"}, refused("token", 18)},
		{"date past the EAV image range", map[string]any{"day": "9007199254740993"}, refused("day", 8)},
		{"datetime past the iso8601 column", map[string]any{"due_iso": time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}, refused("due_iso", 9)},
		{"scalar for a list", map[string]any{"tags": "ordinary"}, refused("tags", 30)},
	}
}

func requireRefusedValue(t *testing.T, err error, want string, msgAndArgs ...any) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput, msgAndArgs...)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	assert.Contains(t, message, want, msgAndArgs...)
}

// TestUpdateConversionRefusalInATxLeavesTheTxUsable is the issue's
// acceptance: every Tx route whose merged write follows a row lock refuses
// the value before it writes, Tx.Update before any statement and
// UpdateWhere, Claim and Upsert once they have read the row they would write
// with it; the write before the refusals commits, and so does the one after.
// Tx.Update is also tried without a precondition and on a missing row.
func TestUpdateConversionRefusalInATxLeavesTheTxUsable(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	earlier := e.create(t, map[string]any{"name": "before"})
	rowID := uuid.Must(uuid.NewV7())
	_, err := e.em.Create(e.ctx, &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID},
		Data: map[string]any{"name": "stored", "label": rowID.String(), "counter": 1}})
	require.NoError(t, err)
	before := e.snapshot(t, rowID)

	err = e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		if _, err := tx.Update(ctx, condUpdate(earlier, nil, map[string]any{"name": "earlier"})); err != nil {
			return err
		}
		for _, tc := range unconvertibleUpdateCases() {
			for route, write := range textImageTxRoutes() {
				requireRefusedValue(t, write(ctx, tx, rowID, tc.updates), tc.want, "%s %s", route, tc.name)
			}
			_, err := tx.Update(ctx, condUpdate(rowID, nil, tc.updates))
			requireRefusedValue(t, err, tc.want, "unconditional Tx.Update %s", tc.name)
			_, err = tx.Update(ctx, condUpdate(uuid.New(), nil, tc.updates))
			requireRefusedValue(t, err, tc.want, "Tx.Update of a missing row %s", tc.name)
		}
		_, err := tx.Update(ctx, condUpdate(rowID, kv("label", "equals:"+rowID.String()), map[string]any{"counter": 2}))
		return err
	})
	require.NoError(t, err, "the refusals did not poison the Tx")
	assert.Equal(t, "earlier", e.get(t, earlier)["name"], "the write before the refusals committed")
	assert.NotEqual(t, before, e.snapshot(t, rowID), "the write after the refusals committed")
	assert.EqualValues(t, 2, e.get(t, rowID)["counter"])
}

// TestUpdateConversionRefusalInAnAtomicBatchWritesNothing: an atomic
// BatchUpdate refuses the value before it locks any row, so neither
// operation is written.
func TestUpdateConversionRefusalInAnAtomicBatchWritesNothing(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	other := e.create(t, map[string]any{"name": "other"})
	rowID := e.create(t, map[string]any{"name": "ab", "counter": 1})
	for _, tc := range unconvertibleUpdateCases() {
		before, otherBefore := e.snapshot(t, rowID), e.snapshot(t, other)
		_, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Atomic: true,
			Operations: []forma.EntityOperation{textFitUpdateOp(other, map[string]any{"name": "changed"}), textFitUpdateOp(rowID, tc.updates)}})
		requireRefusedValue(t, err, "operation[1]: "+tc.want, "atomic BatchUpdate %s", tc.name)

		result, err := e.em.BatchUpdate(e.ctx, &forma.BatchOperation{Operations: []forma.EntityOperation{textFitUpdateOp(rowID, tc.updates)}})
		require.NoError(t, err)
		require.Len(t, result.Failed, 1, "best-effort BatchUpdate %s", tc.name)
		assert.Contains(t, result.Failed[0].Error, tc.want, "best-effort BatchUpdate %s", tc.name)

		assert.Equal(t, before, e.snapshot(t, rowID), "%s: the row as it was", tc.name)
		assert.Equal(t, otherBefore, e.snapshot(t, other), "%s: the atomic batch wrote nothing", tc.name)
	}
}

// TestUpdateConversionRefusalPrecedesTheRow: a conversion refusal no longer
// waits for the row, so it answers ErrInvalidInput where a false
// precondition or a missing row used to answer first (#772; #679 did the
// same for unstorable text). What the walk refuses rather than converts, an
// undefined attribute or a null, keeps its stage: the precondition and the
// row are judged first, as before.
func TestUpdateConversionRefusalPrecedesTheRow(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	rowID := e.create(t, map[string]any{"name": "ab", "label": "ab", "tags": []any{"x"}})
	for _, tc := range append(unconvertibleUpdateCases(), unconvertibleUpdate{
		"nested array in a list", map[string]any{"tags": []any{[]any{"a"}}}, "invalid value for attribute 'tags' (attrID=30): ",
	}) {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, kv("label", "equals:other"), tc.updates))
		requireRefusedValue(t, err, tc.want, "false precondition, %s", tc.name)
		_, err = e.em.Update(e.ctx, condUpdate(uuid.New(), nil, tc.updates))
		requireRefusedValue(t, err, tc.want, "missing row, %s", tc.name)
	}
	for name, updates := range map[string]map[string]any{
		"undefined attribute": {"nope": 1},
		"null":                {"counter": nil},
	} {
		_, err := e.em.Update(e.ctx, condUpdate(rowID, kv("label", "equals:other"), updates))
		require.ErrorIs(t, err, forma.ErrPreconditionFailed, "false precondition, %s", name)
		_, err = e.em.Update(e.ctx, condUpdate(uuid.New(), nil, updates))
		require.ErrorIs(t, err, forma.ErrNotFound, "missing row, %s", name)
		_, err = e.em.Update(e.ctx, condUpdate(rowID, nil, updates))
		require.ErrorIs(t, err, forma.ErrInvalidInput, "the merged write still refuses it, %s", name)
	}
}

// TestAcceptedUpdatesStillCommit guards the other side: values of every type
// at the edge of what the write accepts pass intake and are stored.
func TestAcceptedUpdatesStillCommit(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	rowID := e.create(t, map[string]any{"name": "ab"})
	ref := uuid.NewString()
	_, err := e.em.Update(e.ctx, condUpdate(rowID, nil, map[string]any{
		"counter": json.Number("2147483647"), "big": json.Number("9007199254740992"), "level": 32767,
		"ref": ref, "day": "2026-11-01", "tags": []any{}, "items": []any{map[string]any{"qty": json.Number("2.0")}},
	}))
	require.NoError(t, err)
	got := e.get(t, rowID)
	assert.EqualValues(t, 2147483647, got["counter"])
	assert.EqualValues(t, 32767, got["level"])
	assert.Equal(t, uuid.MustParse(ref), got["ref"])
	assert.Equal(t, []any{}, got["tags"])
}

// TestUpdateConversionRefusalWithNoRowToWriteSucceeds: an UpdateWhere or a
// Claim that takes no row, and an Upsert that creates its row, never write
// Updates, so before #772 they succeeded whatever Updates held, and they
// still do (no accepted update changes outcome). Unstorable text is the
// exception it has been since #679: refused at intake whatever the row, and
// without poisoning the Tx.
func TestUpdateConversionRefusalWithNoRowToWriteSucceeds(t *testing.T) {
	e := newSchemaPG(t, condSchemaName, condSchemaID, textFitSchemaCache())
	existing := e.create(t, map[string]any{"name": "existing", "counter": 1})
	before := e.snapshot(t, existing)
	nobody := kv("label", "equals:nobody")
	var created []uuid.UUID

	err := e.txr.InTx(e.ctx, func(ctx context.Context, tx forma.Tx) error {
		for _, tc := range unconvertibleUpdateCases() {
			updated, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: condSchemaName,
				Condition: nobody, MaxAffected: 10, Updates: tc.updates})
			require.NoError(t, err, "UpdateWhere matching no row, %s", tc.name)
			assert.Empty(t, updated.RowIDs, "UpdateWhere matching no row, %s", tc.name)

			claimed, err := tx.Claim(ctx, &forma.ClaimRequest{SchemaName: condSchemaName,
				Condition: nobody, Limit: 1, Updates: tc.updates})
			require.NoError(t, err, "Claim matching no row, %s", tc.name)
			assert.Empty(t, claimed.Records, "Claim matching no row, %s", tc.name)

			rowID := uuid.Must(uuid.NewV7())
			upserted, err := tx.Upsert(ctx, &forma.UpsertRequest{
				Create: forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: condSchemaName, RowID: rowID},
					Data: map[string]any{"name": "created", "counter": 7}},
				Target: forma.ConflictTarget{RowID: true}, Action: forma.UpsertUpdate, Updates: tc.updates,
			})
			require.NoError(t, err, "Upsert creating its row, %s", tc.name)
			assert.Equal(t, forma.UpsertCreated, upserted.Outcome, "Upsert creating its row, %s", tc.name)
			created = append(created, rowID)
		}
		unstorable := map[string]any{"name": "a\x00b"}
		_, err := tx.UpdateWhere(ctx, &forma.MutateWhereRequest{SchemaName: condSchemaName,
			Condition: nobody, MaxAffected: 10, Updates: unstorable})
		requireRefusedValue(t, err, "invalid value for attribute 'name' (attrID=1): text cannot contain a NUL character", "UpdateWhere, unstorable text")
		_, err = tx.Claim(ctx, &forma.ClaimRequest{SchemaName: condSchemaName, Condition: nobody, Limit: 1, Updates: unstorable})
		requireRefusedValue(t, err, "invalid value for attribute 'name' (attrID=1): text cannot contain a NUL character", "Claim, unstorable text")
		return nil
	})
	require.NoError(t, err, "nothing poisoned the Tx")
	for _, rowID := range created {
		got := e.get(t, rowID)
		assert.Equal(t, "created", got["name"])
		assert.EqualValues(t, 7, got["counter"])
	}
	assert.Equal(t, before, e.snapshot(t, existing), "no row was written with Updates")
}
