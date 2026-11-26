package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countDepthChecksOf counts the depth checks made of cond until the test
// ends.
func countDepthChecksOf(t *testing.T, cond forma.Condition) *int {
	t.Helper()
	n := new(int)
	conditionDepthChecked = func(checked forma.Condition) {
		if checked == cond {
			*n++
		}
	}
	t.Cleanup(func() { conditionDepthChecked = nil })
	return n
}

// An Upsert that finds the target held checks its precondition's depth once,
// in its plan before any SQL, and does not check it again when it updates
// the locked row (#675). The precondition is still enforced under the lock:
// one that fails there answers ErrPreconditionFailed and writes nothing. A
// standalone Update still checks its own precondition.
func TestUpsertChecksItsPreconditionDepthOnce(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	holder := env.mustCreate(t, ukIdem, map[string]any{"instance_id": "i1", "idempotency_key": "k1", "status": "failed", "external_ref": "ref-1"})
	upsert := func(status string) *forma.UpsertRequest {
		req := onIdempotency(idemCreate("i1", "k1", "x", "h"), forma.UpsertUpdate)
		req.Preconditions = nestedCondition(3, &forma.KvCondition{Attr: "status", Value: "equals:" + status})
		req.Updates = map[string]any{"status": "running"}
		return req
	}

	refused := upsert("succeeded")
	checks := countDepthChecksOf(t, refused.Preconditions)
	before := env.snapshot(t, ukIdem, holder)
	_, err := env.upsertInTx(t, refused)
	require.ErrorIs(t, err, forma.ErrPreconditionFailed, "the precondition is judged under the lock")
	assert.Equal(t, before, env.snapshot(t, ukIdem, holder))
	assert.Equal(t, 1, *checks)

	applied := upsert("failed")
	checks = countDepthChecksOf(t, applied.Preconditions)
	res, err := env.upsertInTx(t, applied)
	require.NoError(t, err)
	require.Equal(t, forma.UpsertUpdated, res.Outcome)
	assert.Equal(t, "running", res.Record.Attributes["status"])
	assert.Equal(t, 1, *checks, "one check per Upsert, none on the update of the existing row")

	op := &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: ukIdem, RowID: holder},
		Updates: map[string]any{"status": "done"}, Preconditions: &forma.KvCondition{Attr: "status", Value: "equals:running"}}
	checks = countDepthChecksOf(t, op.Preconditions)
	require.NoError(t, env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, op)
		return err
	}))
	assert.Equal(t, 1, *checks, "a standalone Update still checks its precondition")
}
