package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// LTFlow's two upsert-shaped writes (#651), and concurrent upserts of one
// target on real PostgreSQL.

// LTFlow's idempotency record: a retry of a failed request with the same
// hash takes the record over, and a request with another hash is refused.
func TestUpsertLTFlowIdempotencyRecord(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	first, err := env.upsertInTx(t, onIdempotency(idemCreate("i1", "k1", "failed", "h1"), forma.UpsertDoNothing))
	require.NoError(t, err)
	require.Equal(t, forma.UpsertCreated, first.Outcome)

	retry := func(hash string) *forma.UpsertRequest {
		req := onIdempotency(idemCreate("i1", "k1", "running", hash), forma.UpsertUpdate)
		req.Preconditions = &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "status", Value: "equals:failed"},
			&forma.KvCondition{Attr: "request_hash", Value: "equals:" + hash},
		}}
		req.Updates = map[string]any{"status": "running"}
		return req
	}
	before := env.snapshot(t, ukIdem, first.Record.RowID)
	_, err = env.upsertInTx(t, retry("h2"))
	require.ErrorIs(t, err, forma.ErrPreconditionFailed, "another request under the same key")
	assert.Equal(t, before, env.snapshot(t, ukIdem, first.Record.RowID))

	res, err := env.upsertInTx(t, retry("h1"))
	require.NoError(t, err)
	assert.Equal(t, forma.UpsertUpdated, res.Outcome)
	assert.Equal(t, first.Record.RowID, res.Record.RowID)
	assert.Equal(t, "running", res.Record.Attributes["status"])
	env.requireStored(t, res.Record)
}

// LTFlow's history dedup: an event repeated inside the window bumps the
// stored event's count; outside the window the precondition fails, which
// LTFlow treats as a silent no-op.
func TestUpsertLTFlowHistoryDedup(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	dedup := func(rowID uuid.UUID, createdAt time.Time) *forma.UpsertRequest {
		return &forma.UpsertRequest{
			Create: forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: ukHistory, RowID: rowID},
				Data:             map[string]any{"event": "e", "createdAt": createdAt.UTC().Format(time.RFC3339), "dedup_count": 0},
			},
			Target:        forma.ConflictTarget{RowID: true},
			Action:        forma.UpsertUpdate,
			Preconditions: &forma.KvCondition{Attr: "createdAt", Value: "gte:$now-5m"},
			UpdateOps:     []forma.UpdateOp{forma.Increment("dedup_count", 1)},
		}
	}
	now := time.Now()
	for _, tc := range []struct {
		name      string
		createdAt time.Time
		wantCount int64
	}{
		{"inside the window", now.Add(-time.Minute), 1},
		{"outside the window", now.Add(-10 * time.Minute), 0},
	} {
		rowID := uuid.New()
		res, err := env.upsertInTx(t, dedup(rowID, tc.createdAt))
		require.NoError(t, err, tc.name)
		require.Equal(t, forma.UpsertCreated, res.Outcome, tc.name)

		before := env.snapshot(t, ukHistory, rowID)
		res, err = env.upsertInTx(t, dedup(rowID, tc.createdAt))
		if tc.wantCount == 0 {
			require.ErrorIs(t, err, forma.ErrPreconditionFailed, tc.name)
			assert.Equal(t, before, env.snapshot(t, ukHistory, rowID), "%s: nothing written", tc.name)
		} else {
			require.NoError(t, err, tc.name)
			assert.Equal(t, forma.UpsertUpdated, res.Outcome, tc.name)
			env.requireStored(t, res.Record)
		}
		assert.EqualValues(t, tc.wantCount, env.attrs(t, ukHistory, rowID)["dedup_count"], tc.name)
	}
}

// Concurrent upserts of one key value: one creates the row and every other
// one resolves against it, never with an error.
func TestUpsertConcurrentUpsertsOfOneKey_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for _, action := range []forma.UpsertAction{forma.UpsertDoNothing, forma.UpsertUpdate} {
		for iteration := 0; iteration < 20; iteration++ {
			key := fmt.Sprintf("%s-%d", action, iteration)
			results := make([]*forma.UpsertResult, ukWriters)
			errs := race(ukWriters, func(w int) error {
				req := onIdempotency(idemCreate("race", key, fmt.Sprintf("w%d", w), "h"), action)
				if action == forma.UpsertUpdate {
					req.UpdateOps = []forma.UpdateOp{forma.Increment("counter", 1)}
				}
				var err error
				results[w], err = env.upsertInTx(t, req)
				return err
			})
			requireUpsertsSettled(t, errs, results, action)
			if action == forma.UpsertUpdate {
				rowID := results[0].Record.RowID
				assert.EqualValues(t, ukWriters-1, env.attrs(t, ukIdem, rowID)["counter"], "%s: one increment per updater", key)
			}
		}
	}
	assert.Equal(t, 40, env.mainRowCount(t, ukIdem))
	env.assertKeyTableMatchesRows(t)
}

// Concurrent upserts of one row ID settle on its row lock the same way.
func TestUpsertConcurrentUpsertsOfOneRowID_PostgresOnly(t *testing.T) {
	env := setupUniqueKeyEnv(t)
	for iteration := 0; iteration < 20; iteration++ {
		rowID := uuid.New()
		results := make([]*forma.UpsertResult, ukWriters)
		errs := race(ukWriters, func(w int) error {
			var err error
			results[w], err = env.upsertInTx(t, &forma.UpsertRequest{
				Create: forma.EntityOperation{
					EntityIdentifier: forma.EntityIdentifier{SchemaName: ukHistory, RowID: rowID},
					Data:             map[string]any{"event": fmt.Sprintf("w%d", w), "dedup_count": 0},
				},
				Target: forma.ConflictTarget{RowID: true},
				Action: forma.UpsertDoNothing,
			})
			return err
		})
		requireUpsertsSettled(t, errs, results, forma.UpsertDoNothing)
		require.True(t, env.mainRowExists(t, ukHistory, rowID))
	}
}

// requireUpsertsSettled asserts that every upsert succeeded, exactly one
// created the row, and every other one answered the same row.
func requireUpsertsSettled(t *testing.T, errs []error, results []*forma.UpsertResult, action forma.UpsertAction) {
	t.Helper()
	want := forma.UpsertExisting
	if action == forma.UpsertUpdate {
		want = forma.UpsertUpdated
	}
	created := 0
	for w, err := range errs {
		require.NoError(t, err, "worker %d", w)
		switch results[w].Outcome {
		case forma.UpsertCreated:
			created++
		case want:
		default:
			t.Fatalf("worker %d answered %s, want created or %s", w, results[w].Outcome, want)
		}
		require.Equal(t, results[0].Record.RowID, results[w].Record.RowID, "worker %d answered another row", w)
	}
	require.Equal(t, 1, created, "exactly one upsert creates the row")
}
