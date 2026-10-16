package coordinationtest

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// requireConsistent asserts the soak iteration's end state by direct SQL:
// versions contiguous, each visit's tasks and history written exactly once with the
// version that reached it, every fence at a version committed before the
// advance that left it (invariant 2), every message published once in its
// partition's version order, nothing written by a step that did not commit,
// and one hot change_log entry per row.
func (s *soak) requireConsistent(t *testing.T, run *relayRun) {
	t.Helper()
	e := s.e
	tasks := s.taskCounts(t)
	states := e.eavTexts(t, HistorySchema, "state_to")
	transitions, observations := s.historyByInstance(t, states)
	fenced := s.fencesByVersion(t)
	partitions := make([][]OutboxMessage, 0, len(s.instances))
	for _, id := range s.instances {
		final := e.stored(t, InstanceSchema, id, "version")["version"].(int64)
		recs := slices.SortedFunc(slices.Values(s.advanced[id]), func(a, b advanceRecord) int { return cmp.Compare(a.version, b.version) })
		reached := make([]int64, len(recs))
		for i, rec := range recs {
			reached[i] = rec.version
		}
		require.Equal(t, versionsBetween(2, final), reached, "instance %s: committed steps reached each version once", id)

		var wantTasks, wantStates []string
		for v := int64(1); v <= final; v++ {
			status := TaskCompleted + "+" + TaskCancelled
			if v == final {
				status = TaskPending + "+" + TaskPending
			}
			wantTasks = append(wantTasks, fmt.Sprintf("%d:%s", v, status))
			if v > 1 {
				wantStates = append(wantStates, visitState(v))
			}
		}
		require.Equal(t, wantTasks, tasks[id.String()], "instance %s: each visit's two tasks, resolved before the last", id)
		require.ElementsMatch(t, wantStates, transitions[id.String()], "instance %s: one transition per version", id)
		require.Len(t, observations[id.String()], s.observed[id], "instance %s: one history event per fenced step", id)
		for _, state := range observations[id.String()] {
			require.Contains(t, slices.Concat([]string{visitState(1)}, wantStates), state, "instance %s: a fenced step saw a version it reached", id)
		}
		s.requireFencesBeforeAdvances(t, id, recs, fenced[id.String()])

		partition := make([]OutboxMessage, 0, 2*len(recs))
		for _, rec := range recs {
			for _, m := range rec.messages {
				partition = append(partition, OutboxMessage{ID: m})
			}
		}
		partitions = append(partitions, partition)
	}
	requireDrained(t, e, run, partitions)
	var advanced []uuid.UUID
	for _, id := range s.instances {
		if len(s.advanced[id]) > 0 {
			advanced = append(advanced, id)
			continue
		}
		require.Equal(t, s.seeded[id], e.stamps(t, InstanceSchema, id)[id], "instance %s: no advance committed, so nothing wrote it", id)
	}
	e.requireWritten(t, InstanceSchema, s.seeded, advanced...)
	for schema := range schemaIDs {
		e.requireOneHotEntryPerRow(t, schema)
	}
}

// requireEveryInstanceAdvanced is a mechanism assertion. On PostgreSQL a
// fence makes the advance it races wait and then commit, so an advance wins
// every race. Under optimistic concurrency the fence can commit first and
// abort the advance with ErrRetryableConflict, so an instance whose every
// advance lost that way stays at version 1 (#633).
func (s *soak) requireEveryInstanceAdvanced(t *testing.T) {
	t.Helper()
	for _, id := range s.instances {
		require.NotEmpty(t, s.advanced[id], "instance %s: an advance committed", id)
	}
}

// requireFencesBeforeAdvances asserts invariant 2 for one instance: every
// committed fence record is at a version the instance reached, and each
// advance, after its update, saw every fence that committed at the version
// it left. A fence that committed after the advance moved the version would
// be one the advance did not count.
func (s *soak) requireFencesBeforeAdvances(t *testing.T, id uuid.UUID, recs []advanceRecord, fenced map[int64]int) {
	t.Helper()
	total := 0
	for v, n := range fenced {
		require.True(t, v >= 1 && v <= int64(len(recs))+1, "instance %s: a fence record at version %d it never reached", id, v)
		total += n
	}
	require.Equal(t, s.observed[id], total, "instance %s: one event record per fenced step", id)
	for _, rec := range recs {
		require.Equal(t, fenced[rec.version-1], rec.fences,
			"instance %s: the advance to version %d saw every fence at version %d", id, rec.version, rec.version-1)
	}
}

// fencesByVersion answers, per instance, how many committed event records
// carry each result_version.
func (s *soak) fencesByVersion(t *testing.T) map[string]map[int64]int {
	t.Helper()
	e := s.e
	version, ok := e.columns[IdempotencySchema]["result_version"]
	require.True(t, ok && version.column == "", "result_version is an EAV attribute")
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(`SELECT m.%s, d.value_numeric::bigint, count(*) FROM %s m
		JOIN %s d ON d.schema_id = m.ltbase_schema_id AND d.row_id = m.ltbase_row_id AND d.attr_id = $2
		WHERE m.ltbase_schema_id = $1 AND m.ltbase_deleted_at IS NULL GROUP BY 1, 2`,
		e.column(t, IdempotencySchema, "instance_id"), e.tables.entityMain, e.tables.eavData),
		schemaIDs[IdempotencySchema], version.id)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]map[int64]int{}
	for rows.Next() {
		var instance string
		var v int64
		var n int
		require.NoError(t, rows.Scan(&instance, &v, &n))
		if out[instance] == nil {
			out[instance] = map[int64]int{}
		}
		out[instance][v] = n
	}
	require.NoError(t, rows.Err())
	return out
}

// versionsBetween answers from..to, empty when to < from.
func versionsBetween(from, to int64) []int64 {
	out := make([]int64, 0, max(to-from+1, 0))
	for v := from; v <= to; v++ {
		out = append(out, v)
	}
	return out
}

// taskCounts answers, per instance, each visit's task statuses as
// "version:status+status", in version order.
func (s *soak) taskCounts(t *testing.T) map[string][]string {
	t.Helper()
	e := s.e
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(`SELECT %s, %s, string_agg(%s, '+' ORDER BY %s) FROM %s
		WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL GROUP BY 1, 2 ORDER BY 1, 2`,
		e.column(t, TaskSchema, "instance_id"), e.column(t, TaskSchema, "version"), e.column(t, TaskSchema, "status"),
		e.column(t, TaskSchema, "purpose")+" DESC", e.tables.entityMain), schemaIDs[TaskSchema])
	require.NoError(t, err)
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var instance, statuses string
		var version int64
		require.NoError(t, rows.Scan(&instance, &version, &statuses))
		out[instance] = append(out[instance], fmt.Sprintf("%d:%s", version, statuses))
	}
	require.NoError(t, rows.Err())
	return out
}

// historyByInstance answers each instance's transition and observation
// events, as their state_to.
func (s *soak) historyByInstance(t *testing.T, states map[uuid.UUID]string) (transitions, observations map[string][]string) {
	t.Helper()
	e := s.e
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf(`SELECT ltbase_row_id, %s, %s FROM %s
		WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL`,
		e.column(t, HistorySchema, "instance_id"), e.column(t, HistorySchema, "type"), e.tables.entityMain), schemaIDs[HistorySchema])
	require.NoError(t, err)
	defer rows.Close()
	transitions, observations = map[string][]string{}, map[string][]string{}
	for rows.Next() {
		var row uuid.UUID
		var instance, kind string
		require.NoError(t, rows.Scan(&row, &instance, &kind))
		switch kind {
		case "transition":
			transitions[instance] = append(transitions[instance], states[row])
		case "observed":
			observations[instance] = append(observations[instance], states[row])
		default:
			t.Fatalf("history row %s has type %q", row, kind)
		}
	}
	require.NoError(t, rows.Err())
	return transitions, observations
}
