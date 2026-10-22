package coordinationtest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// requireDSQLEngine skips a test of the aurora-dsql engine (#633) on a
// backend whose manager runs the PostgreSQL engine.
func requireDSQLEngine(t *testing.T) {
	t.Helper()
	if b := activeBackend(); b.engine() != forma.EngineAuroraDSQL {
		t.Skipf("aurora-dsql engine only, the backend is %q", b.name)
	}
}

// dsqlUnsupported are the PostgreSQL features Aurora DSQL lacks or
// restricts that a write could reach for: advisory locks, row-locking
// clauses, and an isolation level other than its fixed REPEATABLE READ.
var dsqlUnsupported = []string{"ADVISORY", "FOR UPDATE", "FOR SHARE", "FOR NO KEY", "SKIP LOCKED", "NOWAIT", "ISOLATION LEVEL"}

// On the aurora-dsql engine, one InTx that reaches every per-row lock the
// write path takes sends none of dsqlUnsupported: the lock of one row (a CAS
// update, a fence, a create under a unique key), of a row set (a predicate
// update and delete), the claim's non-blocking lock and re-check, and an
// upsert's, both when it creates and when it resolves the conflict with the
// row it created. Every transaction it opens begins with a plain BEGIN.
func TestDSQLEngineSendsOnlyStatementsDSQLRuns(t *testing.T) {
	requireDSQLEngine(t)
	e := newEnv(t)
	inst := e.seedInstance(t, 1)
	fenced := e.seedInstance(t, 4)
	done := task(inst, "hash-done", TaskCompleted, "approve", PurposeWorker)
	eligible := time.Now().Add(-time.Hour)
	done.EligibleCleanupAt = &eligible
	e.seedTasks(t, task(inst, "hash-pending", TaskPending, "approve", PurposeWorker), done)
	e.seedOutbox(t, 2, 1)

	statements := e.tracer.record(func() {
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return everyLockingPath(e, uow, inst, fenced) }))
	})

	var begins int
	for _, s := range statements {
		sql := strings.ToUpper(s.sql)
		for _, construct := range dsqlUnsupported {
			require.NotContains(t, sql, construct, "a statement Aurora DSQL does not run:\n%s", s.sql)
		}
		if strings.HasPrefix(strings.TrimSpace(sql), "BEGIN") {
			begins++
			require.Equal(t, "BEGIN", strings.TrimSpace(sql), "a transaction begins at DSQL's one isolation level")
		}
	}
	require.Equal(t, 1, begins, "the InTx is one transaction, and the audit saw it begin")
	require.Equal(t, map[string]any{"dedup_count": int64(1)}, e.stored(t, HistorySchema, DedupRowID(e.app), "dedup_count"),
		"the second upsert resolved its conflict rather than failing its precondition")
}

// everyLockingPath runs, in uow, a write of each per-row locking path.
func everyLockingPath(e *env, uow UnitOfWork, inst, fenced *Instance) error {
	if err := uow.Instances().Update(e.ctx, advanced(inst, "shipping"), 1); err != nil {
		return fmt.Errorf("cas update: %w", err)
	}
	if err := uow.Instances().FenceVersion(e.ctx, fenced.ID, 4); err != nil {
		return fmt.Errorf("fence: %w", err)
	}
	if err := uow.Tasks().Insert(e.ctx, task(inst, "hash-new", TaskPending, "ship", PurposeWorker)); err != nil {
		return fmt.Errorf("create under a unique key: %w", err)
	}
	if err := uow.Tasks().UpdateStatus(e.ctx, "hash-pending", TaskPending, TaskCompleted); err != nil {
		return fmt.Errorf("predicate update: %w", err)
	}
	if n, err := uow.Tasks().DeleteEligible(e.ctx, time.Now(), 10); err != nil || n != 1 {
		return fmt.Errorf("predicate delete removed %d tasks: %w", n, err)
	}
	if claimed, err := uow.Outbox().ClaimBatch(e.ctx, "relay", time.Hour, 5); err != nil || len(claimed) != 2 {
		return fmt.Errorf("claim took %d messages: %w", len(claimed), err)
	}
	evt := HistoryEvent{InstanceID: inst.ID, Type: "stuck", Details: e.app}
	if err := uow.History().UpsertDeduped(e.ctx, evt, e.app, time.Hour); err != nil {
		return fmt.Errorf("upsert that creates: %w", err)
	}
	if err := uow.History().UpsertDeduped(e.ctx, evt, e.app, time.Hour); err != nil {
		return fmt.Errorf("upsert that resolves its conflict: %w", err)
	}
	return nil
}

// The CI Test job runs the suite a second time on the dsql-emulated backend,
// the only CI run of its contract assertions on the aurora-dsql engine.
func TestSuiteRunsOnTheDSQLEngineInCI(t *testing.T) {
	ci, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	require.NoError(t, err)
	step := string(ci)
	start := strings.Index(step, "FORMA_COORDINATION_BACKEND: "+dsqlEmulated)
	require.NotEqual(t, -1, start, "a CI step names the %s backend", dsqlEmulated)
	run := strings.SplitN(step[start:], "run:", 2)
	require.Len(t, run, 2, "the step runs a command")
	command := strings.TrimSpace(strings.SplitN(run[1], "\n", 2)[0])
	require.Equal(t, "go test -count=1 ./internal/coordinationtest/", command)
}
