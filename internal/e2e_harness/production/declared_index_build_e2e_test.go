//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/declaredindex"
	"github.com/lychee-technology/forma/internal/model"
)

const outboxWriters = 4

// TestDeclaredIndexBuildUnderConcurrentWrites is the issue's concurrency
// test: the declared-index step builds while writers on their own pool
// connections keep creating and updating outbox rows through the
// EntityManager, and no write fails.
//
// The overlap is arranged, not hoped for. A barrier releases the step once
// every writer has committed; an open ROW EXCLUSIVE transaction then holds the
// first build in its "waiting for writers" phase, and only after every writer
// has committed again while the build waits is that transaction ended. The
// test then asserts zero write errors, valid indexes that amcheck finds
// complete against the heap, and every committed write readable through
// Forma.
func TestDeclaredIndexBuildUnderConcurrentWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	planned := plannedDeclaredIndexes(t, env)
	for _, ix := range planned {
		env.ExecSQL(ctx, "DROP INDEX CONCURRENTLY "+pgx.Identifier{ix.Name}.Sanitize())
	}

	writes := newWriteLog(outboxWriters)
	manager := env.EntityManager()
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := range outboxWriters {
		writers.Add(1)
		go func() {
			defer writers.Done()
			runOutboxWriter(ctx, manager, w, writes, stop)
		}()
	}
	stopWriters := sync.OnceFunc(func() { close(stop); writers.Wait() })
	t.Cleanup(stopWriters)

	writes.awaitEach(t, nil)
	builder, pid := connectBackend(ctx, t, env)
	blocker := holdRowExclusive(ctx, t, env)
	buildStart := time.Now()
	built := make(chan error, 1)
	var report declaredindex.Report
	go func() {
		var err error
		report, err = declaredindex.Ensure(ctx, builder, env.Tables.EntityMain, planned)
		built <- err
	}()
	waitForBuildPhase(ctx, t, env, pid, "waiting for writers before build")
	writes.awaitEach(t, writes.counts())
	if err := blocker.Commit(ctx); err != nil {
		t.Fatalf("end the open write transaction: %v", err)
	}
	if err := <-built; err != nil {
		t.Fatalf("declared-index step under writes: %v", err)
	}
	buildEnd := time.Now()
	stopWriters()

	writes.assertNoErrors(t)
	writes.logLatency(t, buildStart, buildEnd)
	if len(report.Created) != len(planned) {
		t.Fatalf("the step built %d indexes, want %d", len(report.Created), len(planned))
	}
	assertDeclaredIndexesComplete(ctx, t, env, planned)
	assertWritesReadable(ctx, t, env, writes)
}

// writeLog is what the writers committed, per writer: a commit count the
// barriers wait on, each row's last committed status, and every write's
// latency and outcome.
type writeLog struct {
	mu      sync.Mutex
	changed *sync.Cond
	commits []int
	rows    []map[uuid.UUID]string
	timings []writeTiming
	errs    []error
}

type writeTiming struct {
	end     time.Time
	latency time.Duration
}

func newWriteLog(writers int) *writeLog {
	l := &writeLog{commits: make([]int, writers), rows: make([]map[uuid.UUID]string, writers)}
	for w := range l.rows {
		l.rows[w] = map[uuid.UUID]string{}
	}
	l.changed = sync.NewCond(&l.mu)
	return l
}

func (l *writeLog) record(w int, row uuid.UUID, status string, started time.Time, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.timings = append(l.timings, writeTiming{end: time.Now(), latency: time.Since(started)})
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("writer %d: %w", w, err))
	} else {
		l.commits[w]++
		l.rows[w][row] = status
	}
	l.changed.Broadcast()
}

func (l *writeLog) counts() []int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.commits)
}

// awaitEach blocks until every writer has committed more writes than after
// (nil: at least one), and fails the test as soon as any write fails.
func (l *writeLog) awaitEach(t *testing.T, after []int) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if len(l.errs) > 0 {
			t.Fatalf("a write failed before the barrier: %v", errors.Join(l.errs...))
		}
		ready := true
		for w, n := range l.commits {
			if after == nil && n == 0 || after != nil && n <= after[w] {
				ready = false
			}
		}
		if ready {
			return
		}
		l.changed.Wait()
	}
}

func (l *writeLog) assertNoErrors(t *testing.T) {
	t.Helper()
	if len(l.errs) > 0 {
		t.Fatalf("%d writes failed during the declared-index build: %v", len(l.errs), errors.Join(l.errs...))
	}
}

// logLatency reports write latency inside and outside the build window. It
// records, and does not assert: a CI runner's latency is too noisy to gate on.
func (l *writeLog) logLatency(t *testing.T, from, to time.Time) {
	t.Helper()
	var during, outside []time.Duration
	for _, w := range l.timings {
		if w.end.After(from) && w.end.Before(to) {
			during = append(during, w.latency)
		} else {
			outside = append(outside, w.latency)
		}
	}
	t.Logf("writes during the build (%s): %s; outside it: %s", to.Sub(from), latencySummary(during), latencySummary(outside))
	if len(during) == 0 {
		t.Fatalf("no write committed during the build window")
	}
}

func latencySummary(d []time.Duration) string {
	if len(d) == 0 {
		return "none"
	}
	slices.Sort(d)
	return fmt.Sprintf("%d writes, p50 %s, p99 %s, max %s", len(d), d[len(d)/2], d[len(d)*99/100], d[len(d)-1])
}

// runOutboxWriter creates an outbox row in its own partition and leases it,
// over and over, until stop closes.
func runOutboxWriter(ctx context.Context, manager forma.EntityManager, w int, writes *writeLog, stop <-chan struct{}) {
	partition := fmt.Sprintf("writer-%d", w)
	enqueued := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for seq := 0; ; seq++ {
		select {
		case <-stop:
			return
		default:
		}
		started := time.Now()
		record, err := manager.Create(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "e2e_outbox"},
			Type:             forma.OperationCreate,
			Data: map[string]any{
				"status": "pending", "partition_key": partition,
				"next_attempt_at": started.UTC().Format(time.RFC3339),
				"enqueued_at":     enqueued.Add(time.Duration(seq) * time.Second).Format(time.RFC3339),
			},
		})
		if err != nil {
			writes.record(w, uuid.Nil, "", started, fmt.Errorf("create: %w", err))
			continue
		}
		writes.record(w, record.RowID, "pending", started, nil)

		started = time.Now()
		_, err = manager.Update(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "e2e_outbox", RowID: record.RowID},
			Type:             forma.OperationUpdate,
			Updates:          map[string]any{"status": "leased", "locked_until": started.Add(time.Minute).UTC().Format(time.RFC3339)},
		})
		if err != nil {
			err = fmt.Errorf("update %s: %w", record.RowID, err)
		}
		writes.record(w, record.RowID, "leased", started, err)
	}
}

// connectBackend opens a dedicated connection for the declared-index step and
// returns its backend pid, which pg_stat_progress_create_index reports.
func connectBackend(ctx context.Context, t *testing.T, env *Env) (*pgx.Conn, int32) {
	t.Helper()
	conn, err := pgx.Connect(ctx, env.PGDSN())
	if err != nil {
		t.Fatalf("connect the index builder: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	var pid int32
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("read the builder's pid: %v", err)
	}
	return conn, pid
}

// holdRowExclusive opens a transaction holding the lock every writer takes on
// entity_main. CREATE INDEX CONCURRENTLY waits for it before building.
func holdRowExclusive(ctx context.Context, t *testing.T, env *Env) pgx.Tx {
	t.Helper()
	conn, _ := connectBackend(ctx, t, env)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin the open write transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE entity_main IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatalf("take ROW EXCLUSIVE on entity_main: %v", err)
	}
	return tx
}

// waitForBuildPhase polls pg_stat_progress_create_index until backend pid's
// build reports phase. Progress reporting has no notification to wait on.
func waitForBuildPhase(ctx context.Context, t *testing.T, env *Env, pid int32, phase string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		var current string
		err := env.Pool.QueryRow(ctx, `SELECT COALESCE((SELECT phase FROM pg_stat_progress_create_index WHERE pid = $1), '')`, pid).Scan(&current)
		switch {
		case err != nil:
			t.Fatalf("read build progress: %v", err)
		case current == phase:
			return
		case time.Now().After(deadline):
			t.Fatalf("build by pid %d never reached %q (last phase %q)", pid, phase, current)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertDeclaredIndexesComplete checks each built index is valid and, through
// amcheck's heapallindexed verification, holds an entry for every heap tuple:
// no write that landed during the build is missing from it.
func assertDeclaredIndexesComplete(ctx context.Context, t *testing.T, env *Env, planned []declaredindex.Index) {
	t.Helper()
	env.ExecSQL(ctx, "CREATE EXTENSION IF NOT EXISTS amcheck")
	catalog := entityMainIndexes(ctx, t, env)
	for _, ix := range planned {
		if got, ok := catalog[ix.Name]; !ok || !got.valid {
			t.Fatalf("declared index %s missing or invalid after the build: %+v", ix.Name, got)
		}
		if _, err := env.Pool.Exec(ctx, "SELECT bt_index_check($1::regclass, true)", ix.Name); err != nil {
			t.Fatalf("amcheck on %s: %v", ix.Name, err)
		}
	}
}

// assertWritesReadable reads each writer's partition back through the
// repository and requires exactly the rows it committed, in their last
// committed status.
func assertWritesReadable(ctx context.Context, t *testing.T, env *Env, writes *writeLog) {
	t.Helper()
	repo := internal.NewDBPersistentRecordRepository(env.Pool, env.Metadata)
	outbox := SchemaRef{ID: SchemaIDOutbox, Name: "e2e_outbox"}
	for w, want := range writes.rows {
		page, err := repo.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
			Tables: env.Tables, SchemaID: outbox.ID, Condition: eq("partition_key", fmt.Sprintf("writer-%d", w)), Limit: 100_000,
		})
		if err != nil {
			t.Fatalf("read writer %d's rows: %v", w, err)
		}
		got := map[uuid.UUID]string{}
		for _, rec := range page.Records {
			got[rec.RowID] = rec.TextItems["text_01"]
		}
		if len(got) != len(want) {
			t.Fatalf("writer %d committed %d rows, %d are readable", w, len(want), len(got))
		}
		for row, status := range want {
			if got[row] != status {
				t.Fatalf("writer %d's row %s reads %q, last committed %q", w, row, got[row], status)
			}
		}
	}
}

// TestEvolveSchemaBuildsNewlyDeclaredIndexes pins the harness's restart path:
// a generation that adds a declaration gets its index built by EvolveSchema,
// as init-db would build it before the restart, and the existing declared
// indexes are kept.
func TestEvolveSchemaBuildsNewlyDeclaredIndexes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	before := plannedDeclaredIndexes(t, env)

	dir := copyFixtureSchemaDir(t)
	path := filepath.Join(dir, "e2e_outbox.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read e2e_outbox.json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode e2e_outbox.json: %v", err)
	}
	doc["x-forma-indexes"] = append(doc["x-forma-indexes"].([]any),
		map[string]any{"name": "by_partition", "attributes": []any{"partition_key"}})
	if raw, err = json.Marshal(doc); err != nil {
		t.Fatalf("encode e2e_outbox.json: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write e2e_outbox.json: %v", err)
	}

	if err := env.EvolveSchema(ctx, dir); err != nil {
		t.Fatalf("evolve schema: %v", err)
	}
	added := declaredIndexFor(t, env, "e2e_outbox.by_partition")
	if want := []string{"ltbase_schema_id", "text_02", "ltbase_row_id"}; !slices.Equal(added.Columns, want) {
		t.Fatalf("by_partition planned over %v, want %v", added.Columns, want)
	}
	catalog := entityMainIndexes(ctx, t, env)
	for _, ix := range append(before, added) {
		if got, ok := catalog[ix.Name]; !ok || !got.valid {
			t.Fatalf("declared index %s missing or invalid after the evolution: %+v", ix.Name, got)
		}
	}
}
