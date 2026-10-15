package internal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Group FIFO under contention (#653): eight relays claim partition heads
// and publish each claimed message in a Tx of its own, until every message
// is published. Each partition publishes in enqueued order, never has two
// messages publishing at once, and each message is published exactly once.
// It relies on PostgreSQL's skip, so it is _PostgresOnly.

const (
	fifoGroups   = 20
	fifoPerGroup = 10
	fifoWorkers  = 8
)

// fifoRun is the shared state of one contention run.
type fifoRun struct {
	env   *outboxPG
	topic string
	mu    sync.Mutex
	// progress is closed, and replaced, at every publish, so a relay whose
	// claim came back empty waits for one instead of spinning.
	progress  chan struct{}
	published int
	// draws counts the publishes begun. A publish draws its place inside
	// its Tx, before the commit, and the next message of a partition is
	// claimable only once that commit lands, so the draws of one partition
	// follow its commit order.
	draws int
	// order is each published row's draw.
	order      map[uuid.UUID]int
	violations []string
}

// draw answers the next place in publish order.
func (r *fifoRun) draw() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.draws++
	return r.draws
}

func (r *fifoRun) snapshot() (chan struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progress, r.published == fifoGroups*fifoPerGroup
}

func (r *fifoRun) violate(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.violations = append(r.violations, fmt.Sprintf(format, args...))
}

// publishedAfterCommit records rowID's publish, drawn at place, once its Tx
// has committed.
func (r *fifoRun) publishedAfterCommit(rowID uuid.UUID, place int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, twice := r.order[rowID]; twice {
		return fmt.Errorf("row %s published twice", rowID)
	}
	r.order[rowID] = place
	r.published++
	close(r.progress)
	r.progress = make(chan struct{})
	return nil
}

// relay claims heads and publishes them until every message is published.
func (r *fifoRun) relay(worker string) error {
	for {
		progress, done := r.snapshot()
		if done {
			return nil
		}
		records, err := r.env.claim(fifo(claimLeasing(r.topic, worker, 5, heldLease)))
		if err != nil {
			return fmt.Errorf("%s: claim: %w", worker, err)
		}
		r.checkOnePerGroup(records)
		r.checkNoGroupPublishingTwice()
		if len(records) == 0 {
			select {
			case <-progress:
			case <-time.After(lockWaitDeadline):
				return fmt.Errorf("%s: no message was published for %v", worker, lockWaitDeadline)
			}
			continue
		}
		for _, record := range records {
			if err := r.publish(record); err != nil {
				return fmt.Errorf("%s: %w", worker, err)
			}
		}
	}
}

func (r *fifoRun) publish(record *forma.DataRecord) error {
	g, ok := record.Attributes["lock_generation"].(int64)
	if !ok {
		return fmt.Errorf("row %s: lock_generation is %T", record.RowID, record.Attributes["lock_generation"])
	}
	var place int
	err := r.env.inTx(func(ctx context.Context, tx forma.Tx) error {
		_, err := tx.Update(ctx, complete(record.RowID, g))
		place = r.draw()
		return err
	})
	if err != nil {
		return fmt.Errorf("publish %s at generation %d: %w", record.RowID, g, err)
	}
	return r.publishedAfterCommit(record.RowID, place)
}

// checkOnePerGroup: a claim takes at most one row of a group (invariant 5).
func (r *fifoRun) checkOnePerGroup(records []*forma.DataRecord) {
	seen := map[any]bool{}
	for _, record := range records {
		key := record.Attributes["partition_key"]
		if seen[key] {
			r.violate("one claim took two rows of partition %v", key)
		}
		seen[key] = true
	}
}

// checkNoGroupPublishingTwice samples, by direct SQL, whether any partition
// has two messages publishing at once.
func (r *fifoRun) checkNoGroupPublishingTwice() {
	var partitions []string
	rows, err := r.env.pool.Query(r.env.ctx, fmt.Sprintf(`SELECT text_01 FROM %s
		WHERE ltbase_schema_id = $1 AND text_02 = 'publishing' GROUP BY text_01 HAVING count(*) > 1`,
		sanitizeIdentifier(r.env.tables.EntityMain)), outboxSchemaID)
	if err == nil {
		for rows.Next() {
			var p string
			if err = rows.Scan(&p); err != nil {
				break
			}
			partitions = append(partitions, p)
		}
		rows.Close()
	}
	if err != nil {
		r.violate("sample publishing partitions: %v", err)
	}
	for _, p := range partitions {
		r.violate("partition %s had two messages publishing at once", p)
	}
}

func TestClaimGroupFIFOUnderContention_PostgresOnly(t *testing.T) {
	env := newOutboxPG(t)
	for iteration := range 3 {
		run := &fifoRun{env: env, topic: fmt.Sprintf("fifo-%d", iteration), progress: make(chan struct{}), order: map[uuid.UUID]int{}}
		messages := make([]map[string]any, 0, fifoGroups*fifoPerGroup)
		// Partition-major: a claim that ignored its Group would take five
		// messages of one partition at once.
		for g := range fifoGroups {
			for k := range fifoPerGroup {
				messages = append(messages, partitioned(run.topic, fmt.Sprintf("it%d-p%02d", iteration, g), g*fifoPerGroup+k))
			}
		}
		// Row IDs descend against enqueued order, so FIFO is the group
		// order's, not the row-ID tiebreak's.
		ids := env.createDescending(t, iteration, messages)
		errs := race(fifoWorkers, func(w int) error { return run.relay(fmt.Sprintf("relay-%d", w)) })
		for w, err := range errs {
			require.NoError(t, err, "iteration %d relay %d", iteration, w)
		}
		require.Empty(t, run.violations, "iteration %d", iteration)
		require.Len(t, run.order, len(ids), "iteration %d: every message is published once", iteration)

		stamps := env.changeLogStamps(t)
		for g := range fifoGroups {
			group := ids[g*fifoPerGroup : (g+1)*fifoPerGroup] // in enqueued order
			for k := 1; k < len(group); k++ {
				assert.Less(t, run.order[group[k-1]], run.order[group[k]],
					"iteration %d partition %d: message %d committed its publish before message %d", iteration, g, k, k-1)
				assert.LessOrEqual(t, stamps[group[k-1]], stamps[group[k]],
					"iteration %d partition %d: change_log stamps follow enqueued order", iteration, g)
			}
			for _, id := range group {
				assert.Equal(t, "published", env.stored(t, id).status)
			}
		}
	}
}

// changeLogStamps answers each row's unflushed change_log stamp: the
// version of its last write. Stamps are per-row strictly increasing
// (GREATEST(now, previous + 1), #274) on the process clock, so across rows
// two writes in one millisecond can tie; the commit order of the publishes
// is the strict order, and the stamps must not contradict it.
func (e *outboxPG) changeLogStamps(t *testing.T) map[uuid.UUID]int64 {
	t.Helper()
	rows, err := e.pool.Query(e.ctx, fmt.Sprintf("SELECT row_id, changed_at FROM %s WHERE schema_id = $1 AND flushed_at = 0",
		sanitizeIdentifier(e.tables.ChangeLog)), outboxSchemaID)
	require.NoError(t, err)
	defer rows.Close()
	stamps := map[uuid.UUID]int64{}
	for rows.Next() {
		var id uuid.UUID
		var at int64
		require.NoError(t, rows.Scan(&id, &at))
		stamps[id] = at
	}
	require.NoError(t, rows.Err())
	return stamps
}
