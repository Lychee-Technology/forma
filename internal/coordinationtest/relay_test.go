package coordinationtest

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// relayRun drives relays that claim outbox messages and publish them, each
// claim and each completion in an adapter InTx of its own, until every
// message is published. No call is retried (#654 rule 6): a relay that gets
// ErrRetryableConflict counts it and claims again.
type relayRun struct {
	e     *env
	total int
	// open, while set, keeps the relays running past total: more messages
	// are being enqueued (enqueued, enqueueDone).
	open  bool
	lease time.Duration
	limit int
	// failEvery, when not zero, makes every failEvery-th claimed message
	// fail once with MarkFailed, due again at once, before it is published.
	failEvery int

	mu sync.Mutex
	// progress is closed, and replaced, at every completion and every
	// enqueue, so a relay whose claim came back empty waits for one instead
	// of spinning.
	progress chan struct{}
	// draws counts the publishes begun. A publish draws its place inside
	// its Tx, before the commit, and a partition's next message is
	// claimable only once that commit lands, so one partition's draws
	// follow its publish commit order.
	draws     int
	published map[uuid.UUID]int
	failed    map[uuid.UUID]bool
	claimed   int
	retryable int
}

func newRelayRun(e *env, total int) *relayRun {
	return &relayRun{e: e, total: total, lease: time.Hour, limit: 3, progress: make(chan struct{}),
		published: map[uuid.UUID]int{}, failed: map[uuid.UUID]bool{}}
}

// run runs n relays to completion and answers each relay's error.
func (r *relayRun) run(n int) []error {
	return race(n, func(i int) error { return r.relay(fmt.Sprintf("relay-%d", i)) })
}

func (r *relayRun) relay(name string) error {
	for {
		progress, done := r.snapshot()
		if done {
			return nil
		}
		var msgs []OutboxMessage
		err := r.e.inTx(func(uow UnitOfWork) error {
			var err error
			msgs, err = uow.Outbox().ClaimBatch(r.e.ctx, name, r.lease, r.limit)
			return err
		})
		if r.countRetryable(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: claim: %w", name, err)
		}
		if len(msgs) == 0 {
			if err := r.await(progress); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			continue
		}
		r.note(len(msgs))
		for _, m := range msgs {
			if err := r.complete(name, m); err != nil {
				return err
			}
		}
	}
}

// complete publishes m, or fails it once when failEvery picks it.
func (r *relayRun) complete(name string, m OutboxMessage) error {
	if r.pickFailure(m.ID) {
		err := r.e.inTx(func(uow UnitOfWork) error {
			return uow.Outbox().MarkFailed(r.e.ctx, m.ID, m.LockGeneration, "transient", time.Now().Add(-time.Second))
		})
		if r.countRetryable(err) {
			return r.recover(name, m)
		}
		if err != nil {
			return fmt.Errorf("%s: fail %s: %w", name, m.ID, err)
		}
		r.advance()
		return nil
	}
	var place int
	err := r.e.inTx(func(uow UnitOfWork) error {
		err := uow.Outbox().MarkPublished(r.e.ctx, m.ID, m.LockGeneration)
		place = r.draw()
		return err
	})
	if r.countRetryable(err) {
		return r.recover(name, m)
	}
	if err != nil {
		return fmt.Errorf("%s: publish %s at generation %d: %w", name, m.ID, m.LockGeneration, err)
	}
	return r.publishedAt(m.ID, place)
}

// recover ends the lease of a message whose completion failed, as scenario
// 7 sets one up, so that a relay reclaims it rather than waiting out the
// lease.
func (r *relayRun) recover(name string, m OutboxMessage) error {
	if err := r.e.tryExpireLease(m); err != nil {
		return fmt.Errorf("%s: expire the lease of %s after a failed completion: %w", name, m.ID, err)
	}
	r.advance()
	return nil
}

func (r *relayRun) snapshot() (chan struct{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progress, !r.open && len(r.published) == r.total
}

// enqueued counts n more messages to publish, committed while the relays
// run, and wakes the relays waiting for work.
func (r *relayRun) enqueued(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.total += n
	r.advanceLocked()
}

// enqueueDone ends an open run's enqueueing: the relays stop once every
// message counted is published.
func (r *relayRun) enqueueDone() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.open = false
	r.advanceLocked()
}

func (r *relayRun) await(progress chan struct{}) error {
	select {
	case <-progress:
		return nil
	case <-time.After(20 * time.Second):
		return errors.New("no message was completed or enqueued for 20 s")
	case <-r.e.ctx.Done():
		return fmt.Errorf("the relays did not drain the outbox: %w", r.e.ctx.Err())
	}
}

func (r *relayRun) note(claimed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimed += claimed
}

func (r *relayRun) pickFailure(id uuid.UUID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failEvery == 0 || r.failed[id] || (r.claimed+len(r.failed))%r.failEvery != 0 {
		return false
	}
	r.failed[id] = true
	return true
}

func (r *relayRun) draw() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.draws++
	return r.draws
}

func (r *relayRun) publishedAt(id uuid.UUID, place int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, twice := r.published[id]; twice {
		return fmt.Errorf("message %s published twice", id)
	}
	r.published[id] = place
	r.advanceLocked()
	return nil
}

func (r *relayRun) advance() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advanceLocked()
}

func (r *relayRun) advanceLocked() {
	close(r.progress)
	r.progress = make(chan struct{})
}

// countRetryable counts err when it is ErrRetryableConflict, a failed
// attempt that is not re-run, and reports whether it was.
func (r *relayRun) countRetryable(err error) bool {
	if !errors.Is(err, forma.ErrRetryableConflict) {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retryable++
	return true
}
