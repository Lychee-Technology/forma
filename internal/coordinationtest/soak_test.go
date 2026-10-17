package coordinationtest

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The engine-shaped soak: 8 goroutines drive 50 instances through event
// steps while 4 relays claim, publish and fail the outbox messages the steps
// enqueue. Each step and each relay call is one adapter InTx, and none is
// retried (#654 rule 6). Goroutines 2k and 2k+1 step the same instance at the
// same time, and two such pairs step each instance, so most steps race
// another for a version. soak_check_test.go asserts the end state.

const (
	soakInstances = 50
	soakWorkers   = 8
	soakSteps     = 25
)

func TestEngineShapedSoak(t *testing.T) {
	for iteration := 0; iteration < 5; iteration++ {
		passed := t.Run(fmt.Sprintf("iteration %d", iteration), func(t *testing.T) {
			e := newEnv(t)
			s := newSoak(t, e)
			run := newRelayRun(e, 0)
			run.open, run.failEvery = true, 7
			relays := async(func() error { return errors.Join(run.run(4)...) })
			stepErrs := race(soakWorkers, func(g int) error { return s.drive(g, run) })
			run.enqueueDone()
			require.NoError(t, errors.Join(stepErrs...))
			require.NoError(t, e.result(t, relays))

			s.requireConsistent(t, run)
			require.NotZero(t, s.advances(), "an advance committed")
			t.Logf("%d steps advanced, %d fenced, %d lost a race (ErrPreconditionFailed); %d ErrRetryableConflict in steps, %d in relays",
				s.advances(), s.fences(), s.lost, s.retryable, run.retryable)
			t.Run("mechanism: no ErrRetryableConflict", func(t *testing.T) {
				requirePostgresMechanisms(t)
				require.Zero(t, s.retryable, "steps")
				require.Zero(t, run.retryable, "relays")
				s.requireEveryInstanceAdvanced(t)
			})
		})
		if !passed {
			break
		}
	}
}

// soak is one iteration's instances and what its committed steps did.
type soak struct {
	e         *env
	instances []uuid.UUID
	// seeded is each instance's change_log stamp after seeding.
	seeded map[uuid.UUID]int64
	// base and clock enqueue the messages: a step draws its messages'
	// enqueued_at after it read the instance, so an instance's messages are
	// enqueued in version order.
	base  time.Time
	clock atomic.Int64
	// enqueued counts the committed messages, for analyzeAt.
	enqueued atomic.Int64

	mu sync.Mutex
	// advanced holds what each instance's committed advances did, and
	// observed counts its committed fences.
	advanced  map[uuid.UUID][]advanceRecord
	observed  map[uuid.UUID]int
	lost      int
	retryable int
}

// advanceRecord is what one committed advance did.
type advanceRecord struct {
	// version is the version it reached.
	version int64
	// messages are its messages, in the order they must publish.
	messages []uuid.UUID
	// fences counts the fence records committed at the version it left,
	// as it read them after its update.
	fences int
}

// newSoak seeds the instances at version 1, each with its first visit's
// tasks.
func newSoak(t *testing.T, e *env) *soak {
	t.Helper()
	s := &soak{e: e, base: time.Now().Add(-time.Hour).Truncate(time.Millisecond),
		advanced: map[uuid.UUID][]advanceRecord{}, observed: map[uuid.UUID]int{}}
	require.NoError(t, e.inTx(func(uow UnitOfWork) error {
		for i := 0; i < soakInstances; i++ {
			inst := &Instance{ID: newID(), WorkflowName: workflowName, State: visitState(1), Status: "running", Version: 1}
			if err := uow.Instances().Create(e.ctx, inst); err != nil {
				return err
			}
			if err := insertVisit(e, uow, inst); err != nil {
				return err
			}
			s.instances = append(s.instances, inst.ID)
		}
		return nil
	}))
	s.seeded = e.stamps(t, InstanceSchema, s.instances...)
	e.analyze(t)
	return s
}

func visitState(version int64) string { return fmt.Sprintf("s%d", version) }

func visitHash(inst *Instance, purpose string) string {
	return fmt.Sprintf("%s/%d/%s", inst.ID, inst.Version, purpose)
}

// insertVisit creates the tasks of inst's current visit: a worker task the
// next step completes and a timer task it cancels.
func insertVisit(e *env, uow UnitOfWork, inst *Instance) error {
	for _, purpose := range []string{PurposeWorker, PurposeTimer} {
		if err := uow.Tasks().Insert(e.ctx, task(inst, visitHash(inst, purpose), TaskPending, "advance", purpose)); err != nil {
			return err
		}
	}
	return nil
}

// drive runs goroutine g's steps: pair g/2 steps instances 4·step + g/2,
// so the four pairs step every instance twice. In each pair, one goroutine
// advances the instance and the other fences it every fourth step and
// advances it otherwise. A step that lost its race is counted, not retried.
func (s *soak) drive(g int, run *relayRun) error {
	for step := 0; step < soakSteps; step++ {
		id := s.instances[(step*4+g/2)%soakInstances]
		var err error
		if (step+g)%4 == 3 {
			err = s.fence(id, fmt.Sprintf("observe-%d-%d", g, step))
		} else {
			err = s.advance(id, run)
		}
		switch {
		case err == nil:
		case errors.Is(err, forma.ErrPreconditionFailed):
			s.count(&s.lost)
		case errors.Is(err, forma.ErrRetryableConflict):
			s.count(&s.retryable)
		default:
			return fmt.Errorf("goroutine %d step %d on instance %s: %w", g, step, id, err)
		}
	}
	return nil
}

// advance is an event step that moves the instance to its next version.
func (s *soak) advance(id uuid.UUID, run *relayRun) error {
	var rec advanceRecord
	err := s.e.inTx(func(uow UnitOfWork) error {
		var err error
		rec, err = s.advanceIn(uow, id)
		return err
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.advanced[id] = append(s.advanced[id], rec)
	s.mu.Unlock()
	run.enqueued(len(rec.messages))
	return s.analyzeAt(len(rec.messages))
}

// analyzeEvery is how many committed messages the soak lets pass between
// two ANALYZEs.
const analyzeEvery = 50

// analyzeAt counts n more committed messages and analyzes the tables each
// time the count passes a multiple of analyzeEvery. Autovacuum analyzes a
// table once 50 rows and a tenth of it have changed, but looks only once a
// minute, longer than an iteration lasts. On statistics from before the
// messages existed, a claim can take seconds, and an iteration took 5 to
// 61 s instead of about 2 s (#685).
func (s *soak) analyzeAt(n int) error {
	total := s.enqueued.Add(int64(n))
	if total/analyzeEvery == (total-int64(n))/analyzeEvery {
		return nil
	}
	return s.e.analyzeTables()
}

// advanceIn runs an advance's writes, in a fixed order: read the instance,
// update it at its version, count the fences at that version, resolve the
// visit's tasks and create the next visit's, then write history and two
// outbox messages enqueued at one instant.
func (s *soak) advanceIn(uow UnitOfWork, id uuid.UUID) (advanceRecord, error) {
	ctx := s.e.ctx
	inst, err := uow.Instances().Get(ctx, id)
	if err != nil {
		return advanceRecord{}, err
	}
	next := *inst
	next.Version, next.State = inst.Version+1, visitState(inst.Version+1)
	if err := uow.Instances().Update(ctx, &next, inst.Version); err != nil {
		return advanceRecord{}, err
	}
	// The update holds the instance, so no fence at inst.Version can commit
	// from here on (invariant 2): the count read now must be final.
	fences, err := queryAll(ctx, uow.(*unitOfWork).tx, IdempotencySchema,
		all(equals("instance_id", id), equals("result_version", inst.Version)))
	if err != nil {
		return advanceRecord{}, err
	}
	if _, _, err := uow.Tasks().CompleteMatchingAndCancelPending(ctx, id, inst.State, inst.Version, "advance"); err != nil {
		return advanceRecord{}, err
	}
	worker, err := uow.Tasks().GetByTokenHash(ctx, visitHash(inst, PurposeWorker))
	if err != nil {
		return advanceRecord{}, err
	}
	if worker.Status != TaskCompleted {
		return advanceRecord{}, fmt.Errorf("Tx.Query after the Tx's UpdateWhere reads worker task %s as %s", worker.TokenHash, worker.Status)
	}
	if err := insertVisit(s.e, uow, &next); err != nil {
		return advanceRecord{}, err
	}
	if err := uow.History().Insert(ctx, []HistoryEvent{{InstanceID: id, Type: "transition", StateFrom: inst.State, StateTo: next.State}}); err != nil {
		return advanceRecord{}, err
	}
	msgs := s.visitMessages(id, next.Version)
	inserted := slices.Clone(msgs)
	slices.Reverse(inserted)
	if err := uow.Outbox().Insert(ctx, inserted); err != nil {
		return advanceRecord{}, err
	}
	return advanceRecord{version: next.Version, messages: ids(msgs), fences: len(fences)}, nil
}

// visitMessages answers the two messages of the instance's visit at
// version, enqueued at one instant as LTFlow enqueues a step's messages at
// one now(), in the order their row IDs must publish them.
func (s *soak) visitMessages(id uuid.UUID, version int64) []OutboxMessage {
	at := s.base.Add(time.Duration(s.clock.Add(1)) * time.Millisecond)
	msgs := make([]OutboxMessage, 2)
	for i, rowID := range ascendingIDs(len(msgs)) {
		msgs[i] = message(id.String(), at)
		msgs[i].ID, msgs[i].CommandType = rowID, []string{"transition", "notify"}[i]
		msgs[i].Payload = fmt.Sprintf(`{"version":%d}`, version)
	}
	return msgs
}

// fence is an event step that leaves the instance where it is: read it,
// fence its version, record the event's outcome and its history.
func (s *soak) fence(id uuid.UUID, key string) error {
	ctx := s.e.ctx
	err := s.e.inTx(func(uow UnitOfWork) error {
		inst, err := uow.Instances().Get(ctx, id)
		if err != nil {
			return err
		}
		if err := uow.Instances().FenceVersion(ctx, id, inst.Version); err != nil {
			return err
		}
		if _, err := uow.Idempotency().Upsert(ctx, &EventRecord{InstanceID: id, IdempotencyKey: key, RequestHash: key,
			Event: "observe", Status: RecordSucceeded, ResultState: inst.State, ResultVersion: inst.Version}); err != nil {
			return err
		}
		return uow.History().Insert(ctx, []HistoryEvent{{InstanceID: id, Type: "observed", StateFrom: inst.State, StateTo: inst.State}})
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.observed[id]++
	s.mu.Unlock()
	return nil
}

func (s *soak) count(n *int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*n++
}

func (s *soak) advances() (n int) {
	for _, recs := range s.advanced {
		n += len(recs)
	}
	return n
}

func (s *soak) fences() (n int) {
	for _, c := range s.observed {
		n += c
	}
	return n
}
