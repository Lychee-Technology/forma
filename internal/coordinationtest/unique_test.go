package coordinationtest

import (
	"fmt"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Special scenario 4 and the uniqueness invariant: concurrent creations of
// one natural key never both commit.

func TestUniqueKeyScenario(t *testing.T) {
	e := newEnv(t)
	inst := e.seedInstance(t, 1)
	record := func() *EventRecord {
		return &EventRecord{InstanceID: inst.ID, IdempotencyKey: "approve-1", RequestHash: "h1", Event: "approve",
			Status: RecordSucceeded, ResultState: "approved", ResultVersion: 2}
	}
	var firstOutcome, secondOutcome forma.UpsertOutcome
	first := e.hold(t, func(uow UnitOfWork) error {
		var err error
		firstOutcome, err = uow.Idempotency().Upsert(e.ctx, record())
		return err
	})
	done := async(func() error {
		return e.inTx(func(uow UnitOfWork) error {
			var err error
			secondOutcome, err = uow.Idempotency().Upsert(e.ctx, record())
			return err
		})
	})
	answered, err := e.settle(t, done)
	firstErr := first.commit()
	second := e.collect(t, done, answered, err)

	t.Run("contract: of two upserts of a new (instance, key), exactly one is created", func(t *testing.T) {
		created := 0
		for _, o := range []struct {
			outcome forma.UpsertOutcome
			err     error
		}{{firstOutcome, firstErr}, {secondOutcome, second.err}} {
			if o.err == nil && o.outcome == forma.UpsertCreated {
				created++
			}
		}
		require.Equal(t, 1, created, "first %q %v, second %q %v", firstOutcome, firstErr, secondOutcome, second.err)
		require.Equal(t, 1, e.liveRows(t, IdempotencySchema))
		e.requireWritten(t, IdempotencySchema, nil, e.liveIDs(t, IdempotencySchema, "status", RecordSucceeded)...)
		e.requireOneHotEntryPerRow(t, IdempotencySchema)
	})
	t.Run("mechanism: the second waits on the key, then finds the succeeded record and fails its precondition", func(t *testing.T) {
		requirePostgresMechanisms(t)
		require.False(t, second.early, "the second upsert waited on the first")
		require.NoError(t, firstErr)
		require.Equal(t, forma.UpsertCreated, firstOutcome)
		requireMechanismLoser(t, second.err, forma.ErrPreconditionFailed)
	})
}

func TestUniqueness(t *testing.T) {
	const writers = 6
	cases := []struct {
		name   string
		schema string
		create func(e *env, inst *Instance, i int) func(UnitOfWork) error
	}{
		{"idempotency key", IdempotencySchema, func(e *env, inst *Instance, i int) func(UnitOfWork) error {
			return func(uow UnitOfWork) error {
				_, err := uow.Idempotency().Upsert(e.ctx, &EventRecord{InstanceID: inst.ID, IdempotencyKey: "k",
					RequestHash: fmt.Sprintf("h%d", i), Status: RecordSucceeded})
				return err
			}
		}},
		{"token hash", TaskSchema, func(e *env, inst *Instance, i int) func(UnitOfWork) error {
			return func(uow UnitOfWork) error {
				return uow.Tasks().Insert(e.ctx, task(inst, "hash-shared", TaskPending, fmt.Sprintf("event-%d", i), PurposeWorker))
			}
		}},
		{"parent task hash", LinkSchema, func(e *env, inst *Instance, i int) func(UnitOfWork) error {
			return func(uow UnitOfWork) error {
				return uow.Links().Insert(e.ctx, &Link{ParentTaskHash: "parent-shared", ParentInstanceID: inst.ID, Status: "pending"})
			}
		}},
		{"definition (name, version)", DefinitionSchema, func(e *env, _ *Instance, i int) func(UnitOfWork) error {
			return func(uow UnitOfWork) error {
				return uow.Definitions().SaveDraft(e.ctx, &Definition{Name: "approval", Version: 1, Body: fmt.Sprintf("body-%d", i)})
			}
		}},
		{"instance ID", InstanceSchema, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			inst := &Instance{ID: newID(), WorkflowName: workflowName, State: "start", Status: "running", Version: 1}
			create := func(i int) func(UnitOfWork) error {
				if c.create == nil {
					return func(uow UnitOfWork) error { return uow.Instances().Create(e.ctx, inst) }
				}
				return c.create(e, inst, i)
			}
			if c.create != nil {
				require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.Instances().Create(e.ctx, inst) }))
			}
			errs := race(writers, func(i int) error { return e.inTx(create(i)) })
			loser := forma.ErrUniqueConflict
			if c.schema == IdempotencySchema {
				loser = forma.ErrPreconditionFailed // the loser finds the winner's succeeded record
			}
			t.Run("contract: exactly one commits", func(t *testing.T) {
				require.Len(t, succeeded(errs), 1, "errors %v", errs)
				require.Equal(t, 1, e.liveRows(t, c.schema))
				for _, err := range errs {
					if err != nil {
						requireLoser(t, err, loser)
					}
				}
			})
			t.Run("mechanism: every loser answers the typed outcome", func(t *testing.T) {
				requirePostgresMechanisms(t)
				for _, err := range errs {
					if err != nil {
						requireMechanismLoser(t, err, loser)
					}
				}
			})
		})
	}
}
