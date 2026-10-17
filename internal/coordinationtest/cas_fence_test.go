package coordinationtest

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Special scenarios 2 (CAS) and 3 (fence). Each interleaving runs once; its
// contract subtest checks the outcome every backend owes, its mechanism
// subtest how PostgreSQL reaches it.

// requireLoser asserts err is an outcome a losing transaction may answer:
// want, or on any backend ErrRetryableConflict.
func requireLoser(t *testing.T, err, want error) {
	t.Helper()
	require.True(t, errors.Is(err, want) || errors.Is(err, forma.ErrRetryableConflict),
		"a loser answers %v or ErrRetryableConflict, got %v", want, err)
}

// requireMechanismLoser asserts err is want and not ErrRetryableConflict,
// as PostgreSQL answers every loser.
func requireMechanismLoser(t *testing.T, err, want error) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.NotErrorIs(t, err, forma.ErrRetryableConflict)
}

func TestCASScenario(t *testing.T) {
	e := newEnv(t)
	inst := e.seedInstance(t, 7)
	before := e.stamps(t, InstanceSchema, inst.ID)
	update := func(state string) func(UnitOfWork) error {
		return func(uow UnitOfWork) error { return uow.Instances().Update(e.ctx, advanced(inst, state), 7) }
	}
	first := e.hold(t, update("first"))
	done := async(func() error { return e.inTx(update("second")) })
	answered, err := e.settle(t, done)
	firstErr := first.commit()
	second := e.collect(t, done, answered, err)

	t.Run("contract: of two updates expecting version 7, exactly one commits", func(t *testing.T) {
		require.True(t, (firstErr == nil) != (second.err == nil), "first %v, second %v", firstErr, second.err)
		winner := "first"
		if firstErr != nil {
			requireLoser(t, firstErr, forma.ErrPreconditionFailed)
			winner = "second"
		} else {
			requireLoser(t, second.err, forma.ErrPreconditionFailed)
		}
		require.Equal(t, map[string]any{"state": winner, "version": int64(8)}, e.stored(t, InstanceSchema, inst.ID, "state", "version"))
		e.requireWritten(t, InstanceSchema, before, inst.ID)
	})
	t.Run("mechanism: the second waits for the first, then fails its precondition", func(t *testing.T) {
		requirePostgresMechanisms(t)
		require.False(t, second.early, "the second update waited on the first's lock")
		require.NoError(t, firstErr)
		requireMechanismLoser(t, second.err, forma.ErrPreconditionFailed)
	})
}

// fenceScenario holds an instance at version 7 and the two transactions of
// #650's scenario 3: the fencer fences the instance at 7 and writes a
// dependent idempotency record; the advancer updates it to 8.
type fenceScenario struct {
	e    *env
	inst *Instance
	// before and entries are the instance's change_log as seeded.
	before  map[uuid.UUID]int64
	entries [][2]int64
}

func newFenceScenario(t *testing.T) *fenceScenario {
	e := newEnv(t)
	inst := e.seedInstance(t, 7)
	return &fenceScenario{e: e, inst: inst, before: e.stamps(t, InstanceSchema, inst.ID),
		entries: e.changeLogEntries(t, InstanceSchema, inst.ID)}
}

func (s *fenceScenario) fence(uow UnitOfWork) error {
	if err := uow.Instances().FenceVersion(s.e.ctx, s.inst.ID, 7); err != nil {
		return err
	}
	_, err := uow.Idempotency().Upsert(s.e.ctx, &EventRecord{InstanceID: s.inst.ID, IdempotencyKey: "approve-1",
		RequestHash: "h1", Event: "approve", Status: RecordSucceeded, ResultState: "start", ResultVersion: 7})
	return err
}

func (s *fenceScenario) advance(uow UnitOfWork) error {
	return uow.Instances().Update(s.e.ctx, advanced(s.inst, "approved"), 7)
}

// requireOutcome asserts the committed state: the dependent record exists,
// with its change_log entry, exactly when the fencer committed, and the
// version is 8, with a new stamp, exactly when the advancer committed.
func (s *fenceScenario) requireOutcome(t *testing.T, fenceErr, advanceErr error) {
	t.Helper()
	records := s.e.liveIDs(t, IdempotencySchema, "status", RecordSucceeded)
	require.Equal(t, fenceErr == nil, len(records) == 1, "the dependent record commits with the fence, fence error %v", fenceErr)
	s.e.requireWritten(t, IdempotencySchema, nil, records...)
	s.e.requireOneHotEntryPerRow(t, IdempotencySchema)
	if advanceErr != nil {
		require.Equal(t, map[string]any{"version": int64(7)}, s.e.stored(t, InstanceSchema, s.inst.ID, "version"))
		require.Equal(t, s.entries, s.e.changeLogEntries(t, InstanceSchema, s.inst.ID), "the failed advance wrote no change_log entry")
		return
	}
	require.Equal(t, map[string]any{"version": int64(8)}, s.e.stored(t, InstanceSchema, s.inst.ID, "version"))
	s.e.requireWritten(t, InstanceSchema, s.before, s.inst.ID)
}

func TestFenceScenario(t *testing.T) {
	t.Run("a: assert first", func(t *testing.T) {
		s := newFenceScenario(t)
		fencer := s.e.hold(t, s.fence)
		done := async(func() error { return s.e.inTx(s.advance) })
		answered, err := s.e.settle(t, done)
		fenceErr := fencer.commit()
		advancer := s.e.collect(t, done, answered, err)
		t.Run("contract: no fenced commit after the instance advanced", func(t *testing.T) {
			if advancer.early && advancer.err == nil {
				require.Error(t, fenceErr, "the advance committed while the fencer was open")
			}
			s.requireOutcome(t, fenceErr, advancer.err)
		})
		t.Run("mechanism: the advance blocks until the fencer commits, then commits", func(t *testing.T) {
			requirePostgresMechanisms(t)
			require.False(t, advancer.early, "the update waited on the fence")
			require.NoError(t, fenceErr)
			require.NoError(t, advancer.err)
		})
	})

	t.Run("b: advance committed first", func(t *testing.T) {
		s := newFenceScenario(t)
		require.NoError(t, s.e.inTx(s.advance))
		fenceErr := s.e.inTx(s.fence)
		t.Run("contract: the fence fails and its dependent record is not written", func(t *testing.T) {
			requireLoser(t, fenceErr, forma.ErrPreconditionFailed)
			s.requireOutcome(t, fenceErr, nil)
		})
		t.Run("mechanism: the fence answers ErrPreconditionFailed", func(t *testing.T) {
			requirePostgresMechanisms(t)
			requireMechanismLoser(t, fenceErr, forma.ErrPreconditionFailed)
		})
	})

	t.Run("c: advance in flight", func(t *testing.T) {
		s := newFenceScenario(t)
		advancer := s.e.hold(t, s.advance)
		done := async(func() error { return s.e.inTx(s.fence) })
		answered, err := s.e.settle(t, done)
		advanceErr := advancer.commit()
		fencer := s.e.collect(t, done, answered, err)
		t.Run("contract: the fence and the advance do not both commit", func(t *testing.T) {
			require.False(t, fencer.err == nil && advanceErr == nil, "a fence at 7 committed beside the advance to 8")
			s.requireOutcome(t, fencer.err, advanceErr)
		})
		t.Run("mechanism: the fence blocks until the advance commits, then fails", func(t *testing.T) {
			requirePostgresMechanisms(t)
			require.False(t, fencer.early, "the fence waited on the update")
			require.NoError(t, advanceErr)
			requireMechanismLoser(t, fencer.err, forma.ErrPreconditionFailed)
		})
	})
}
