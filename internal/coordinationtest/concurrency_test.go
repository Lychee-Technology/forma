package coordinationtest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// #593's concurrency tests, through the adapter.

func TestConcurrency(t *testing.T) {
	t.Run("two writers using the same expected version: only one succeeds", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 7)
		const writers = 8
		errs := race(writers, func(i int) error {
			return e.inTx(func(uow UnitOfWork) error {
				return uow.Instances().Update(e.ctx, advanced(inst, fmt.Sprintf("writer-%d", i)), 7)
			})
		})
		winners := succeeded(errs)
		t.Run("contract: exactly one writer commits, and the row is its write", func(t *testing.T) {
			require.Len(t, winners, 1, "errors %v", errs)
			for i, err := range errs {
				if err != nil {
					requireLoser(t, err, forma.ErrPreconditionFailed)
				} else {
					require.Equal(t, map[string]any{"state": fmt.Sprintf("writer-%d", i), "version": int64(8)},
						e.stored(t, InstanceSchema, inst.ID, "state", "version"))
				}
			}
		})
		t.Run("mechanism: every loser answers ErrPreconditionFailed", func(t *testing.T) {
			requirePostgresMechanisms(t)
			for _, err := range errs {
				if err != nil {
					requireMechanismLoser(t, err, forma.ErrPreconditionFailed)
				}
			}
		})
	})

	t.Run("precondition failure is deterministic and distinguishable", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 8)
		entries := e.changeLogEntries(t, InstanceSchema, inst.ID)
		for attempt := 0; attempt < 3; attempt++ {
			err := e.inTx(func(uow UnitOfWork) error {
				return uow.Instances().Update(e.ctx, advanced(inst, "stale"), 7)
			})
			require.ErrorIs(t, err, forma.ErrPreconditionFailed, "attempt %d", attempt)
			require.NotErrorIs(t, err, forma.ErrRetryableConflict)
			require.NotErrorIs(t, err, forma.ErrNotFound)
		}
		missing := e.inTx(func(uow UnitOfWork) error {
			return uow.Instances().Update(e.ctx, &Instance{ID: uuid.Must(uuid.NewV7()), Version: 8}, 7)
		})
		require.ErrorIs(t, missing, forma.ErrNotFound, "a missing instance is not a precondition failure")
		require.NotErrorIs(t, missing, forma.ErrPreconditionFailed)
		require.Equal(t, map[string]any{"state": "start", "version": int64(8)}, e.stored(t, InstanceSchema, inst.ID, "state", "version"))
		require.Equal(t, entries, e.changeLogEntries(t, InstanceSchema, inst.ID), "a failed precondition writes no change_log entry")
	})

	t.Run("no lost update under concurrent conditional mutations", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		before := e.stamps(t, InstanceSchema, inst.ID)
		const writers = 32
		counts := make([]int64, writers)
		errs := race(writers, func(i int) error {
			return e.inTx(func(uow UnitOfWork) error {
				var err error
				counts[i], err = uow.Instances().RecordEvent(e.ctx, inst.ID)
				return err
			})
		})
		winners := succeeded(errs)
		t.Run("contract: the count is the number of commits, each returned count once", func(t *testing.T) {
			seen := map[int64]bool{}
			for _, i := range winners {
				require.False(t, seen[counts[i]], "count %d returned twice", counts[i])
				require.True(t, counts[i] >= 1 && counts[i] <= int64(len(winners)), "returned count %d", counts[i])
				seen[counts[i]] = true
			}
			for _, err := range errs {
				if err != nil {
					require.ErrorIs(t, err, forma.ErrRetryableConflict)
				}
			}
			require.Equal(t, map[string]any{"event_count": int64(len(winners))}, e.stored(t, InstanceSchema, inst.ID, "event_count"))
			e.requireWritten(t, InstanceSchema, before, inst.ID)
		})
		t.Run("mechanism: all 32 commit", func(t *testing.T) {
			requirePostgresMechanisms(t)
			require.Len(t, winners, writers, "errors %v", errs)
		})
	})
}

// succeeded answers the indexes of the nil errors.
func succeeded(errs []error) []int {
	var out []int
	for i, err := range errs {
		if err == nil {
			out = append(out, i)
		}
	}
	return out
}

// retryable counts the errors that are ErrRetryableConflict.
func retryable(errs []error) int {
	n := 0
	for _, err := range errs {
		if errors.Is(err, forma.ErrRetryableConflict) {
			n++
		}
	}
	return n
}
