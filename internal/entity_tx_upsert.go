package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// Tx.Upsert (#651 part C): a create that resolves a conflict on a named
// target instead of failing.
//
// The target's holder is looked up and locked, as AssertRow locks a row,
// before the create is tried; with a holder the create never runs. A create
// that races another creator of the target value waits for it on the key
// table, finds its entry once it commits, and refuses with ErrUniqueConflict
// without a failed statement; the next round then finds and locks the
// winner. So every outcome but an unexpected error leaves the Tx usable.

// maxUpsertAttempts bounds the rounds of a key target. A round repeats only
// when another transaction took the value, or released it, between this
// round's lookup and its lock or create. Running out of rounds is
// ErrRetryableConflict: nothing was written, and a re-run of the Tx can
// settle.
const maxUpsertAttempts = 4

// upsertPlan is a validated UpsertRequest with its create prepared.
type upsertPlan struct {
	schemaName string
	create     *preparedCreate
	// createdAt is the repository clock read the create is stamped with in
	// every round, so a key over the creation stamp is looked up under the
	// value the create would reserve.
	createdAt int64
	// key is the target key, nil for a row-ID target, and value its value
	// in the create.
	key   *uniqueKeySpec
	value string
	// update is the update action's operation without its row ID, nil for
	// do_nothing.
	update *forma.EntityOperation
}

func (s *txSession) Upsert(ctx context.Context, req *forma.UpsertRequest) (*forma.UpsertResult, error) {
	var result *forma.UpsertResult
	err := s.do(ctx, "Upsert", func(ctx context.Context) error {
		plan, err := s.planUpsert(ctx, req)
		if err != nil {
			return err
		}
		if plan.key == nil {
			result, err = s.upsertOnRowID(ctx, plan)
		} else {
			result, err = s.upsertOnKey(ctx, plan)
		}
		return err
	})
	return result, err
}

// upsertOnRowID locks the create's row ID first. A live row is the
// conflict; a missing one keeps the per-row lock, so no other writer can
// create the row before this create does.
func (s *txSession) upsertOnRowID(ctx context.Context, plan *upsertPlan) (*forma.UpsertResult, error) {
	rowID := plan.create.record.RowID
	err := s.lockRow(ctx, plan.schemaName, rowID, nil)
	switch {
	case err == nil:
		return s.resolveUpsert(ctx, plan, rowID)
	case errors.Is(err, forma.ErrNotFound):
		return s.createUpserted(ctx, plan)
	default:
		return nil, fmt.Errorf("failed to lock the upsert's target row: %w", err)
	}
}

// upsertOnKey resolves against the target value's holder when there is one,
// and creates otherwise. A create refused for any unique conflict is retried
// while the target is held by then, since the target's conflict decides the
// outcome; a conflict on another key, with the target free, is the answer.
func (s *txSession) upsertOnKey(ctx context.Context, plan *upsertPlan) (*forma.UpsertResult, error) {
	var conflict error
	for attempt := 0; attempt < maxUpsertAttempts; attempt++ {
		holder, found, err := s.targetHolder(ctx, plan)
		if err != nil {
			return nil, err
		}
		if s.testHooks != nil && s.testHooks.afterTargetLookup != nil {
			s.testHooks.afterTargetLookup(holder, found)
		}
		if found {
			held, err := s.lockTargetHolder(ctx, plan, holder)
			if err != nil {
				return nil, err
			}
			if held {
				return s.resolveUpsert(ctx, plan, holder)
			}
			continue
		}
		if conflict != nil && !conflictsOnKey(conflict, plan.create.schemaID, plan.key.id) {
			return nil, conflict
		}
		result, err := s.createUpserted(ctx, plan)
		if !errors.Is(err, forma.ErrUniqueConflict) {
			return result, err
		}
		conflict = err
	}
	return nil, &storageOutcome{sentinel: forma.ErrRetryableConflict, cause: fmt.Errorf(
		"upsert of %s on unique key %s did not settle after %d attempts: other transactions kept taking and releasing its value",
		plan.schemaName, plan.key.name, maxUpsertAttempts)}
}

// targetHolder answers the row that holds the target value, as the
// transaction sees the key table: committed entries and its own.
func (s *txSession) targetHolder(ctx context.Context, plan *upsertPlan) (uuid.UUID, bool, error) {
	holder, found, err := s.rows.uniqueKeyOwner(ctx, s.crud.resolveTables(), plan.create.schemaID, plan.key.id, plan.value)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to look up the holder of unique key %s: %w", plan.key.name, err)
	}
	return holder, found, nil
}

// lockTargetHolder locks holder's row and says whether it still holds the
// target value. Only a write of the row changes its entries, so once the
// lock is held the answer stays true until the Tx ends. A row that was
// deleted or moved its value away before the lock was granted keeps the
// lock until the Tx ends too: a transaction-scoped lock is not released
// early.
func (s *txSession) lockTargetHolder(ctx context.Context, plan *upsertPlan, holder uuid.UUID) (bool, error) {
	err := s.lockRow(ctx, plan.schemaName, holder, nil)
	if errors.Is(err, forma.ErrNotFound) {
		return false, nil // deleted since the lookup
	}
	if err != nil {
		return false, fmt.Errorf("failed to lock the holder of unique key %s: %w", plan.key.name, err)
	}
	current, found, err := s.targetHolder(ctx, plan)
	if err != nil {
		return false, err
	}
	return found && current == holder, nil
}

// createUpserted runs the planned create under the plan's creation stamp.
func (s *txSession) createUpserted(ctx context.Context, plan *upsertPlan) (*forma.UpsertResult, error) {
	insert := func(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
		return s.rows.insertPersistentRecordAt(ctx, tables, record, plan.createdAt)
	}
	record, err := s.crud.insertPrepared(ctx, plan.create, insert)
	if err != nil {
		return nil, err
	}
	return &forma.UpsertResult{Outcome: forma.UpsertCreated, Record: record}, nil
}

// resolveUpsert applies the action to the locked row that holds the target.
// Every outcome answers the storage round-trip, as Update does, without
// relation enrichment.
func (s *txSession) resolveUpsert(ctx context.Context, plan *upsertPlan, holder uuid.UUID) (*forma.UpsertResult, error) {
	if plan.update == nil {
		record, err := s.storedRow(ctx, s.crud.resolveTables(), plan.schemaName, plan.create.schemaID, holder)
		if err != nil {
			return nil, fmt.Errorf("failed to read the existing row: %w", err)
		}
		return &forma.UpsertResult{Outcome: forma.UpsertExisting, Record: record}, nil
	}
	op := *plan.update
	op.RowID = holder
	record, err := s.crud.Update(ctx, &op)
	if err != nil {
		return nil, fmt.Errorf("failed to update the existing row %s: %w", holder, err)
	}
	return &forma.UpsertResult{Outcome: forma.UpsertUpdated, Record: record}, nil
}
