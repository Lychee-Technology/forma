package internal

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// Predicate mutation inside a Tx (#652). A call validates the whole request
// before any statement, selects the candidates with Query's own SQL on the
// transaction, locks them in canonical order and re-checks the condition on
// the locked rows, then writes each surviving row through the single-row
// Update or Delete path, so an affected row is stored exactly as Tx.Update
// or Tx.Delete would store it.

type mutateKind int

const (
	mutateUpdate mutateKind = iota
	mutateDelete
)

func (k mutateKind) method() string {
	if k == mutateUpdate {
		return "UpdateWhere"
	}
	return "DeleteWhere"
}

// mutationPlan is a validated request, with everything its statements need
// resolved before the first one is sent.
type mutationPlan struct {
	kind        mutateKind
	req         *forma.MutateWhereRequest
	schemaID    int16
	tables      model.StorageTables
	orders      []model.AttributeOrder
	condition   *renderedGuard
	maxAffected int
}

// refusedBeforeWrite marks an ErrInvalidInput a predicate mutation answers
// after sending only reads: the MaxAffected overflow. Nothing was written,
// so it leaves the Tx usable although SQL was sent.
type refusedBeforeWrite struct{ error }

func (r refusedBeforeWrite) Unwrap() error { return r.error }

func (s *txSession) UpdateWhere(ctx context.Context, req *forma.MutateWhereRequest) (*forma.MutateWhereResult, error) {
	return s.mutateWhere(ctx, mutateUpdate, req)
}

func (s *txSession) DeleteWhere(ctx context.Context, req *forma.MutateWhereRequest) (*forma.MutateWhereResult, error) {
	return s.mutateWhere(ctx, mutateDelete, req)
}

func (s *txSession) mutateWhere(ctx context.Context, kind mutateKind, req *forma.MutateWhereRequest) (*forma.MutateWhereResult, error) {
	var result *forma.MutateWhereResult
	err := s.do(ctx, kind.method(), func(ctx context.Context) error {
		plan, err := s.planMutation(kind, req)
		if err != nil {
			return err
		}
		survivors, err := s.lockSurvivors(ctx, plan)
		if err != nil {
			return err
		}
		result, err = s.applyMutation(ctx, plan, survivors)
		return err
	})
	return result, err
}

// planMutation validates req without sending a statement: every refusal
// here is ErrInvalidInput and leaves the Tx usable.
func (s *txSession) planMutation(kind mutateKind, req *forma.MutateWhereRequest) (*mutationPlan, error) {
	method := kind.method()
	if req == nil {
		return nil, forma.InvalidInputf("%s request cannot be nil", method)
	}
	if req.SchemaName == "" {
		return nil, forma.InvalidInputf("%s needs a schema name", method)
	}
	schemaID, cache, err := s.crud.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if errors.Is(err, forma.ErrNotFound) {
		return nil, forma.InvalidInputf("%s names schema '%s', which is not registered", method, req.SchemaName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema %s: %w", req.SchemaName, err)
	}
	if req.Condition == nil {
		return nil, forma.InvalidInputf("%s on %s needs a condition: there is no unconditioned bulk mutation", method, req.SchemaName)
	}
	if err := validateGuardCondition("condition", req.Condition, req.SchemaName, cache); err != nil {
		return nil, fmt.Errorf("failed to validate %s condition on %s: %w", method, req.SchemaName, err)
	}
	if err := refuseEmptyGroups(method, req.SchemaName, req.Condition); err != nil {
		return nil, err
	}
	maxAffected, err := resolveMaxAffected(method, req)
	if err != nil {
		return nil, err
	}
	orders, err := buildAttributeOrders(&forma.QueryRequest{SchemaName: req.SchemaName, Sort: req.Sort}, cache)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %s sort on %s: %w", method, req.SchemaName, err)
	}
	if err := s.validateMutationValues(kind, req, schemaID, cache); err != nil {
		return nil, err
	}
	tables := s.crud.resolveTables()
	condition, err := s.rows.RenderCondition(tables, schemaID, model.RowGuard{Precondition: req.Condition})
	if err != nil {
		return nil, fmt.Errorf("failed to prepare %s on %s: %w", method, req.SchemaName, err)
	}
	return &mutationPlan{kind: kind, req: req, schemaID: schemaID, tables: tables, orders: orders,
		condition: condition, maxAffected: maxAffected}, nil
}

// refuseEmptyGroups refuses a condition holding an and or or with no
// conditions. An empty and renders as true, so and() alone, or or(kv, and()),
// would mutate every row: a filter list that turned out empty must not become
// an unconditioned bulk mutation. An empty or (false) is refused alongside it
// because it tests no attribute either. cond has passed
// validateGuardCondition, so every node is non-nil.
func refuseEmptyGroups(method, schemaName string, cond forma.Condition) error {
	group, ok := cond.(*forma.CompositeCondition)
	if !ok {
		return nil
	}
	if len(group.Conditions) == 0 {
		return forma.InvalidInputf("%s on %s has an empty %s group in its condition; an empty group tests no attribute, and there is no unconditioned bulk mutation",
			method, schemaName, group.Logic)
	}
	for _, child := range group.Conditions {
		if err := refuseEmptyGroups(method, schemaName, child); err != nil {
			return err
		}
	}
	return nil
}

// resolveMaxAffected checks Limit and MaxAffected and answers the effective
// MaxAffected.
func resolveMaxAffected(method string, req *forma.MutateWhereRequest) (int, error) {
	if req.Limit < 0 {
		return 0, forma.InvalidInputf("%s limit %d is negative: use 0 for every matching row", method, req.Limit)
	}
	maxAffected := req.MaxAffected
	if maxAffected == 0 {
		maxAffected = forma.DefaultMaxAffected
	}
	if maxAffected < 1 || maxAffected > forma.MaxMaxAffected {
		return 0, forma.InvalidInputf("%s MaxAffected %d is out of range: it must be 1 to %d, or 0 for the default of %d",
			method, req.MaxAffected, forma.MaxMaxAffected, forma.DefaultMaxAffected)
	}
	if req.Limit > maxAffected {
		return 0, forma.InvalidInputf("%s limit %d exceeds MaxAffected %d", method, req.Limit, maxAffected)
	}
	return maxAffected, nil
}

// validateMutationValues checks Updates and UpdateOps as Update checks them
// before its first statement (#649), and refuses both on a delete.
func (s *txSession) validateMutationValues(kind mutateKind, req *forma.MutateWhereRequest, schemaID int16, cache forma.SchemaAttributeCache) error {
	if kind == mutateDelete {
		if req.Updates != nil || len(req.UpdateOps) > 0 {
			return forma.InvalidInputf("DeleteWhere on %s does not accept Updates or UpdateOps: a delete writes no value", req.SchemaName)
		}
		return nil
	}
	if req.Updates == nil && len(req.UpdateOps) == 0 {
		return forma.InvalidInputf("UpdateWhere on %s needs Updates or UpdateOps", req.SchemaName)
	}
	op := &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: req.SchemaName},
		Updates: req.Updates, UpdateOps: req.UpdateOps}
	if err := validateConditionalUpdate(op, cache, s.crud.relations, s.crud.validator.ArrayPaths(schemaID)); err != nil {
		return fmt.Errorf("failed to validate UpdateWhere on %s: %w", req.SchemaName, err)
	}
	return nil
}

// lockSurvivors selects the candidates, refuses an overflow, locks the
// candidates and answers the ones the re-check keeps, charged to the
// operation limit.
func (s *txSession) lockSurvivors(ctx context.Context, plan *mutationPlan) ([]uuid.UUID, error) {
	req := plan.req
	fetch := req.Limit
	if fetch == 0 {
		fetch = plan.maxAffected
	}
	candidates, matched, err := s.rows.SelectCandidates(ctx, &model.PersistentRecordQuery{Tables: plan.tables,
		SchemaID: plan.schemaID, Condition: req.Condition, AttributeOrders: plan.orders, Limit: fetch})
	if err != nil {
		return nil, fmt.Errorf("failed to select %s candidates on %s: %w", plan.kind.method(), req.SchemaName, err)
	}
	if req.Limit == 0 && matched > int64(plan.maxAffected) {
		return nil, refusedBeforeWrite{forma.InvalidInputf(
			"%s on %s matches %d rows, more than MaxAffected %d: narrow the condition, set a Limit, or raise MaxAffected (at most %d)",
			plan.kind.method(), req.SchemaName, matched, plan.maxAffected, forma.MaxMaxAffected)}
	}
	if s.testHooks != nil && s.testHooks.afterCandidates != nil {
		s.testHooks.afterCandidates(candidates)
	}
	survivors, err := s.rows.LockAndRecheck(ctx, plan.tables, plan.schemaID, candidates, plan.condition)
	if err != nil {
		return nil, fmt.Errorf("failed to lock %s candidates on %s: %w", plan.kind.method(), req.SchemaName, err)
	}
	if err := s.chargeOperations(len(survivors) - 1); err != nil {
		return nil, err
	}
	return survivors, nil
}

// applyMutation writes each surviving row in candidate order. A failure
// after the first row was written poisons the Tx, whatever its class, so
// the Tx cannot commit part of the mutation.
func (s *txSession) applyMutation(ctx context.Context, plan *mutationPlan, survivors []uuid.UUID) (*forma.MutateWhereResult, error) {
	result := &forma.MutateWhereResult{RowIDs: make([]uuid.UUID, 0, len(survivors))}
	if plan.req.Returning {
		result.Records = make([]*forma.DataRecord, 0, len(survivors))
	}
	for i, rowID := range survivors {
		record, err := s.mutateRow(ctx, plan, i, rowID)
		if err != nil {
			err = fmt.Errorf("failed at row %d of %d (%s/%s) of %s: %w",
				i+1, len(survivors), plan.req.SchemaName, rowID, plan.kind.method(), err)
			if i > 0 {
				s.poison(err)
			}
			return nil, err
		}
		result.RowIDs = append(result.RowIDs, rowID)
		if plan.req.Returning {
			result.Records = append(result.Records, record)
		}
	}
	return result, nil
}

// mutateRow writes one surviving row through the CRUD service's own Update
// or Delete. It answers the row's record when the request asked for it.
func (s *txSession) mutateRow(ctx context.Context, plan *mutationPlan, i int, rowID uuid.UUID) (*forma.DataRecord, error) {
	if s.testHooks != nil && s.testHooks.beforeWrite != nil {
		if err := s.testHooks.beforeWrite(i, rowID); err != nil {
			return nil, err
		}
	}
	op := &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: plan.req.SchemaName, RowID: rowID}}
	if plan.kind == mutateUpdate {
		if plan.req.Updates != nil {
			op.Updates = copyMapDeep(plan.req.Updates)
		}
		op.UpdateOps = plan.req.UpdateOps
		return s.crud.Update(ctx, op)
	}
	var before *forma.DataRecord
	if plan.req.Returning {
		var err error
		if before, err = s.storedRecord(ctx, plan, rowID); err != nil {
			return nil, err
		}
	}
	if err := s.crud.Delete(ctx, op); err != nil {
		return nil, err
	}
	return before, nil
}

// storedRecord reads a locked row before its delete; see storedRow.
func (s *txSession) storedRecord(ctx context.Context, plan *mutationPlan, rowID uuid.UUID) (*forma.DataRecord, error) {
	record, err := s.storedRow(ctx, plan.tables, plan.req.SchemaName, plan.schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to read the row before deleting it: %w", err)
	}
	return record, nil
}

// storedRow reads a locked row as Update answers a row: the storage
// round-trip, without relation enrichment.
func (s *txSession) storedRow(ctx context.Context, tables model.StorageTables, schemaName string, schemaID int16, rowID uuid.UUID) (*forma.DataRecord, error) {
	stored, err := s.rows.GetPersistentRecord(ctx, tables, schemaID, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s/%s: %w", schemaName, rowID, err)
	}
	if stored == nil {
		return nil, fmt.Errorf("locked row %s/%s is missing", schemaName, rowID)
	}
	attributes, err := s.crud.transformer.FromPersistentRecord(ctx, stored)
	if err != nil {
		return nil, fmt.Errorf("failed to transform %s/%s: %w", schemaName, rowID, err)
	}
	return &forma.DataRecord{SchemaName: schemaName, RowID: rowID, Attributes: attributes}, nil
}
