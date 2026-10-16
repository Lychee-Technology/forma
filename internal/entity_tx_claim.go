package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// Claim inside a Tx (#653). A call validates the whole request before any
// statement, selects the candidates with Query's own SQL on the transaction,
// then takes them in selection order without waiting: a candidate another
// transaction holds is skipped, and a taken one is re-checked against the
// condition, and with a Group against the head predicate, under its row
// lock. Each claimed row is written through the
// single-row Update path, so a claimed row is stored exactly as Tx.Update
// would store it. The call then checks that the mutation falsified the
// condition on every claimed row.

// claimOverfetch is how many candidates a claim selects per row it may
// claim: rows other claimers hold are skipped, so a claim considers more
// rows than it takes.
const claimOverfetch = 2

// claimPlan is a validated claim, with everything its statements need
// resolved before the first one is sent.
type claimPlan struct {
	req       *forma.ClaimRequest
	schemaID  int16
	tables    model.StorageTables
	orders    []model.AttributeOrder
	head      *model.GroupHead
	condition *renderedGuard
	// eligible is what the re-check tests: condition, and with a Group the
	// head predicate too.
	eligible *renderedGuard
}

func (s *txSession) Claim(ctx context.Context, req *forma.ClaimRequest) (*forma.ClaimResult, error) {
	var result *forma.ClaimResult
	err := s.do(ctx, "Claim", func(ctx context.Context) error {
		plan, err := s.planClaim(req)
		if err != nil {
			return err
		}
		claimed, err := s.takeClaimable(ctx, plan)
		if err != nil {
			return err
		}
		result, err = s.applyClaim(ctx, plan, claimed)
		return err
	})
	return result, err
}

// planClaim validates req without sending a statement: every refusal here is
// ErrInvalidInput and leaves the Tx usable.
func (s *txSession) planClaim(req *forma.ClaimRequest) (*claimPlan, error) {
	if req == nil {
		return nil, forma.InvalidInputf("Claim request cannot be nil")
	}
	if req.SchemaName == "" {
		return nil, forma.InvalidInputf("Claim needs a schema name")
	}
	schemaID, cache, err := s.crud.registry.GetSchemaAttributeCacheByName(req.SchemaName)
	if errors.Is(err, forma.ErrNotFound) {
		return nil, forma.InvalidInputf("Claim names schema '%s', which is not registered", req.SchemaName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema %s: %w", req.SchemaName, err)
	}
	if req.Condition == nil {
		return nil, forma.InvalidInputf("Claim on %s needs a condition: it says which rows are claimable", req.SchemaName)
	}
	if err := validateGuardCondition("condition", req.Condition, req.SchemaName, cache); err != nil {
		return nil, fmt.Errorf("failed to validate Claim condition on %s: %w", req.SchemaName, err)
	}
	if err := refuseEmptyGroups("Claim", req.SchemaName, req.Condition); err != nil {
		return nil, fmt.Errorf("failed to validate Claim condition on %s: %w", req.SchemaName, err)
	}
	if req.Limit < 1 || req.Limit > forma.MaxClaimLimit {
		return nil, forma.InvalidInputf("Claim limit %d is out of range: it must be 1 to %d", req.Limit, forma.MaxClaimLimit)
	}
	orders, err := buildAttributeOrders(&forma.QueryRequest{SchemaName: req.SchemaName, Sort: req.Sort}, cache)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Claim sort on %s: %w", req.SchemaName, err)
	}
	head, err := planClaimGroup(req, cache)
	if err != nil {
		return nil, fmt.Errorf("failed to validate Claim group on %s: %w", req.SchemaName, err)
	}
	if err := s.validateClaimMutation(req, schemaID, cache); err != nil {
		return nil, fmt.Errorf("failed to validate Claim mutation on %s: %w", req.SchemaName, err)
	}
	plan := &claimPlan{req: req, schemaID: schemaID, tables: s.crud.resolveTables(), orders: orders, head: head}
	if err := s.renderClaim(plan); err != nil {
		return nil, fmt.Errorf("failed to prepare Claim on %s: %w", req.SchemaName, err)
	}
	return plan, nil
}

// renderClaim renders the plan's condition, for the rule-8 check, and what
// the re-check tests, so that a malformed operand fails before any SQL.
func (s *txSession) renderClaim(plan *claimPlan) error {
	condition, err := s.rows.RenderCondition(plan.tables, plan.schemaID, model.RowGuard{Precondition: plan.req.Condition})
	if err != nil {
		return forma.WrapPublicf(err, "Claim condition on %s", plan.req.SchemaName)
	}
	plan.condition, plan.eligible = condition, condition
	if plan.head == nil {
		return nil
	}
	// The condition rendered above, so a failure here is the live condition's.
	plan.eligible, err = s.rows.RenderClaimEligibility(plan.tables, plan.schemaID, plan.req.Condition, plan.head)
	if err != nil {
		return forma.WrapPublicf(err, "Claim group live condition on %s", plan.req.SchemaName)
	}
	return nil
}

// validateClaimMutation checks Updates and UpdateOps as Update checks them
// before its first statement (#649), and refuses a mutation that changes an
// attribute of a unique key: moving a key entry can wait for another
// transaction writing the same value, and a claim never waits.
func (s *txSession) validateClaimMutation(req *forma.ClaimRequest, schemaID int16, cache forma.SchemaAttributeCache) error {
	if len(req.Updates) == 0 && len(req.UpdateOps) == 0 {
		return forma.InvalidInputf("Claim on %s needs Updates or UpdateOps: the claim mutation must make a claimed row stop matching the claim condition", req.SchemaName)
	}
	op := &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: req.SchemaName},
		Updates: req.Updates, UpdateOps: req.UpdateOps}
	if err := validateConditionalUpdate(op, cache, s.crud.relations, s.crud.validator.ArrayPaths(schemaID)); err != nil {
		return fmt.Errorf("failed to validate Claim mutation on %s: %w", req.SchemaName, err)
	}
	for _, spec := range s.rows.repo.uniqueKeys.keysOf(schemaID) {
		for _, attr := range spec.attrs {
			if claimWrites(req, attr.name) {
				return forma.InvalidInputf("Claim on %s cannot change attribute '%s' of unique key %s: a claim never waits for another transaction, and a unique key write can",
					req.SchemaName, attr.name, spec.name)
			}
		}
	}
	return nil
}

// claimWrites reports whether the claim mutation writes attr, a dotted
// attribute path: an Updates entry writes every attribute under its key,
// and an operator writes the attribute it names.
func claimWrites(req *forma.ClaimRequest, attr string) bool {
	for key := range req.Updates {
		if attr == key || strings.HasPrefix(attr, key+".") {
			return true
		}
	}
	for _, op := range req.UpdateOps {
		if op.Attribute == attr {
			return true
		}
	}
	return false
}

// candidateQuery is the candidate select: Query's SQL for the condition,
// restricted to group heads with a Group, over-fetching claimOverfetch
// candidates per row the claim may take.
func (plan *claimPlan) candidateQuery() *model.PersistentRecordQuery {
	return &model.PersistentRecordQuery{Tables: plan.tables, SchemaID: plan.schemaID, Condition: plan.req.Condition,
		AttributeOrders: plan.orders, Head: plan.head, Limit: claimOverfetch * plan.req.Limit}
}

// takeClaimable selects the candidates and takes, in selection order, up to
// Limit of them that no other transaction holds and that still match once
// held. A candidate held elsewhere is skipped and one that stopped matching
// is dropped; either way the next candidate is tried in its place. Each
// round tries only as many candidates as rows are still wanted, so the call
// never holds more claimable rows than it claims.
func (s *txSession) takeClaimable(ctx context.Context, plan *claimPlan) ([]uuid.UUID, error) {
	req := plan.req
	candidates, _, err := s.rows.SelectCandidates(ctx, plan.candidateQuery())
	if err != nil {
		return nil, fmt.Errorf("failed to select Claim candidates on %s: %w", req.SchemaName, err)
	}
	if s.testHooks != nil && s.testHooks.afterCandidates != nil {
		s.testHooks.afterCandidates(candidates)
	}
	claimed := make([]uuid.UUID, 0, min(req.Limit, len(candidates)))
	for len(candidates) > 0 && len(claimed) < req.Limit {
		round := candidates[:min(req.Limit-len(claimed), len(candidates))]
		candidates = candidates[len(round):]
		taken, err := s.rows.TryLockRows(ctx, plan.tables, plan.schemaID, round)
		if err != nil {
			return nil, fmt.Errorf("failed to take Claim candidates on %s: %w", req.SchemaName, err)
		}
		held, err := s.rows.RecheckHeld(ctx, plan.tables, plan.schemaID, taken, plan.eligible)
		if err != nil {
			return nil, fmt.Errorf("failed to re-check Claim candidates on %s: %w", req.SchemaName, err)
		}
		claimed = append(claimed, held...)
	}
	return claimed, nil
}

// applyClaim charges the claimed rows to the operation limit, writes each in
// selection order, then checks that the claim mutation falsified the
// condition on every one of them. Every failure here poisons the Tx: the
// rows are held and possibly written, so it cannot commit part of a claim.
func (s *txSession) applyClaim(ctx context.Context, plan *claimPlan, claimed []uuid.UUID) (*forma.ClaimResult, error) {
	result := &forma.ClaimResult{Records: make([]*forma.DataRecord, 0, len(claimed))}
	if len(claimed) == 0 {
		return result, nil
	}
	if err := s.chargeOperations(len(claimed) - 1); err != nil {
		return nil, err
	}
	for i, rowID := range claimed {
		record, err := s.claimRow(ctx, plan, i, rowID)
		if err != nil {
			err = fmt.Errorf("failed at row %d of %d (%s/%s) of Claim: %w", i+1, len(claimed), plan.req.SchemaName, rowID, err)
			s.poison(err)
			return nil, err
		}
		result.Records = append(result.Records, record)
	}
	if err := s.requireClaimFalsified(ctx, plan, claimed); err != nil {
		s.poison(err)
		return nil, err
	}
	return result, nil
}

// claimRow writes one claimed row through the CRUD service's own Update and
// answers the row as stored.
func (s *txSession) claimRow(ctx context.Context, plan *claimPlan, i int, rowID uuid.UUID) (*forma.DataRecord, error) {
	if s.testHooks != nil && s.testHooks.beforeWrite != nil {
		if err := s.testHooks.beforeWrite(i, rowID); err != nil {
			return nil, err
		}
	}
	op := &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: plan.req.SchemaName, RowID: rowID},
		UpdateOps: plan.req.UpdateOps}
	if plan.req.Updates != nil {
		op.Updates = copyMapDeep(plan.req.Updates)
	}
	return s.crud.Update(ctx, op)
}

// requireClaimFalsified refuses a claim whose mutation left a claimed row
// matching the condition (#653 rule 8): such a row could be claimed again at
// once by the next claimer, so a committed claim would not be exclusive.
func (s *txSession) requireClaimFalsified(ctx context.Context, plan *claimPlan, claimed []uuid.UUID) error {
	still, err := s.rows.StillMatching(ctx, plan.tables, plan.schemaID, claimed, plan.condition)
	if err != nil {
		return fmt.Errorf("failed to check that the Claim on %s falsified its condition: %w", plan.req.SchemaName, err)
	}
	if len(still) == 0 {
		return nil
	}
	return forma.WithOperatorDetail(
		forma.InvalidInputf("the claim mutation must make a claimed row stop matching the claim condition: %d of the %d rows claimed on %s still match it",
			len(still), len(claimed), plan.req.SchemaName),
		fmt.Errorf("rows still matching: %v", still))
}
