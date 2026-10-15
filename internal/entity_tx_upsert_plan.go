package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// planUpsert validates req and prepares its create before any SQL, so every
// refusal of the request itself is an ErrInvalidInput that leaves the Tx
// usable.
func (s *txSession) planUpsert(ctx context.Context, req *forma.UpsertRequest) (*upsertPlan, error) {
	if req == nil {
		return nil, forma.InvalidInputf("upsert request cannot be nil")
	}
	create := req.Create
	if create.Type != "" && create.Type != forma.OperationCreate {
		return nil, forma.InvalidInputf("upsert of %s creates, so Create.Type must be empty or %q, got %q",
			create.SchemaName, forma.OperationCreate, create.Type)
	}
	if err := validateUpsertTarget(req); err != nil {
		return nil, err
	}
	update, err := upsertUpdateOp(req)
	if err != nil {
		return nil, err
	}
	prepared, err := s.crud.prepareCreate(ctx, &create)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the create of the upsert: %w", err)
	}
	plan := &upsertPlan{schemaName: create.SchemaName, create: prepared, update: update}
	if update != nil {
		if err := s.validateUpsertUpdate(update, prepared); err != nil {
			return nil, err
		}
	}
	if req.Target.Key != "" {
		if err := s.planKeyTarget(plan, req.Target.Key); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// validateUpsertUpdate checks the update action as Update checks it before
// its first statement (#649): its operators, and its preconditions down to
// their operands, which only rendering parses. The holder is locked before
// the update runs, so a malformed operand found there would arrive after
// SQL and poison the Tx, and only when a row held the target.
func (s *txSession) validateUpsertUpdate(update *forma.EntityOperation, prepared *preparedCreate) error {
	if err := validateConditionalUpdate(update, prepared.cache, s.crud.relations, s.crud.validator.ArrayPaths(prepared.schemaID)); err != nil {
		return fmt.Errorf("failed to validate the update action of the upsert of %s: %w", update.SchemaName, err)
	}
	if update.Preconditions == nil {
		return nil
	}
	if _, err := s.rows.RenderCondition(s.crud.resolveTables(), prepared.schemaID, model.RowGuard{Precondition: update.Preconditions}); err != nil {
		return fmt.Errorf("failed to prepare the preconditions of the upsert of %s: %w", update.SchemaName, err)
	}
	return nil
}

// validateUpsertTarget refuses a request that does not name exactly one
// conflict target, or targets a row ID it does not set.
func validateUpsertTarget(req *forma.UpsertRequest) error {
	schema, target := req.Create.SchemaName, req.Target
	switch {
	case target.RowID && target.Key != "":
		return forma.InvalidInputf("upsert of %s names two conflict targets, the row ID and unique key %s; set exactly one", schema, target.Key)
	case !target.RowID && target.Key == "":
		return forma.InvalidInputf("upsert of %s needs a conflict target: set Target.RowID or Target.Key", schema)
	case target.RowID && req.Create.RowID == uuid.Nil:
		return forma.InvalidInputf("upsert of %s targets the row ID, so Create.RowID must be set", schema)
	}
	return nil
}

// upsertUpdateOp checks the action and answers the update it applies to the
// existing row, nil for do_nothing.
func upsertUpdateOp(req *forma.UpsertRequest) (*forma.EntityOperation, error) {
	schema := req.Create.SchemaName
	switch req.Action {
	case forma.UpsertDoNothing:
		if req.Preconditions != nil || req.Updates != nil || len(req.UpdateOps) > 0 {
			return nil, forma.InvalidInputf("upsert of %s with action %s takes no Preconditions, Updates or UpdateOps; they apply only with action %s",
				schema, forma.UpsertDoNothing, forma.UpsertUpdate)
		}
		return nil, nil
	case forma.UpsertUpdate:
		if req.Updates == nil && len(req.UpdateOps) == 0 {
			return nil, forma.InvalidInputf("upsert of %s with action %s needs Updates or UpdateOps", schema, forma.UpsertUpdate)
		}
		return &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: schema},
			Type:             forma.OperationUpdate,
			Updates:          req.Updates,
			UpdateOps:        req.UpdateOps,
			Preconditions:    req.Preconditions,
		}, nil
	default:
		return nil, forma.InvalidInputf("upsert of %s: Action must be %q or %q, got %q",
			schema, forma.UpsertDoNothing, forma.UpsertUpdate, req.Action)
	}
}

// planKeyTarget resolves the target key and its value in the create, which
// must set every attribute of the key: a create without one is not subject
// to the key, so it could never conflict on it.
func (s *txSession) planKeyTarget(plan *upsertPlan, keyName string) error {
	spec, ok := s.rows.repo.uniqueKeys.keyNamed(plan.create.schemaID, keyName)
	if !ok {
		return forma.InvalidInputf("upsert of %s targets unique key %s, which the schema does not declare", plan.schemaName, keyName)
	}
	value, present, err := uniqueKeyValueOf(spec, plan.create.record)
	if err != nil {
		return fmt.Errorf("failed to compute unique key %s of the upsert's create: %w", keyName, err)
	}
	if !present {
		names := make([]string, len(spec.attrs))
		for i, attr := range spec.attrs {
			names[i] = attr.name
		}
		return forma.InvalidInputf("upsert of %s targets unique key %s, so Create.Data must set every attribute of it (%s)",
			plan.schemaName, keyName, strings.Join(names, ", "))
	}
	plan.key, plan.value = &spec, value
	return nil
}
