package internal

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/lychee-technology/forma"
)

// Validation of a claim's Group (#653). The head predicate compares rows of
// one schema with each other in a correlated subquery on entity_main, so
// every attribute of the key, the order and the live condition must be
// column-bound. Every refusal names the attribute or the part at fault and
// is ErrInvalidInput sent before any SQL.

// planClaimGroup validates req.Group and resolves it to the columns the head
// predicate compares. Without a Group it answers nil.
func planClaimGroup(req *forma.ClaimRequest, cache forma.SchemaAttributeCache) (*model.GroupHead, error) {
	group := req.Group
	if group == nil {
		return nil, nil
	}
	keys, err := claimGroupKeyColumns(req.SchemaName, group.Key, cache)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Claim group key on %s: %w", req.SchemaName, err)
	}
	if len(group.Order) == 0 {
		return nil, forma.InvalidInputf("Claim group on %s needs an order: it says which row of a group is its head", req.SchemaName)
	}
	orders, err := buildAttributeOrders(&forma.QueryRequest{SchemaName: req.SchemaName, Sort: group.Order}, cache)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Claim group order on %s: %w", req.SchemaName, err)
	}
	for _, order := range orders {
		if order.StorageLocation != forma.AttributeStorageLocationMain {
			return nil, claimGroupNotColumnBound("order", order.AttrName, req.SchemaName)
		}
		if err := refuseMovedByTheClaim(req, "order", order.AttrName, cache); err != nil {
			return nil, fmt.Errorf("failed to validate Claim group order on %s: %w", req.SchemaName, err)
		}
	}
	for _, attr := range group.Key {
		if err := refuseMovedByTheClaim(req, "key", attr, cache); err != nil {
			return nil, fmt.Errorf("failed to validate Claim group key on %s: %w", req.SchemaName, err)
		}
	}
	if group.Live == nil {
		return nil, forma.InvalidInputf("Claim group on %s needs a live condition: it says which rows still block later rows of their group", req.SchemaName)
	}
	if err := validateGuardCondition("group live condition", group.Live, req.SchemaName, cache); err != nil {
		return nil, fmt.Errorf("failed to validate Claim group live condition on %s: %w", req.SchemaName, err)
	}
	if err := refuseEmptyLiveGroups(req.SchemaName, group.Live); err != nil {
		return nil, fmt.Errorf("failed to validate Claim group live condition on %s: %w", req.SchemaName, err)
	}
	if attr, ok := firstEAVLeaf(group.Live, cache); ok {
		return nil, claimGroupNotColumnBound("live condition", attr, req.SchemaName)
	}
	return &model.GroupHead{KeyColumns: keys, Orders: orders, Live: group.Live}, nil
}

// claimGroupKeyColumns checks a group key and answers its columns.
func claimGroupKeyColumns(schemaName string, key []string, cache forma.SchemaAttributeCache) ([]string, error) {
	if len(key) < 1 || len(key) > forma.MaxClaimGroupKey {
		return nil, forma.InvalidInputf("Claim group on %s has %d key attributes: it must have 1 to %d", schemaName, len(key), forma.MaxClaimGroupKey)
	}
	columns := make([]string, len(key))
	seen := make(map[string]bool, len(key))
	for i, attr := range key {
		meta, ok := cache[attr]
		if !ok {
			return nil, forma.InvalidInputf("Claim group key names attribute '%s', which schema '%s' does not define", attr, schemaName)
		}
		if meta.ColumnBinding == nil {
			return nil, claimGroupNotColumnBound("key", attr, schemaName)
		}
		if seen[attr] {
			return nil, forma.InvalidInputf("Claim group key on %s lists attribute '%s' twice", schemaName, attr)
		}
		seen[attr] = true
		columns[i] = string(meta.ColumnBinding.ColumnName)
	}
	return columns, nil
}

// refuseMovedByTheClaim refuses a group key or order attribute whose value
// the claim's own write changes: one bound to a column Forma stamps on every
// update, or one the claim mutation writes. Either moves a claimed row away
// from its place in its group while it is still live, so another row of
// the group would become a second head.
func refuseMovedByTheClaim(req *forma.ClaimRequest, part, attr string, cache forma.SchemaAttributeCache) error {
	if binding := cache[attr].ColumnBinding; binding != nil && schemameta.IsServerMaintained(binding.ColumnName) {
		return forma.InvalidInputf("Claim group %s uses attribute '%s' of schema '%s', which is bound to %s: Forma rewrites it on the claim's own write, which would move a claimed row within its group",
			part, attr, req.SchemaName, binding.ColumnName)
	}
	if claimWrites(req, attr) {
		return forma.InvalidInputf("Claim on %s writes attribute '%s' of its group %s: the claim's write would move a claimed row within its group",
			req.SchemaName, attr, part)
	}
	return nil
}

func claimGroupNotColumnBound(part, attr, schemaName string) error {
	return forma.InvalidInputf("Claim group %s uses attribute '%s' of schema '%s', which is stored in EAV: a group's key, order and live condition must be column-bound",
		part, attr, schemaName)
}

// refuseEmptyLiveGroups refuses an empty and or or anywhere in a live
// condition, as a claim condition refuses one: an empty group tests no
// attribute.
func refuseEmptyLiveGroups(schemaName string, cond forma.Condition) error {
	group, ok := cond.(*forma.CompositeCondition)
	if !ok {
		return nil
	}
	if len(group.Conditions) == 0 {
		return forma.InvalidInputf("Claim group on %s has an empty %s group in its live condition; an empty group tests no attribute", schemaName, group.Logic)
	}
	for _, child := range group.Conditions {
		if err := refuseEmptyLiveGroups(schemaName, child); err != nil {
			return err
		}
	}
	return nil
}

// firstEAVLeaf answers the first attribute cond tests that is stored in EAV.
// cond has passed validateGuardCondition, so every attribute it names is in
// cache.
func firstEAVLeaf(cond forma.Condition, cache forma.SchemaAttributeCache) (string, bool) {
	switch c := cond.(type) {
	case *forma.KvCondition:
		if cache[c.Attr].ColumnBinding == nil {
			return c.Attr, true
		}
	case *forma.CompositeCondition:
		for _, child := range c.Conditions {
			if attr, ok := firstEAVLeaf(child, cache); ok {
				return attr, true
			}
		}
	}
	return "", false
}
