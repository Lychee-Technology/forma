package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// sortKey is one normalized (attribute, direction) pair extracted from the
// request's sort surface — either the structured Sort field or legacy
// SortBy + shared SortOrder.
type sortKey struct {
	attr  string
	order forma.SortOrder
}

// resolveSortKeys normalizes the request's two sort surfaces into ordered
// sortKey pairs. The structured Sort field carries per-key directions (#240);
// the legacy SortBy list shares the single SortOrder, normalized by the same
// rules as a Sort entry's direction (#296). The two surfaces are mutually
// exclusive so a caller bug cannot be silently half-applied.
func resolveSortKeys(req *forma.QueryRequest) ([]sortKey, error) {
	if len(req.Sort) == 0 {
		// The legacy surface shares one direction across all SortBy keys. It goes
		// through the same normalizeSortOrder as structured Sort entries (#296):
		// non-empty garbage is invalid input even when SortBy is empty, because a
		// direction that cannot apply to anything is still a caller bug.
		sortOrder, err := normalizeSortOrder(req.SortOrder)
		if err != nil {
			// A plain wrap on purpose, not WrapPublicf: this prefix is operator
			// context (there is no per-entry attribute to name), and the published
			// body must stay exactly the normalizeSortOrder message.
			return nil, fmt.Errorf("failed to normalize legacy sort_order: %w", err)
		}
		keys := make([]sortKey, 0, len(req.SortBy))
		for _, attr := range req.SortBy {
			keys = append(keys, sortKey{attr: attr, order: sortOrder})
		}
		return keys, nil
	}

	if len(req.SortBy) > 0 || req.SortOrder != "" {
		return nil, forma.InvalidInputf(
			"sort cannot be combined with sort_by/sort_order: use sort alone for per-key directions")
	}

	keys := make([]sortKey, 0, len(req.Sort))
	for i, entry := range req.Sort {
		attr := strings.TrimSpace(entry.Attribute)
		if attr == "" {
			return nil, forma.InvalidInputf("sort entry %d has an empty attribute", i)
		}
		order, err := normalizeSortOrder(entry.SortOrder)
		if err != nil {
			// The attribute name identifies which entry is wrong — caller-actionable,
			// so the prefix belongs in the published message too.
			return nil, forma.WrapPublicf(err, "sort entry for attribute '%s'", attr)
		}
		keys = append(keys, sortKey{attr: attr, order: order})
	}
	return keys, nil
}

// normalizeSortOrder folds a Sort entry's direction case-insensitively:
// empty defaults to asc, anything other than asc/desc is invalid input.
func normalizeSortOrder(raw forma.SortOrder) (forma.SortOrder, error) {
	switch forma.SortOrder(strings.ToLower(string(raw))) {
	case "", forma.SortOrderAsc:
		return forma.SortOrderAsc, nil
	case forma.SortOrderDesc:
		return forma.SortOrderDesc, nil
	default:
		return "", forma.InvalidInputf("invalid sort_order '%s': expected 'asc' or 'desc'", raw)
	}
}

// buildAttributeOrders resolves the request's sort keys against the schema
// cache into typed AttributeOrders, tagging each with its storage location
// (main column vs EAV) and its own direction. It errors on an unknown sort
// attribute, and on an attribute bound to a column entity_main does not have.
func buildAttributeOrders(req *forma.QueryRequest, schemaCache forma.SchemaAttributeCache) ([]model.AttributeOrder, error) {
	keys, err := resolveSortKeys(req)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve sort keys for schema '%s': %w", req.SchemaName, err)
	}

	orders := make([]model.AttributeOrder, 0, len(keys))
	for _, key := range keys {
		meta, ok := schemaCache[key.attr]
		if !ok {
			// A client error (#296): naming an attribute the schema does not define
			// is caller fault, and the HTTP boundary classifies on sentinel evidence
			// alone (#301). The carrier's published message is the whole body text
			// the caller sees (#313), so it must name the attribute and the schema.
			return nil, forma.InvalidInputf("cannot sort by unknown attribute '%s' in schema '%s'",
				key.attr, req.SchemaName)
		}
		order := model.AttributeOrder{
			AttrID:    meta.AttributeID,
			ValueType: meta.ValueType,
			SortOrder: key.order,
			AttrName:  key.attr,
		}
		if meta.ColumnBinding != nil {
			column := string(meta.ColumnBinding.ColumnName)
			if !model.IsMainTableColumn(column) {
				// Metadata drift, not caller fault: registration refuses such a
				// binding (#557), so a cache holding one is an operator-visible
				// read-path failure. Checked here so the sort emitter, like the
				// write path's allowlist, never renders an unknown column (#472).
				return nil, fmt.Errorf("cannot sort by attribute '%s' in schema '%s': it is bound to column '%s', which entity_main does not have",
					key.attr, req.SchemaName, column)
			}
			order.StorageLocation = forma.AttributeStorageLocationMain
			order.ColumnName = column
		} else {
			order.StorageLocation = forma.AttributeStorageLocationEAV
		}
		orders = append(orders, order)
	}
	return orders, nil
}

// validateFederatedSort refuses a sort on a list attribute on a federated
// request (#853). DuckDB orders a LIST element by element, and the hot
// leg's Go comparator, which must reproduce the parquet leg's order key for
// key (design.md §6.1.1, Ordering parity), has no LIST order: such a page
// would merge its hot rows in an order no cursor can continue. The check
// runs before routing, like validateFederatedCondition, so the answer does
// not depend on whether the engine routed the request to PostgreSQL or to
// DuckDB. A request without the federated flag keeps its sort.
func validateFederatedSort(req *forma.QueryRequest, orders []model.AttributeOrder) error {
	if req.Federated == nil || !req.Federated.Enabled {
		return nil
	}
	for _, ao := range orders {
		if ao.ValueType == forma.ValueTypeList {
			return forma.InvalidInputf("cannot sort a federated query on list attribute '%s' in schema '%s': the federated route does not order lists; sort on a scalar attribute, or query without federated", ao.AttrName, req.SchemaName)
		}
	}
	return nil
}
