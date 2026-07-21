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
// the legacy SortBy list shares the single SortOrder. The two surfaces are
// mutually exclusive so a caller bug cannot be silently half-applied.
func resolveSortKeys(req *forma.QueryRequest) ([]sortKey, error) {
	if len(req.Sort) == 0 {
		sortOrder := req.SortOrder
		if sortOrder == "" {
			sortOrder = forma.SortOrderAsc
		}
		keys := make([]sortKey, 0, len(req.SortBy))
		for _, attr := range req.SortBy {
			keys = append(keys, sortKey{attr: attr, order: sortOrder})
		}
		return keys, nil
	}

	if len(req.SortBy) > 0 || req.SortOrder != "" {
		return nil, fmt.Errorf(
			"sort cannot be combined with sort_by/sort_order: use sort alone for per-key directions: %w",
			forma.ErrInvalidInput)
	}

	keys := make([]sortKey, 0, len(req.Sort))
	for i, entry := range req.Sort {
		attr := strings.TrimSpace(entry.Attribute)
		if attr == "" {
			return nil, fmt.Errorf("sort entry %d has an empty attribute: %w", i, forma.ErrInvalidInput)
		}
		order, err := normalizeSortOrder(entry.SortOrder)
		if err != nil {
			return nil, fmt.Errorf("sort entry for attribute '%s': %w", attr, err)
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
		return "", fmt.Errorf("invalid sort_order '%s': expected 'asc' or 'desc': %w", raw, forma.ErrInvalidInput)
	}
}

// buildAttributeOrders resolves the request's sort keys against the schema
// cache into typed AttributeOrders, tagging each with its storage location
// (main column vs EAV) and its own direction. It errors on an unknown sort
// attribute.
func buildAttributeOrders(req *forma.QueryRequest, schemaCache forma.SchemaAttributeCache) ([]model.AttributeOrder, error) {
	keys, err := resolveSortKeys(req)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve sort keys for schema '%s': %w", req.SchemaName, err)
	}

	orders := make([]model.AttributeOrder, 0, len(keys))
	for _, key := range keys {
		meta, ok := schemaCache[key.attr]
		if !ok {
			return nil, fmt.Errorf("cannot sort by unknown attribute '%s' in schema '%s'", key.attr, req.SchemaName)
		}
		order := model.AttributeOrder{
			AttrID:    meta.AttributeID,
			ValueType: meta.ValueType,
			SortOrder: key.order,
			AttrName:  key.attr,
		}
		if meta.ColumnBinding != nil {
			order.StorageLocation = forma.AttributeStorageLocationMain
			order.ColumnName = string(meta.ColumnBinding.ColumnName)
		} else {
			order.StorageLocation = forma.AttributeStorageLocationEAV
		}
		orders = append(orders, order)
	}
	return orders, nil
}
