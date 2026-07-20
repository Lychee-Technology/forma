package internal

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma"
)

// sortTestCache mirrors the e2e_wide shape used by the sort-stability suite:
// two main-bound attributes plus one pure-EAV attribute.
func sortTestCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"rank": {
			AttributeName: "rank", AttributeID: 1, ValueType: forma.ValueTypeNumeric,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("smallint_01")},
		},
		"count": {
			AttributeName: "count", AttributeID: 2, ValueType: forma.ValueTypeNumeric,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")},
		},
		"qty": {AttributeName: "qty", AttributeID: 3, ValueType: forma.ValueTypeNumeric},
	}
}

func TestBuildAttributeOrdersStructuredSort(t *testing.T) {
	req := &forma.QueryRequest{
		SchemaName: "e2e_wide",
		Sort: []forma.OrderBy{
			{Attribute: "rank"},                                  // omitted direction → asc
			{Attribute: "count", SortOrder: forma.SortOrderDesc}, // explicit desc
			{Attribute: "qty", SortOrder: forma.SortOrder("ASC")}, // case-insensitive
		},
	}
	orders, err := buildAttributeOrders(req, sortTestCache())
	if err != nil {
		t.Fatalf("buildAttributeOrders: %v", err)
	}
	if len(orders) != 3 {
		t.Fatalf("orders length = %d, want 3", len(orders))
	}
	if orders[0].SortOrder != forma.SortOrderAsc || orders[0].ColumnName != "smallint_01" {
		t.Fatalf("orders[0] = %+v, want asc on smallint_01", orders[0])
	}
	if orders[1].SortOrder != forma.SortOrderDesc || orders[1].ColumnName != "integer_01" {
		t.Fatalf("orders[1] = %+v, want desc on integer_01", orders[1])
	}
	if orders[2].SortOrder != forma.SortOrderAsc || orders[2].StorageLocation != forma.AttributeStorageLocationEAV {
		t.Fatalf("orders[2] = %+v, want normalized asc on EAV qty", orders[2])
	}
}

func TestBuildAttributeOrdersLegacyUnchanged(t *testing.T) {
	req := &forma.QueryRequest{
		SchemaName: "e2e_wide",
		SortBy:     []string{"rank", "count"},
		SortOrder:  forma.SortOrderDesc,
	}
	orders, err := buildAttributeOrders(req, sortTestCache())
	if err != nil {
		t.Fatalf("buildAttributeOrders legacy: %v", err)
	}
	for i, o := range orders {
		if o.SortOrder != forma.SortOrderDesc {
			t.Fatalf("legacy orders[%d].SortOrder = %s, want shared desc", i, o.SortOrder)
		}
	}
}

func TestBuildAttributeOrdersLegacyDefaultAsc(t *testing.T) {
	req := &forma.QueryRequest{SchemaName: "e2e_wide", SortBy: []string{"qty"}}
	orders, err := buildAttributeOrders(req, sortTestCache())
	if err != nil {
		t.Fatalf("buildAttributeOrders legacy default: %v", err)
	}
	if orders[0].SortOrder != forma.SortOrderAsc {
		t.Fatalf("legacy default SortOrder = %s, want asc", orders[0].SortOrder)
	}
}

func TestBuildAttributeOrdersValidation(t *testing.T) {
	cases := []struct {
		name string
		req  *forma.QueryRequest
	}{
		{"sort_conflicts_with_sortby", &forma.QueryRequest{
			SchemaName: "e2e_wide",
			SortBy:     []string{"rank"},
			Sort:       []forma.OrderBy{{Attribute: "count"}},
		}},
		{"sort_conflicts_with_sortorder", &forma.QueryRequest{
			SchemaName: "e2e_wide",
			SortOrder:  forma.SortOrderDesc,
			Sort:       []forma.OrderBy{{Attribute: "count"}},
		}},
		{"empty_attribute", &forma.QueryRequest{
			SchemaName: "e2e_wide",
			Sort:       []forma.OrderBy{{Attribute: "  "}},
		}},
		{"invalid_direction", &forma.QueryRequest{
			SchemaName: "e2e_wide",
			Sort:       []forma.OrderBy{{Attribute: "rank", SortOrder: forma.SortOrder("descending")}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildAttributeOrders(tc.req, sortTestCache())
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !errors.Is(err, forma.ErrInvalidInput) {
				t.Fatalf("error %v does not wrap forma.ErrInvalidInput", err)
			}
		})
	}
}

func TestBuildAttributeOrdersUnknownSortAttribute(t *testing.T) {
	req := &forma.QueryRequest{
		SchemaName: "e2e_wide",
		Sort:       []forma.OrderBy{{Attribute: "ghost"}},
	}
	_, err := buildAttributeOrders(req, sortTestCache())
	if err == nil {
		t.Fatalf("expected unknown-attribute error, got nil")
	}
	// Same message contract as the legacy SortBy path (plain error, names attr + schema).
	want := "cannot sort by unknown attribute 'ghost' in schema 'e2e_wide'"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
	}
}
