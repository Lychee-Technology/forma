package internal

import (
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

// buildSortTestCache mirrors the e2e_wide shape used by the sort-stability suite:
// two main-bound attributes plus one pure-EAV attribute.
func buildSortTestCache() forma.SchemaAttributeCache {
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
			{Attribute: "rank"}, // omitted direction → asc
			{Attribute: "count", SortOrder: forma.SortOrderDesc},  // explicit desc
			{Attribute: "qty", SortOrder: forma.SortOrder("ASC")}, // case-insensitive
		},
	}
	orders, err := buildAttributeOrders(req, buildSortTestCache())
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
	orders, err := buildAttributeOrders(req, buildSortTestCache())
	if err != nil {
		t.Fatalf("buildAttributeOrders legacy: %v", err)
	}
	if len(orders) != 2 {
		t.Fatalf("legacy orders length = %d, want 2", len(orders))
	}
	for i, o := range orders {
		if o.SortOrder != forma.SortOrderDesc {
			t.Fatalf("legacy orders[%d].SortOrder = %s, want shared desc", i, o.SortOrder)
		}
	}
}

func TestBuildAttributeOrdersLegacyDefaultAsc(t *testing.T) {
	req := &forma.QueryRequest{SchemaName: "e2e_wide", SortBy: []string{"qty"}}
	orders, err := buildAttributeOrders(req, buildSortTestCache())
	if err != nil {
		t.Fatalf("buildAttributeOrders legacy default: %v", err)
	}
	if orders[0].SortOrder != forma.SortOrderAsc {
		t.Fatalf("legacy default SortOrder = %s, want asc", orders[0].SortOrder)
	}
}

func TestBuildAttributeOrdersLegacyCaseInsensitiveDesc(t *testing.T) {
	req := &forma.QueryRequest{
		SchemaName: "e2e_wide",
		SortBy:     []string{"rank"},
		SortOrder:  forma.SortOrder("DESC"),
	}
	orders, err := buildAttributeOrders(req, buildSortTestCache())
	if err != nil {
		t.Fatalf("buildAttributeOrders legacy DESC: %v", err)
	}
	if orders[0].SortOrder != forma.SortOrderDesc {
		t.Fatalf("legacy orders[0].SortOrder = %s, want folded desc", orders[0].SortOrder)
	}
}

func TestBuildAttributeOrdersLegacyInvalidDirectionMessage(t *testing.T) {
	// Pins the legacy surface to the same carrier and prose as the structured
	// Sort path (#296): the message is produced by the shared normalizeSortOrder,
	// so the two surfaces cannot drift.
	req := &forma.QueryRequest{
		SchemaName: "e2e_wide",
		SortBy:     []string{"rank"},
		SortOrder:  forma.SortOrder("descending"),
	}
	_, err := buildAttributeOrders(req, buildSortTestCache())
	if err == nil {
		t.Fatalf("expected invalid sort_order error, got nil")
	}
	want := "invalid sort_order 'descending': expected 'asc' or 'desc'"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err.Error(), want)
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("error %v does not wrap forma.ErrInvalidInput", err)
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
		{"legacy_invalid_direction", &forma.QueryRequest{
			SchemaName: "e2e_wide",
			SortBy:     []string{"rank"},
			SortOrder:  forma.SortOrder("descending"),
		}},
		{"legacy_direction_without_sortby", &forma.QueryRequest{
			// A garbage direction is caller fault even with no sort_by to apply it
			// to (#296 boundary contract): non-empty SortOrder is always validated.
			SchemaName: "e2e_wide",
			SortOrder:  forma.SortOrder("descending"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildAttributeOrders(tc.req, buildSortTestCache())
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
	_, err := buildAttributeOrders(req, buildSortTestCache())
	if err == nil {
		t.Fatalf("expected unknown-attribute error, got nil")
	}
	// Same message contract as the legacy SortBy path (names attr + schema), now
	// carrying forma.ErrInvalidInput (#296). The sentinel is what earns this a 400
	// with a verbatim body at the HTTP boundary: since #301, classifyManagerError
	// reads sentinels only, so an unwrapped version of this error would answer 500
	// with a redacted body. The prose is deliberately unchanged.
	want := "cannot sort by unknown attribute 'ghost' in schema 'e2e_wide'"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err.Error(), want)
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("error %v does not wrap forma.ErrInvalidInput", err)
	}
}
