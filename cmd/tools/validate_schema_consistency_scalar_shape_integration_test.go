package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// TestValidateSchemaConsistencyCensusesIndexedScalarRecords runs the
// scalar-shape census (#696) over a real eav_data. contact declares its
// array ancestry: note has none, addresses.city one. Array elements under
// note and a second index under addresses.city are failures; a member of the
// array of objects and a list's elements are not reported. Each finding's
// example is one record that exists, its row ID and indices read together:
// the group's smallest row ID holds the larger index.
func TestValidateSchemaConsistencyCensusesIndexedScalarRecords(t *testing.T) {
	tg := newInitDBTarget(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	dir := scalarShapeSchemaDir(t)
	cache := schemameta.NewMetadataCache()
	if err := cache.RegisterSchema("contact", 100, forma.SchemaAttributeCache{
		"note":           {AttributeName: "note", AttributeID: 1, ValueType: forma.ValueTypeText},
		"tags":           {AttributeName: "tags", AttributeID: 2, ValueType: forma.ValueTypeList},
		"addresses.city": {AttributeName: "addresses.city", AttributeID: 3, ValueType: forma.ValueTypeText},
	}); err != nil {
		t.Fatalf("register schema: %v", err)
	}

	low, high := uuid.MustParse("00000000-0000-4000-8000-000000000001"), uuid.MustParse("ffffffff-0000-4000-8000-000000000001")
	ctx := context.Background()
	insert := fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text) VALUES (100, $1, $2, $3, 'v')",
		quoteIdentifier(tg.opts.eavTable))
	for _, r := range []struct {
		row     uuid.UUID
		attr    int16
		indices string
	}{
		{low, 1, "5"}, {high, 1, "0"}, {high, 1, ""},
		{low, 2, "0"}, {low, 2, "1"},
		{low, 3, "0"}, {high, 3, "1,0"},
	} {
		if _, err := tg.pool.Exec(ctx, insert, r.row, r.attr, r.indices); err != nil {
			t.Fatalf("seed %+v: %v", r, err)
		}
	}

	v := schemaConsistencyValidator{pool: tg.pool, schemaDir: dir, eavTable: tg.opts.eavTable, out: io.Discard}
	issues, err := v.checkIndexedRowsUnderScalarAttrs(ctx, cache)
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	want := []string{
		fmt.Sprintf("schema=contact schema_id=100 attr_id=1 attribute=note type=text indices=1 array_ancestors=0 rows=2 example=row_id=%s array_indices=\"5\"", low),
		fmt.Sprintf("schema=contact schema_id=100 attr_id=3 attribute=addresses.city type=text indices=2 array_ancestors=1 rows=1 example=row_id=%s array_indices=\"1,0\"", high),
	}
	if len(issues) != len(want) {
		t.Fatalf("got %d issues, want %d: %+v", len(issues), len(want), issues)
	}
	for i, issue := range issues {
		if issue.details != want[i] || issue.severity != severityError ||
			issue.category != "array elements stored under scalar attributes in "+tg.opts.eavTable {
			t.Errorf("issue %d = %+v, want a failure %q", i, issue, want[i])
		}
	}
	var exists bool
	if err := tg.pool.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE schema_id = 100 AND attr_id = 1 AND row_id = $1 AND array_indices = '5')",
		quoteIdentifier(tg.opts.eavTable)), low).Scan(&exists); err != nil || !exists {
		t.Fatalf("the reported example must be a stored record: exists=%t err=%v", exists, err)
	}
	if strings.Contains(fmt.Sprint(issues), "'v'") {
		t.Fatalf("the census must not print values: %+v", issues)
	}
}
