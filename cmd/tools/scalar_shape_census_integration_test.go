package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestScalarShapeCensusReportsArrayElementsUnderScalars runs the census
// end to end on a real eav_data table (#696): init-db registers the schema,
// records are stored by direct SQL, and the census reports exactly the
// records the shape rule refuses to write (verified) and the ones the
// declaration cannot judge (unverified). Members of an array of objects at
// their ancestry's depth, a pre-#204 primitive array's elements, a list's
// elements and scalars are no finding.
func TestScalarShapeCensusReportsArrayElementsUnderScalars(t *testing.T) {
	tg := newInitDBTarget(t)
	writeSchemaConsistencyArtifacts(t, tg.opts.schemaDir, "shapecensus",
		`{"type":"object","properties":{
			"note":{"type":"string"},
			"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"}}}},
			"phones":{"type":"array","items":{"type":"string"}},
			"opaque":{},
			"tags":{"type":"array","items":{"type":"string"}}}}`,
		`{"note":{"attributeID":1,"valueType":"text"},
			"items.name":{"attributeID":2,"valueType":"text"},
			"phones":{"attributeID":3,"valueType":"text"},
			"opaque":{"attributeID":4,"valueType":"text"},
			"tags":{"attributeID":5,"valueType":"list","items_type":"text"}}`)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	ctx := context.Background()
	var schemaID int16
	if err := tg.pool.QueryRow(ctx, fmt.Sprintf("SELECT schema_id FROM %s WHERE schema_name = 'shapecensus'", quoteIdentifier(tg.opts.schemaTable))).Scan(&schemaID); err != nil {
		t.Fatalf("read schema id: %v", err)
	}

	malformed, other := uuid.MustParse("00000000-0000-7000-8000-000000000001"), uuid.MustParse("00000000-0000-7000-8000-000000000002")
	insert := fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text) VALUES ($1, $2, $3, $4, $5)", quoteIdentifier(tg.opts.eavTable))
	for _, rec := range []struct {
		row     uuid.UUID
		attr    int16
		indices string
	}{
		{malformed, 1, "0"}, {other, 1, "0"}, {other, 1, "1"}, // note: array elements under a scalar
		{malformed, 2, "0,0"},                                // items.name: one index past its array of objects
		{malformed, 1, ""}, {other, 2, "0"}, {other, 2, "1"}, // a scalar, and members at their ancestry's depth
		{malformed, 3, "0"}, {malformed, 3, "1"}, // phones: a pre-#204 primitive array
		{malformed, 4, "0"},                  // opaque: the document does not say
		{malformed, 5, "0"}, {other, 5, "0"}, // tags: a list's elements
	} {
		if _, err := tg.pool.Exec(ctx, insert, schemaID, rec.row, rec.attr, rec.indices, "v"); err != nil {
			t.Fatalf("seed record: %v", err)
		}
	}

	var out strings.Builder
	err := runScalarShapeCensusOut(ctx, []string{
		"-schema-dir", tg.opts.schemaDir, "-schema-registry-table", tg.opts.schemaTable, "-eav-table", tg.opts.eavTable,
	}, &out)
	var found *shapeCensusFound
	if !errors.As(err, &found) || found.ExitCode() != 2 || found.verified != 2 {
		t.Fatalf("expected exit status 2 for two verified attributes, got %v\n%s", err, out.String())
	}
	want := []string{
		fmt.Sprintf("scalar-shape-census of %s over 1 schema(s): 2 verified, 1 unverified", tg.opts.eavTable),
		fmt.Sprintf(`- verified: schema=shapecensus schema_id=%d attr_id=1 attribute=note declared=text indices_per_record=1 array_ancestors=0 records=3 rows=2 example_row=%s example_array_indices="0"`, schemaID, malformed),
		fmt.Sprintf(`- verified: schema=shapecensus schema_id=%d attr_id=2 attribute=items.name declared=text indices_per_record=2 array_ancestors=1 records=1 rows=1 example_row=%s example_array_indices="0,0"`, schemaID, malformed),
		fmt.Sprintf(`- unverified: schema=shapecensus schema_id=%d attr_id=4 attribute=opaque declared=text indices_per_record=1 array_ancestors=0 records=1 rows=1 example_row=%s example_array_indices="0"`, schemaID, malformed),
	}
	if got := strings.Split(strings.TrimSpace(out.String()), "\n"); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("census report:\n%s\nwant:\n%s", out.String(), strings.Join(want, "\n"))
	}
	if strings.Contains(out.String(), `"v"`) {
		t.Fatalf("the report must not print stored values:\n%s", out.String())
	}
}

// An empty census exits successfully.
func TestScalarShapeCensusReportsNothingOnCleanStorage(t *testing.T) {
	var out strings.Builder
	if err := reportShapeCensus(&out, "eav", 3, nil); err != nil {
		t.Fatalf("clean census: %v", err)
	}
	if got := out.String(); got != "scalar-shape-census of eav over 3 schema(s): 0 verified, 0 unverified\n" {
		t.Fatalf("report %q", got)
	}
}
