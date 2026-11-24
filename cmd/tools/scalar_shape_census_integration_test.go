package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestScalarShapeCensusReportsArrayElementsUnderScalars runs the census
// end to end on a real eav_data table (#696): init-db registers the schema,
// records are stored by direct SQL, and the census reports exactly the
// records the shape rule refuses to write (verified) and the ones the
// declaration cannot judge (unverified). Members of an array of objects at
// their ancestry's depth, a pre-#204 primitive array's own elements, a
// list's elements and scalars are no finding; an element of that primitive
// array stored as an array is. Each finding is an attribute/depth group, and
// its example is one record of the group: note's smallest row holds index
// "1" only, while another row holds "0".
func TestScalarShapeCensusReportsArrayElementsUnderScalars(t *testing.T) {
	tg := newInitDBTarget(t)
	writeSchemaConsistencyArtifacts(t, tg.opts.schemaDir, "shapecensus",
		`{"type":"object","properties":{
			"note":{"type":"string"},
			"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"}}}},
			"phones":{"type":"array","items":{"type":"string"}},
			"opaque":{},
			"tags":{"type":"array","items":{"type":"string"}},
			"maybe":{"type":["string","null"]}}}`,
		`{"note":{"attributeID":1,"valueType":"text"},
			"items.name":{"attributeID":2,"valueType":"text"},
			"phones":{"attributeID":3,"valueType":"text"},
			"opaque":{"attributeID":4,"valueType":"text"},
			"tags":{"attributeID":5,"valueType":"list","items_type":"text"},
			"maybe":{"attributeID":6,"valueType":"text"}}`)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	ctx := context.Background()
	var schemaID int16
	if err := tg.pool.QueryRow(ctx, fmt.Sprintf("SELECT schema_id FROM %s WHERE schema_name = 'shapecensus'", quoteIdentifier(tg.opts.schemaTable))).Scan(&schemaID); err != nil {
		t.Fatalf("read schema id: %v", err)
	}

	first, second := uuid.MustParse("00000000-0000-7000-8000-000000000001"), uuid.MustParse("00000000-0000-7000-8000-000000000002")
	insert := fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_text) VALUES ($1, $2, $3, $4, $5)", quoteIdentifier(tg.opts.eavTable))
	for _, rec := range []struct {
		row     uuid.UUID
		attr    int16
		indices string
	}{
		{first, 1, "1"}, {second, 1, "0"}, // note: array elements under a scalar
		{first, 1, "0,0"},                                  // note: a second depth of the same attribute
		{first, 2, "0,0"},                                  // items.name: one index past its array of objects
		{first, 1, ""}, {second, 2, "0"}, {second, 2, "1"}, // a scalar, and members at their ancestry's depth
		{first, 3, "0"}, {first, 3, "1"}, // phones: a pre-#204 primitive array's elements
		{second, 3, "0,0"},                // phones: an element stored as an array
		{first, 4, "0"},                   // opaque: the document does not say
		{first, 5, "0"}, {second, 5, "0"}, // tags: a list's elements
		{first, 6, "0"}, // maybe: a nullable string
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
	if !errors.As(err, &found) || found.ExitCode() != 2 || found.attributes != 4 || found.groups != 5 {
		t.Fatalf("expected exit status 2 for four verified attributes in five groups, got %v\n%s", err, out.String())
	}
	finding := func(class string, attrID int, attr string, depth, legitimate, records, rows int, row uuid.UUID, indices string) string {
		return fmt.Sprintf(`- %s: schema=shapecensus schema_id=%d attr_id=%d attribute=%s declared=text indices_per_record=%d legitimate_indices=%d records=%d rows=%d example_row=%s example_array_indices=%q`,
			class, schemaID, attrID, attr, depth, legitimate, records, rows, row, indices)
	}
	want := []string{
		fmt.Sprintf("scalar-shape-census of %s over 1 schema(s): 5 verified, 1 unverified attribute/depth group(s); 4 attribute(s) verified", tg.opts.eavTable),
		finding("verified", 1, "note", 1, 0, 2, 2, first, "1"),
		finding("verified", 1, "note", 2, 0, 1, 1, first, "0,0"),
		finding("verified", 2, "items.name", 2, 1, 1, 1, first, "0,0"),
		finding("verified", 3, "phones", 2, 1, 1, 1, second, "0,0"),
		finding("verified", 6, "maybe", 1, 0, 1, 1, first, "0"),
		finding("unverified", 4, "opaque", 1, 0, 1, 1, first, "0"),
	}
	if got := strings.Split(strings.TrimSpace(out.String()), "\n"); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("census report:\n%s\nwant:\n%s", out.String(), strings.Join(want, "\n"))
	}
	if strings.Contains(out.String(), `"v"`) {
		t.Fatalf("the report must not print stored values:\n%s", out.String())
	}
	// Every example names a record the table holds.
	for _, line := range want[1:] {
		row, indices := exampleOf(t, line)
		var n int
		if err := tg.pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE row_id = $1 AND array_indices = $2", quoteIdentifier(tg.opts.eavTable)), row, indices).Scan(&n); err != nil || n == 0 {
			t.Fatalf("example of %q names no record (%v)", line, err)
		}
	}
}

// exampleOf reads a finding line's example record.
func exampleOf(t *testing.T, line string) (uuid.UUID, string) {
	t.Helper()
	_, rest, _ := strings.Cut(line, "example_row=")
	row, quoted, _ := strings.Cut(rest, " example_array_indices=")
	indices, err := strconv.Unquote(quoted)
	if err != nil {
		t.Fatalf("parse %q: %v", line, err)
	}
	return uuid.MustParse(row), indices
}

// An empty census exits successfully.
func TestScalarShapeCensusReportsNothingOnCleanStorage(t *testing.T) {
	var out strings.Builder
	if err := reportShapeCensus(&out, "eav", 3, nil); err != nil {
		t.Fatalf("clean census: %v", err)
	}
	if got := out.String(); got != "scalar-shape-census of eav over 3 schema(s): 0 verified, 0 unverified attribute/depth group(s); 0 attribute(s) verified\n" {
		t.Fatalf("report %q", got)
	}
}
