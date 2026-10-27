package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func encodedAttr(name string, id int, valueType, column, encoding string) string {
	return fmt.Sprintf(`"%s":{"attributeID":%d,"valueType":"%s","column_binding":{"col_name":"%s","encoding":"%s"}}`,
		name, id, valueType, column, encoding)
}

// eavInsert renders an eav_data row of attribute 1; %[1]s is the table.
func eavInsert(indices, column, value string) string {
	return fmt.Sprintf("INSERT INTO %%[1]s (schema_id, row_id, attr_id, array_indices, %s) VALUES (100, gen_random_uuid(), 1, '%s', %s)",
		column, indices, value)
}

// mainTextInsert renders an entity_main row with text_01 set; %[2]s is the
// table.
func mainTextInsert(value string) string {
	return fmt.Sprintf("INSERT INTO %%[2]s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01) "+
		"VALUES (100, gen_random_uuid(), 1, 1, %s)", value)
}

// TestBaselineSchemaRefusesAStoredValueTheReadRefuses: each storage shape of
// each declared type holds an image the OLTP read refuses, and the baseline
// refuses it without moving the pointer.
func TestBaselineSchemaRefusesAStoredValueTheReadRefuses(t *testing.T) {
	cases := []struct {
		name, attrs, insert, want string
	}{
		{"EAV bool NaN", `{"a":{"attributeID":1,"valueType":"bool"}}`,
			eavInsert("", "value_numeric", "'NaN'"), "declared bool: row %s holds NaN in eav_data,"},
		{"list<bool> element infinity", `{"a":{"attributeID":1,"valueType":"list","items_type":"bool"}}`,
			eavInsert("0", "value_numeric", "'Infinity'"), "declared bool: row %s holds Infinity in eav_data element 0,"},
		{"EAV datetime fraction", `{"a":{"attributeID":1,"valueType":"datetime"}}`,
			eavInsert("", "value_numeric", "1000.5"), "declared datetime: row %s holds 1000.5 in eav_data,"},
		{"EAV datetime past 2^53", `{"a":{"attributeID":1,"valueType":"datetime"}}`,
			eavInsert("", "value_numeric", "9007199254740993"), "declared datetime: row %s holds 9007199254740993 in eav_data,"},
		{"list<date> element fraction", `{"a":{"attributeID":1,"valueType":"list","items_type":"date"}}`,
			eavInsert("1", "value_numeric", "1.5"), "declared date: row %s holds 1.5 in eav_data element 1,"},
		{"EAV uuid that does not parse", `{"a":{"attributeID":1,"valueType":"uuid"}}`,
			eavInsert("", "value_text", "'not-a-uuid'"), `declared uuid: row %s holds "not-a-uuid" in eav_data,`},
		{"list<uuid> element that does not parse", `{"a":{"attributeID":1,"valueType":"list","items_type":"uuid"}}`,
			eavInsert("0", "value_text", "'6ba7b810-9dad11d1-80b4-00c04fd430c8'"),
			`declared uuid: row %s holds "6ba7b810-9dad11d1-80b4-00c04fd430c8" in eav_data element 0,`},
		{"EAV integer in value_text", eavIntegerAttrs,
			eavInsert("", "value_text", "'12'"), `declared integer: row %s holds "12" in eav_data,`},
		{"EAV text in value_numeric", `{"a":{"attributeID":1,"valueType":"text"}}`,
			eavInsert("", "value_numeric", "42"), "declared text: row %s holds 42 in eav_data,"},
		{"bool_text other than 1/0", "{" + encodedAttr("a", 1, "bool", "text_01", "bool_text") + "}",
			mainTextInsert("'true'"), `declared bool: row %s holds "true" in column text_01,`},
		{"iso8601 day outside its month", "{" + encodedAttr("a", 1, "date", "text_01", "iso8601") + "}",
			mainTextInsert("'2023-02-29T00:00:00Z'"), `declared date: row %s holds "2023-02-29T00:00:00Z" in column text_01,`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lt := newLedgerTarget(t, map[string]string{"orders": tc.attrs})
			lt.forgetLedger(t)
			insert := fmt.Sprintf(tc.insert, quoteIdentifier(lt.opts.eavTable), quoteIdentifier(lt.opts.entityMain))
			if _, err := lt.pool.Exec(context.Background(), insert); err != nil {
				t.Fatalf("seed a row: %v", err)
			}
			rowID := lt.onlyRowID(t)
			out, err := lt.baseline(t, nil)
			if want := fmt.Sprintf(tc.want, rowID); err == nil || !strings.Contains(out, want) {
				t.Fatalf("baseline = %v, want the row refused (%s)\n%s", err, want, out)
			}
			if _, ok := lt.pointer(t, 100); ok {
				t.Fatal("a refused baseline recorded a generation")
			}
		})
	}
}

// TestBaselineSchemaAdmitsWhatTheReadAdmits: the edges of each domain are
// inside it, including images the read admits and the write never renders.
func TestBaselineSchemaAdmitsWhatTheReadAdmits(t *testing.T) {
	const canonical = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	lt := newLedgerTarget(t, map[string]string{"orders": `{` +
		`"flag":{"attributeID":1,"valueType":"bool"},` +
		`"seen":{"attributeID":2,"valueType":"datetime"},` +
		`"ref":{"attributeID":3,"valueType":"uuid"},` +
		`"refs":{"attributeID":4,"valueType":"list","items_type":"uuid"},` +
		`"note":{"attributeID":5,"valueType":"text"},` +
		encodedAttr("on", 6, "bool", "text_01", "bool_text") + "," +
		encodedAttr("due", 7, "datetime", "text_02", "iso8601") + "," +
		encodedAttr("set", 8, "bool", "smallint_01", "bool_smallint") + "}"})
	lt.forgetLedger(t)
	eav := quoteIdentifier(lt.opts.eavTable)
	stmts := []string{
		fmt.Sprintf("INSERT INTO %s (schema_id, row_id, attr_id, array_indices, value_numeric, value_text) VALUES "+
			"(100, gen_random_uuid(), 1, '', 0.3, NULL), "+
			"(100, gen_random_uuid(), 2, '', 9007199254740992, NULL), "+
			"(100, gen_random_uuid(), 2, '', -9007199254740992, NULL), "+
			"(100, gen_random_uuid(), 3, '', NULL, 'urn:uuid:%[2]s'), "+
			"(100, gen_random_uuid(), 3, '', NULL, '{%[2]s}'), "+
			"(100, gen_random_uuid(), 4, '0', NULL, '%[2]s'), "+
			"(100, gen_random_uuid(), 4, '', NULL, NULL), "+
			"(100, gen_random_uuid(), 5, '', NULL, 'anything')", eav, canonical),
		fmt.Sprintf("INSERT INTO %s (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, text_01, text_02, smallint_01) "+
			"VALUES (100, gen_random_uuid(), 1, 1, '1', '2024-02-29T12:00:00.5+05:30', 7)", quoteIdentifier(lt.opts.entityMain)),
	}
	for _, stmt := range stmts {
		if _, err := lt.pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("seed rows: %v", err)
		}
	}
	out, err := lt.baseline(t, nil)
	if err != nil || !strings.Contains(out, "Recorded generation 1 of schema orders") {
		t.Fatalf("baseline = %v\n%s", err, out)
	}
}

// onlyRowID answers the row_id of the one row a case seeded.
func (lt *ledgerTarget) onlyRowID(t *testing.T) string {
	t.Helper()
	query := fmt.Sprintf("SELECT row_id::text FROM %s UNION ALL SELECT ltbase_row_id::text FROM %s",
		quoteIdentifier(lt.opts.eavTable), quoteIdentifier(lt.opts.entityMain))
	var rowID string
	if err := lt.pool.QueryRow(context.Background(), query).Scan(&rowID); err != nil {
		t.Fatalf("read the seeded row: %v", err)
	}
	return rowID
}
