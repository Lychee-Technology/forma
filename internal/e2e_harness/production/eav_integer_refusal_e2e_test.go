//go:build e2e

package production

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// eavIntegerProps/Attrs evolve e2e_simple into EAV-only integers (#729): a
// scalar integer and smallint, a list of integers, and an integer bound to a
// column as wide as its type, whose eav_data rows from before a promotion
// the read still decodes.
const eavIntegerProps = `{
    "name": {"type": "string"},
    "qty": {"type": "integer"},
    "tiny": {"type": "integer"},
    "levels": {"type": "array", "items": {"type": "integer"}},
    "cnt": {"type": "integer"}
  }`

const eavIntegerAttrs = `{
  "name": {"attributeID": 1, "valueType": "text", "column_binding": {"col_name": "text_01"}},
  "qty": {"attributeID": 2, "valueType": "integer"},
  "tiny": {"attributeID": 3, "valueType": "smallint"},
  "levels": {"attributeID": 4, "valueType": "list", "items_type": "integer"},
  "cnt": {"attributeID": 5, "valueType": "integer", "column_binding": {"col_name": "integer_01"}}
}
`

const eavIntegerSeed = `{"name":"kept","qty":1,"tiny":1,"levels":[1,1],"cnt":1}`

// eavIntegerCase is one invalid image planted in one eav_data record.
type eavIntegerCase struct {
	attr     string
	attrID   int16
	indices  string
	declared forma.ValueType
	image    string // SQL literal planted in value_numeric
	stored   string // the NUMERIC text the census and the read name
	repair   string // a PUT body naming the attribute
}

func eavIntegerCases() []eavIntegerCase {
	qty := func(image, stored string) eavIntegerCase {
		return eavIntegerCase{"qty", 2, "", forma.ValueTypeInteger, image, stored, `{"qty":5}`}
	}
	tiny := func(image string) eavIntegerCase {
		return eavIntegerCase{"tiny", 3, "", forma.ValueTypeSmallInt, image, image, `{"tiny":5}`}
	}
	return []eavIntegerCase{
		qty("4294967296", "4294967296"), qty("2147483648", "2147483648"), qty("-2147483649", "-2147483649"),
		qty("1001.5", "1001.5"),
		qty("'NaN'", "NaN"), qty("'Infinity'", "Infinity"), qty("'-Infinity'", "-Infinity"),
		// A fraction float64 cannot keep: its image is the whole 1001.
		qty("1001.0000000000000001", "1001.0000000000000001"),
		tiny("40000"), tiny("-40000"), tiny("32768"), tiny("-32769"),
		{"levels", 4, "1", forma.ValueTypeInteger, "4294967296", "4294967296", `{"levels":[1,2]}`},
	}
}

// eavIntegerState is what a failed update must leave untouched: the planted
// record's NUMERIC text, the row version and its change_log entries.
type eavIntegerState struct {
	value     string
	version   int64
	changes   int
	changedAt int64
}

func readEAVIntegerState(ctx context.Context, t *testing.T, env *Env, schemaID int16, rowID uuid.UUID, attrID int16, indices string) eavIntegerState {
	t.Helper()
	var s eavIntegerState
	if err := env.Pool.QueryRow(ctx, "SELECT COALESCE(value_numeric::text, '') FROM "+env.Tables.EAVData+
		" WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = $4", schemaID, rowID, attrID, indices).Scan(&s.value); err != nil {
		t.Fatalf("read eav_data attr %d of %s: %v", attrID, rowID, err)
	}
	if err := env.Pool.QueryRow(ctx, "SELECT ltbase_updated_at FROM "+env.Tables.EntityMain+
		" WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", schemaID, rowID).Scan(&s.version); err != nil {
		t.Fatalf("read the version of %s: %v", rowID, err)
	}
	if err := env.Pool.QueryRow(ctx, "SELECT COUNT(*), COALESCE(MAX(changed_at), 0) FROM "+env.Tables.ChangeLog+
		" WHERE schema_id = $1 AND row_id = $2", schemaID, rowID).Scan(&s.changes, &s.changedAt); err != nil {
		t.Fatalf("read change_log of %s: %v", rowID, err)
	}
	return s
}

// TestEAVIntegerImageReadRefusalAndRepair pins #729 on a real database: an
// EAV-only smallint or integer, or a list item of one, whose eav_data image
// its declared type cannot hold is reported by the census; the OLTP read
// refuses the row with a server error instead of answering a wrapped or
// truncated value; a PUT that does not name the attribute fails and leaves
// the stored image, the row version and change_log exactly as they were; a
// PUT that names it repairs the row. Before #729 the read answered
// 4294967296 as -2147483648 and 1001.5 as 1001, and the unrelated PUT wrote
// that value back.
func TestEAVIntegerImageReadRefusalAndRepair(t *testing.T) {
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, eavIntegerProps, eavIntegerAttrs)))
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0]
	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()
	tables := widthaudit.Tables{EAV: env.Tables.EAVData, EntityMain: env.Tables.EntityMain, ChangeLog: env.Tables.ChangeLog}

	assertEAVIntegerBoundsRead(t, ctx, env, srv.URL, tables)

	for _, tc := range eavIntegerCases() {
		label := tc.attr + "=" + tc.stored
		rowID := seedEAVIntegerRow(t, srv.URL, label)
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EAVData+" SET value_numeric = "+tc.image+
			" WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3 AND array_indices = $4", schema.ID, rowID, tc.attrID, tc.indices)
		before := readEAVIntegerState(ctx, t, env, schema.ID, rowID, tc.attrID, tc.indices)
		if before.value != tc.stored {
			t.Fatalf("%s: planted value_numeric reads %s", label, before.value)
		}

		f := singleEAVIntegerFinding(t, ctx, env, tables, label)
		if f.RowID != rowID || f.AttrName != tc.attr || f.Declared != tc.declared || f.Column != "" ||
			f.ArrayIndices != tc.indices || f.StoredValue != tc.stored || f.Classify() != widthaudit.ClassEAVIntegerOutOfContract {
			t.Fatalf("%s: finding on row %s = %+v", label, rowID, f)
		}

		assertEAVIntegerReadRefused(t, ctx, env, srv.URL, tc, rowID, label)
		if status, body := putWide(t, srv.URL, schema.Name, rowID, `{"name":"unrelated"}`); status < http.StatusInternalServerError {
			t.Fatalf("%s: PUT name: status %d body %s, want a server error", label, status, body)
		}
		if after := readEAVIntegerState(ctx, t, env, schema.ID, rowID, tc.attrID, tc.indices); after != before {
			t.Fatalf("%s: the failed PUT changed the stored state from %+v to %+v", label, before, after)
		}

		if status, body := putWide(t, srv.URL, schema.Name, rowID, tc.repair); status != http.StatusOK {
			t.Fatalf("%s: PUT %s: status %d body %s, want %d", label, tc.repair, status, body, http.StatusOK)
		}
		assertEAVIntegerRepaired(t, ctx, env, tc, rowID, label)
		if repaired := readEAVIntegerState(ctx, t, env, schema.ID, rowID, tc.attrID, tc.indices); repaired.value == tc.stored ||
			repaired.version <= before.version || repaired.changedAt <= before.changedAt {
			t.Fatalf("%s: the repair did not advance the row (%+v, before %+v)", label, repaired, before)
		}
		if findings := eavIntegerCensus(t, ctx, env, tables); len(findings) != 0 {
			t.Fatalf("%s: census still reports the repaired row: %+v", label, findings)
		}
	}
}

// assertEAVIntegerReadRefused requires the OLTP read and the HTTP GET to
// refuse the row naming the row, the attribute, value_numeric and the
// image. The federated read refuses it too, reading the same value at
// storage width DOUBLE (#384), with two #621 gaps: the DuckDB hot leg
// narrows the NUMERIC to DOUBLE before Go sees it, so a fraction float64
// cannot keep reads as the whole 1001, and a non-finite DOUBLE breaks the
// attributes JSON, failing the query without naming the row.
func assertEAVIntegerReadRefused(t *testing.T, ctx context.Context, env *Env, baseURL string, tc eavIntegerCase, rowID uuid.UUID, label string) {
	t.Helper()
	schema := DefaultSchemaFixtures()[0]
	_, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	refusal := "stored " + string(tc.declared) + " image " + tc.stored + " in value_numeric does not fit declared type " + string(tc.declared)
	if err == nil || errors.Is(err, forma.ErrInvalidInput) || !strings.Contains(err.Error(), refusal) ||
		!strings.Contains(err.Error(), rowID.String()) || !strings.Contains(err.Error(), "'"+tc.attr+"'") {
		t.Fatalf("%s: OLTP read error = %v, want a consistency error naming the row, attribute and image", label, err)
	}
	if status, body := getWide(t, baseURL, schema.Name, rowID); status < http.StatusInternalServerError || strings.Contains(body, tc.stored) {
		t.Fatalf("%s: GET status %d body %s, want a server error that does not publish the image", label, status, body)
	}

	page, err := federatedWideQuery(ctx, env, schema)
	if tc.stored == "1001.0000000000000001" {
		if err != nil || !federatedRowHolds(page, rowID, tc.attr, int32(1001)) {
			t.Fatalf("%s: federated read = %v, %v; want the narrowed 1001", label, page, err)
		}
		return
	}
	if strings.Contains(tc.stored, "N") || strings.Contains(tc.stored, "Inf") {
		if err == nil {
			t.Fatalf("%s: federated read answered %v, want an error", label, page)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), "does not fit declared type "+string(tc.declared)) || !strings.Contains(err.Error(), rowID.String()) {
		t.Fatalf("%s: federated read error = %v, want the refusal", label, err)
	}
}

func federatedRowHolds(page *forma.QueryResult, rowID uuid.UUID, attr string, want any) bool {
	if page == nil {
		return false
	}
	for _, rec := range page.Data {
		if rec.RowID == rowID {
			return reflect.DeepEqual(rec.Attributes[attr], want)
		}
	}
	return false
}

func assertEAVIntegerRepaired(t *testing.T, ctx context.Context, env *Env, tc eavIntegerCase, rowID uuid.UUID, label string) {
	t.Helper()
	got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: DefaultSchemaFixtures()[0].Name, RowID: &rowID})
	if err != nil {
		t.Fatalf("%s: read the repaired row: %v", label, err)
	}
	var want any = int32(5)
	switch {
	case tc.attr == "levels":
		want = []any{int32(1), int32(2)}
	case tc.declared == forma.ValueTypeSmallInt:
		want = int16(5)
	}
	if !reflect.DeepEqual(got.Attributes[tc.attr], want) || got.Attributes["name"] != "kept" || got.Attributes["cnt"] != int32(1) {
		t.Fatalf("%s: repaired row = %#v", label, got.Attributes)
	}
}

func seedEAVIntegerRow(t *testing.T, baseURL, label string) uuid.UUID {
	t.Helper()
	status, body := postWide(t, baseURL, DefaultSchemaFixtures()[0].Name, eavIntegerSeed)
	if status != http.StatusCreated {
		t.Fatalf("%s: create seed: status %d body %s", label, status, body)
	}
	return uuid.MustParse(decodeCreatedRowID(t, body))
}

// TestEAVIntegerRetainedRecordRefusalAndRepair: an eav_data row a
// column-bound integer kept from before an EAV-to-bound promotion is decoded
// under the declared type with the column, so an image the type cannot hold
// is refused, reported by the census without a column, and repaired by an
// update naming the attribute.
func TestEAVIntegerRetainedRecordRefusalAndRepair(t *testing.T) {
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, eavIntegerProps, eavIntegerAttrs)))
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0]
	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()
	tables := widthaudit.Tables{EAV: env.Tables.EAVData, EntityMain: env.Tables.EntityMain, ChangeLog: env.Tables.ChangeLog}

	rowID := seedEAVIntegerRow(t, srv.URL, "retained")
	env.ExecSQL(ctx, "INSERT INTO "+env.Tables.EAVData+" (schema_id, row_id, attr_id, array_indices, value_numeric) VALUES ($1, $2, 5, '', 1001.5)",
		schema.ID, rowID)

	f := singleEAVIntegerFinding(t, ctx, env, tables, "retained")
	if f.RowID != rowID || f.AttrName != "cnt" || f.Column != "" || f.StoredValue != "1001.5" {
		t.Fatalf("retained finding = %+v", f)
	}
	tc := eavIntegerCase{attr: "cnt", attrID: 5, declared: forma.ValueTypeInteger, stored: "1001.5"}
	_, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if err == nil || !strings.Contains(err.Error(), "stored integer image 1001.5 in value_numeric does not fit declared type integer") {
		t.Fatalf("OLTP read of the retained record = %v, want the refusal", err)
	}
	before := readEAVIntegerState(ctx, t, env, schema.ID, rowID, tc.attrID, "")
	if status, body := putWide(t, srv.URL, schema.Name, rowID, `{"name":"unrelated"}`); status < http.StatusInternalServerError {
		t.Fatalf("PUT name: status %d body %s, want a server error", status, body)
	}
	if after := readEAVIntegerState(ctx, t, env, schema.ID, rowID, tc.attrID, ""); after != before {
		t.Fatalf("the failed PUT changed the stored state from %+v to %+v", before, after)
	}
	if status, body := putWide(t, srv.URL, schema.Name, rowID, `{"cnt":5}`); status != http.StatusOK {
		t.Fatalf("PUT cnt: status %d body %s", status, body)
	}
	got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if err != nil || got.Attributes["cnt"] != int32(5) || got.Attributes["qty"] != int32(1) {
		t.Fatalf("repaired row = %v, %v", got, err)
	}
	if findings := eavIntegerCensus(t, ctx, env, tables); len(findings) != 0 {
		t.Fatalf("census still reports the repaired row: %+v", findings)
	}
}

// assertEAVIntegerBoundsRead writes each declared type's own bounds through
// the API, flushes them, plants a whole NUMERIC with trailing zeros, reads
// them back exactly, and requires a clean census.
func assertEAVIntegerBoundsRead(t *testing.T, ctx context.Context, env *Env, baseURL string, tables widthaudit.Tables) {
	t.Helper()
	schema := DefaultSchemaFixtures()[0]
	for _, tc := range []struct {
		payload string
		qty     int32
		tiny    int16
	}{
		{`{"name":"upper","qty":2147483647,"tiny":32767,"levels":[2147483647]}`, 2147483647, 32767},
		{`{"name":"lower","qty":-2147483648,"tiny":-32768,"levels":[-2147483648]}`, -2147483648, -32768},
	} {
		status, body := postWide(t, baseURL, schema.Name, tc.payload)
		if status != http.StatusCreated {
			t.Fatalf("create %s: status %d body %s", tc.payload, status, body)
		}
		rowID := uuid.MustParse(decodeCreatedRowID(t, body))
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
		if err != nil || got.Attributes["qty"] != tc.qty || got.Attributes["tiny"] != tc.tiny ||
			!reflect.DeepEqual(got.Attributes["levels"], []any{tc.qty}) {
			t.Fatalf("boundary row %s reads %v, %v", tc.payload, got, err)
		}
	}
	// The federated reads after this one need the warm tier to exist.
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush the boundary rows: %v", err)
	}
	rowID := seedEAVIntegerRow(t, baseURL, "trailing zeros")
	env.ExecSQL(ctx, "UPDATE "+env.Tables.EAVData+" SET value_numeric = 2147483647.000 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 2",
		schema.ID, rowID)
	got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if err != nil || got.Attributes["qty"] != int32(2147483647) {
		t.Fatalf("2147483647.000 reads %v, %v; want 2147483647", got, err)
	}
	if findings := eavIntegerCensus(t, ctx, env, tables); len(findings) != 0 {
		t.Fatalf("census reports values the declared type holds: %+v", findings)
	}
}

func eavIntegerCensus(t *testing.T, ctx context.Context, env *Env, tables widthaudit.Tables) []widthaudit.Finding {
	t.Helper()
	findings, err := widthaudit.Census(ctx, env.Pool, tables, widthaudit.TargetsWithRetainedEAV(env.Metadata))
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	return findings
}

func singleEAVIntegerFinding(t *testing.T, ctx context.Context, env *Env, tables widthaudit.Tables, label string) widthaudit.Finding {
	t.Helper()
	findings := eavIntegerCensus(t, ctx, env, tables)
	if len(findings) != 1 {
		t.Fatalf("%s: census found %d finding(s), want 1: %+v", label, len(findings), findings)
	}
	return findings[0]
}
