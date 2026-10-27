//go:build e2e

package production

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// boundIntegerProps/Attrs evolve e2e_simple into a smallint and two
// integers, each bound to a column wider than its declared type.
const boundIntegerProps = `{
    "name": {"type": "string"},
    "qty": {"type": "integer"},
    "cnt": {"type": "integer"},
    "tiny": {"type": "integer"}
  }`

const boundIntegerAttrs = `{
  "name": {"attributeID": 1, "valueType": "text", "column_binding": {"col_name": "text_01"}},
  "qty": {"attributeID": 2, "valueType": "integer", "column_binding": {"col_name": "bigint_01"}},
  "cnt": {"attributeID": 3, "valueType": "integer", "column_binding": {"col_name": "double_02"}},
  "tiny": {"attributeID": 4, "valueType": "smallint", "column_binding": {"col_name": "integer_01"}}
}
`

// TestBoundIntegerWideColumnCensusReadAndRepair pins #708 on a real
// database. A smallint or integer bound to a wider column holding an image
// its declared type cannot hold is reported by the census with the schema,
// the row, the attribute and the column; the OLTP read refuses the row with
// a server error instead of answering a wrapped or truncated value; a PUT
// that does not name the attribute fails and leaves the image as stored; a
// PUT that names it repairs the row, which the census then reports clean.
// The declared type's own bounds are never reported and read exactly.
func TestBoundIntegerWideColumnCensusReadAndRepair(t *testing.T) {
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, boundIntegerProps, boundIntegerAttrs)))
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()
	tables := widthaudit.Tables{EAV: env.Tables.EAVData, EntityMain: env.Tables.EntityMain, ChangeLog: env.Tables.ChangeLog}

	assertBoundIntegerBoundsRead(t, ctx, env, srv.URL, tables)

	for _, tc := range []struct {
		attr, column string
		declared     forma.ValueType
		image        string // SQL literal planted in column
		stored       string // the value the census and the read name
	}{
		{"qty", "bigint_01", forma.ValueTypeInteger, "4294967296", "4294967296"},
		{"qty", "bigint_01", forma.ValueTypeInteger, "-2147483649", "-2147483649"},
		{"cnt", "double_02", forma.ValueTypeInteger, "1001.5", "1001.5"},
		{"cnt", "double_02", forma.ValueTypeInteger, "2147483648", "2147483648"},
		{"cnt", "double_02", forma.ValueTypeInteger, "'NaN'", "NaN"},
		{"cnt", "double_02", forma.ValueTypeInteger, "'Infinity'", "Infinity"},
		{"cnt", "double_02", forma.ValueTypeInteger, "'-Infinity'", "-Infinity"},
		{"tiny", "integer_01", forma.ValueTypeSmallInt, "32768", "32768"},
		{"tiny", "integer_01", forma.ValueTypeSmallInt, "-32769", "-32769"},
	} {
		label := tc.attr + "=" + tc.stored
		status, body := postWide(t, srv.URL, schema.Name, `{"name":"kept","qty":1,"cnt":1,"tiny":1}`)
		if status != http.StatusCreated {
			t.Fatalf("%s: create seed: status %d body %s", label, status, body)
		}
		rowID := uuid.MustParse(decodeCreatedRowID(t, body))
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET "+tc.column+" = "+tc.image+
			" WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", schema.ID, rowID)

		finding := singleBoundIntegerFinding(t, ctx, env, tables, label)
		if finding.RowID != rowID || finding.SchemaName != schema.Name || finding.AttrName != tc.attr ||
			finding.Column != tc.column || finding.Declared != tc.declared || finding.StoredValue != tc.stored || !finding.Pending {
			t.Fatalf("%s: finding on row %s = %+v", label, rowID, finding)
		}
		if class := finding.Classify(); class != widthaudit.ClassBoundIntegerOutOfContract {
			t.Fatalf("%s: classified %v", label, class)
		}

		_, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
		if err == nil || errors.Is(err, forma.ErrInvalidInput) ||
			!strings.Contains(err.Error(), "stored "+string(tc.declared)+" image "+tc.stored+" in bound column "+tc.column) ||
			!strings.Contains(err.Error(), rowID.String()) || !strings.Contains(err.Error(), "'"+tc.attr+"'") {
			t.Fatalf("%s: OLTP read error = %v, want a consistency error naming the row, attribute, column and image", label, err)
		}
		if status, body := getWide(t, srv.URL, schema.Name, rowID); status < http.StatusInternalServerError {
			t.Fatalf("%s: GET status %d body %s, want a server error", label, status, body)
		}

		if status, body := putWide(t, srv.URL, schema.Name, rowID, `{"name":"unrelated"}`); status < http.StatusInternalServerError {
			t.Fatalf("%s: PUT name: status %d body %s, want a server error", label, status, body)
		}
		if after := singleBoundIntegerFinding(t, ctx, env, tables, label+" after the unrelated PUT"); after.StoredValue != tc.stored {
			t.Fatalf("%s: the failed PUT changed the stored image to %s", label, after.StoredValue)
		}

		if status, body := putWide(t, srv.URL, schema.Name, rowID, `{"`+tc.attr+`":5}`); status != http.StatusOK {
			t.Fatalf("%s: PUT %s: status %d body %s, want %d", label, tc.attr, status, body, http.StatusOK)
		}
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
		if err != nil {
			t.Fatalf("%s: read the repaired row: %v", label, err)
		}
		var want any = int32(5)
		if tc.declared == forma.ValueTypeSmallInt {
			want = int16(5)
		}
		if got.Attributes[tc.attr] != want || got.Attributes["name"] != "kept" {
			t.Fatalf("%s: repaired row = %#v", label, got.Attributes)
		}
		if findings := boundBigintCensus(t, ctx, env, tables); len(findings) != 0 {
			t.Fatalf("%s: census still reports the repaired row: %+v", label, findings)
		}
	}
}

// assertBoundIntegerBoundsRead writes each declared type's own bounds through
// the API, reads them back exactly, and requires a clean census.
func assertBoundIntegerBoundsRead(t *testing.T, ctx context.Context, env *Env, baseURL string, tables widthaudit.Tables) {
	t.Helper()
	schema := DefaultSchemaFixtures()[0]
	for _, payload := range []string{
		`{"name":"upper","qty":2147483647,"cnt":2147483647,"tiny":32767}`,
		`{"name":"lower","qty":-2147483648,"cnt":-2147483648,"tiny":-32768}`,
	} {
		status, body := postWide(t, baseURL, schema.Name, payload)
		if status != http.StatusCreated {
			t.Fatalf("create %s: status %d body %s", payload, status, body)
		}
		rowID := uuid.MustParse(decodeCreatedRowID(t, body))
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
		if err != nil {
			t.Fatalf("read the boundary row %s: %v", payload, err)
		}
		upper := strings.Contains(payload, "upper")
		wantInt, wantSmall := int32(-2147483648), int16(-32768)
		if upper {
			wantInt, wantSmall = 2147483647, 32767
		}
		if got.Attributes["qty"] != wantInt || got.Attributes["cnt"] != wantInt || got.Attributes["tiny"] != wantSmall {
			t.Fatalf("boundary row %s reads %#v", payload, got.Attributes)
		}
	}
	if findings := boundBigintCensus(t, ctx, env, tables); len(findings) != 0 {
		t.Fatalf("census reports values the funnel admitted: %+v", findings)
	}
}

func singleBoundIntegerFinding(t *testing.T, ctx context.Context, env *Env, tables widthaudit.Tables, label string) widthaudit.Finding {
	t.Helper()
	findings := boundBigintCensus(t, ctx, env, tables)
	if len(findings) != 1 {
		t.Fatalf("%s: census found %d finding(s), want 1: %+v", label, len(findings), findings)
	}
	return findings[0]
}

func getWide(t *testing.T, baseURL, schema string, rowID uuid.UUID) (int, string) {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/v1/" + schema + "/" + rowID.String())
	if err != nil {
		t.Fatalf("get request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read get response: %v", err)
	}
	return resp.StatusCode, string(body)
}
