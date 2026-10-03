//go:build e2e

package production

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// boundBigintProps/Attrs evolve e2e_simple into a bigint bound to double_01,
// ratio, beside the text column name.
const boundBigintProps = `{
    "name": {"type": "string"},
    "ratio": {"type": "integer"}
  }`

const boundBigintAttrs = `{
  "name": {"attributeID": 1, "valueType": "text", "column_binding": {"col_name": "text_01"}},
  "ratio": {"attributeID": 2, "valueType": "bigint", "column_binding": {"col_name": "double_01"}}
}
`

// TestBoundBigintDoubleColumnCensusAndRepair pins #618 on a real database: a
// bigint bound to a double_* column is stored as its float64 image, and the
// census validate-schema-consistency runs reports an entity_main row whose
// image is past ±2^53 or names no int64, with the schema, the row, the
// attribute and the column. Values the funnel admits, ±2^53 included, are
// never reported. A PUT that does not name the attribute carries the image
// into the write and fails; a PUT that names it rewrites it whatever the
// image, and the census then reports the row clean.
func TestBoundBigintDoubleColumnCensusAndRepair(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, boundBigintProps, boundBigintAttrs)))
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()
	tables := widthaudit.Tables{EAV: env.Tables.EAVData, EntityMain: env.Tables.EntityMain, ChangeLog: env.Tables.ChangeLog}

	for _, lit := range []string{"9007199254740992", "-9007199254740992", "9007199254740991", "0"} {
		status, body := postWide(t, srv.URL, schema.Name, `{"name":"in-contract","ratio":`+lit+`}`)
		if status != http.StatusCreated {
			t.Fatalf("create ratio=%s: status %d body %s, want %d", lit, status, body, http.StatusCreated)
		}
	}
	status, body := postWide(t, srv.URL, schema.Name, `{"name":"refused","ratio":9007199254740994}`)
	if status != http.StatusBadRequest || !strings.Contains(body, "bound column double_01 (double)") {
		t.Fatalf("create ratio=9007199254740994: status %d body %s, want %d naming the bound column", status, body, http.StatusBadRequest)
	}
	if findings := boundBigintCensus(t, ctx, env, tables); len(findings) != 0 {
		t.Fatalf("census reports values the funnel admitted: %+v", findings)
	}

	for _, tc := range []struct {
		image    string // SQL literal planted in double_01
		stored   string // the value the census names
		readable bool   // a whole number inside int64: the row still reads
	}{
		{"9007199254740994", "9007199254740994", true},
		{"-9007199254740994", "-9007199254740994", true},
		{"1000.5", "1000.5", false},
		{"9223372036854775808", "9223372036854775808", false},
		{"'NaN'", "NaN", false},
		{"'Infinity'", "Infinity", false},
		{"'-Infinity'", "-Infinity", false},
	} {
		status, body := postWide(t, srv.URL, schema.Name, `{"name":"kept","ratio":1}`)
		if status != http.StatusCreated {
			t.Fatalf("create seed for image %s: status %d body %s", tc.image, status, body)
		}
		rowID := uuid.MustParse(decodeCreatedRowID(t, body))
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET double_01 = "+tc.image+" WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
			schema.ID, rowID)

		findings := boundBigintCensus(t, ctx, env, tables)
		if len(findings) != 1 {
			t.Fatalf("census over the planted image %s: %d finding(s), want 1: %+v", tc.image, len(findings), findings)
		}
		f := findings[0]
		if f.RowID != rowID || f.SchemaName != schema.Name || f.AttrName != "ratio" || f.Column != "double_01" ||
			f.Declared != forma.ValueTypeBigInt || f.StoredValue != tc.stored || !f.Pending {
			t.Fatalf("finding for the planted image %s on row %s = %+v, want pending ratio in double_01 holding %s", tc.image, rowID, f, tc.stored)
		}
		if class := f.Classify(0); class != widthaudit.ClassBigIntOutOfContract {
			t.Fatalf("planted image %s classified %v, want ClassBigIntOutOfContract", tc.image, class)
		}

		status, body = putWide(t, srv.URL, schema.Name, rowID, `{"name":"unrelated"}`)
		if tc.readable {
			if status != http.StatusBadRequest || !strings.Contains(body, "ratio") || !strings.Contains(body, "float64 image "+tc.stored) {
				t.Fatalf("PUT name over image %s: status %d body %s, want %d naming ratio and the image", tc.image, status, body, http.StatusBadRequest)
			}
		} else if status < http.StatusInternalServerError {
			t.Fatalf("PUT name over unreadable image %s: status %d body %s, want a server error", tc.image, status, body)
		}

		status, body = putWide(t, srv.URL, schema.Name, rowID, `{"ratio":5}`)
		if status != http.StatusOK {
			t.Fatalf("PUT ratio over image %s: status %d body %s, want %d", tc.image, status, body, http.StatusOK)
		}
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
		if err != nil {
			t.Fatalf("get repaired row (image %s): %v", tc.image, err)
		}
		if ratio, _ := got.Attributes["ratio"].(int64); ratio != 5 || got.Attributes["name"] != "kept" {
			t.Fatalf("repaired row = %#v, want ratio int64 5 beside name \"kept\"", got.Attributes)
		}
		if findings := boundBigintCensus(t, ctx, env, tables); len(findings) != 0 {
			t.Fatalf("census still reports the row repaired over image %s: %+v", tc.image, findings)
		}
	}
}

func boundBigintCensus(t *testing.T, ctx context.Context, env *Env, tables widthaudit.Tables) []widthaudit.Finding {
	t.Helper()
	findings, err := widthaudit.Census(ctx, env.Pool, tables, widthaudit.Targets(env.Metadata))
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	return findings
}
