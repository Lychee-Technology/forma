//go:build e2e

package production

import (
	"context"
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

// nestedRepairProps/Attrs evolve e2e_simple into two nested objects, each an
// EAV-only bigint beside a text sibling: contact.total under
// required_always, account.total under required_if_parent_present.
const nestedRepairProps = `{
    "contact": {"type": "object", "properties": {"total": {"type": "integer"}, "name": {"type": "string"}}},
    "account": {"type": "object", "properties": {"total": {"type": "integer"}, "name": {"type": "string"}}}
  }`

const nestedRepairAttrs = `{
  "contact.total": {"attributeID": 1, "valueType": "bigint", "required_policy": "required_always"},
  "contact.name": {"attributeID": 2, "valueType": "text"},
  "account.total": {"attributeID": 3, "valueType": "bigint", "required_policy": "required_if_parent_present"},
  "account.name": {"attributeID": 4, "valueType": "text"}
}
`

// TestNestedBigintLegacyImageRepairByUpdate pins the census repair for a
// required nested bigint (#590 review): a PUT naming the attribute by its
// literal dotted key rewrites a stored image the funnel refuses, whatever the
// image, under either required policy. The stored value it replaces is never
// decoded, the sibling the PUT does not name is kept, the row reads
// normally, and the census then reports it clean. A PUT naming only the
// sibling, in either spelling, carries the image into the write and fails
// as TestBigintLegacyImageRepairByUpdate's unrelated PUT does.
func TestNestedBigintLegacyImageRepairByUpdate(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, nestedRepairProps, nestedRepairAttrs)))
	ctx := context.Background()
	schema := DefaultSchemaFixtures()[0]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()
	tables := widthaudit.Tables{EAV: env.Tables.EAVData, ChangeLog: env.Tables.ChangeLog}

	for object, attrID := range map[string]int{"contact": 1, "account": 3} {
		for _, image := range []string{"9007199254740994", "1000.5", "9223372036854775808"} {
			rowID := seedNestedLegacyImage(t, ctx, env, srv.URL, schema, attrID, image)
			if !censusFlagsRow(t, ctx, env, tables, rowID) {
				t.Fatalf("census does not report the planted %s.total image %s", object, image)
			}

			for _, sibling := range []string{`{"` + object + `":{"name":"x"}}`, `{"` + object + `.name":"x"}`} {
				status, body := putWide(t, srv.URL, schema.Name, rowID, sibling)
				assertLegacyImageCarried(t, object+".total", image, sibling, status, body)
			}

			status, body := putWide(t, srv.URL, schema.Name, rowID, `{"`+object+`.total":42}`)
			if status != http.StatusOK {
				t.Fatalf("PUT %s.total over image %s: status %d body %s, want %d", object, image, status, body, http.StatusOK)
			}
			got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
			if err != nil {
				t.Fatalf("get repaired row (%s.total image %s): %v", object, image, err)
			}
			nested, _ := got.Attributes[object].(map[string]any)
			if total, _ := nested["total"].(int64); total != 42 || nested["name"] != "kept" {
				t.Fatalf("repaired %s = %#v, want total int64 42 beside name \"kept\"", object, got.Attributes[object])
			}
			if censusFlagsRow(t, ctx, env, tables, rowID) {
				t.Fatalf("census still reports the row repaired over %s.total image %s", object, image)
			}
		}
	}
}

// seedNestedLegacyImage creates a row holding both objects and rewrites the
// stored image of attrID underneath the funnel.
func seedNestedLegacyImage(t *testing.T, ctx context.Context, env *Env, baseURL string, schema SchemaRef, attrID int, image string) uuid.UUID {
	t.Helper()
	payload := `{"contact":{"total":1,"name":"kept"},"account":{"total":1,"name":"kept"}}`
	resp, err := http.Post(baseURL+"/api/v1/"+schema.Name, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read create response: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create seed for image %s: status %d body %s", image, resp.StatusCode, body)
	}
	rowID := uuid.MustParse(decodeCreatedRowID(t, string(body)))
	env.ExecSQL(ctx, "UPDATE "+env.Tables.EAVData+" SET value_numeric = "+image+" WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3",
		schema.ID, rowID, attrID)
	return rowID
}

// assertLegacyImageCarried checks a PUT that kept a stored image the funnel
// refuses: a whole image past 2^53 is a 400 naming the attribute and the
// image, and one the read cannot decode is a server error.
func assertLegacyImageCarried(t *testing.T, attr, image, payload string, status int, body string) {
	t.Helper()
	if image != "9007199254740994" {
		if status < http.StatusInternalServerError {
			t.Fatalf("PUT %s over unreadable %s image %s: status %d body %s, want a server error", payload, attr, image, status, body)
		}
		return
	}
	if status != http.StatusBadRequest || !strings.Contains(body, attr) || !strings.Contains(body, "float64 image "+image) {
		t.Fatalf("PUT %s over %s image %s: status %d body %s, want %d naming %s and the image",
			payload, attr, image, status, body, http.StatusBadRequest, attr)
	}
}
