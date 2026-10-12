//go:build e2e

package production

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/httpapi"
)

// TestHTTPAdvancedQueryConditionDSLOperators is #646's HTTP acceptance
// probe: the new operators and $now go through POST /api/v1/advanced_query
// into the real query path. Before #646 each 200 below was a 400 ("unsupported
// operator: in" from conditionexpr.ToSQLOperator, or an unparseable date for
// $now), raised by the SQL generator behind the handler; the 200s are the
// intended change. The refusals pin the error mapping: a malformed list and a
// federated $now are caller input, answered 400 with the published message.
func TestHTTPAdvancedQueryConditionDSLOperators(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()

	// #646's Given: statuses pending, failed and published, and one row with
	// no status. `note` is EAV text; `seen` is an EAV datetime in the past.
	ids := map[string]string{}
	for _, row := range []struct{ title, payload string }{
		{"pending", `{"title":"pending","note":"pending","seen":"2024-01-02T03:04:05Z"}`},
		{"failed", `{"title":"failed","note":"failed","seen":"2024-01-02T03:04:05Z"}`},
		{"published", `{"title":"published","note":"published"}`},
		{"none", `{"title":"none"}`},
	} {
		status, body := postWide(t, srv.URL, wide.Name, row.payload)
		if status != http.StatusCreated {
			t.Fatalf("create %s: status %d body %s, want %d", row.title, status, body, http.StatusCreated)
		}
		ids[row.title] = decodeCreatedRowID(t, body)
	}
	rows := func(titles ...string) []string {
		out := make([]string, 0, len(titles))
		for _, title := range titles {
			out = append(out, ids[title])
		}
		sort.Strings(out)
		return out
	}

	accepted := []struct {
		condition string
		want      []string
	}{
		{`{"a":"note","v":"in:[\"pending\",\"failed\"]"}`, rows("pending", "failed")},
		{`{"a":"note","v":"not_in:[\"published\"]"}`, rows("pending", "failed")},
		{`{"a":"note","v":"is_null:true"}`, rows("none")},
		{`{"a":"note","v":"is_not_null:true"}`, rows("pending", "failed", "published")},
		{`{"a":"seen","v":"lte:$now"}`, rows("pending", "failed")},
	}
	for _, tc := range accepted {
		status, body := postAdvancedQuery(t, srv.URL, wide.Name, tc.condition, "")
		if status != http.StatusOK {
			t.Errorf("%s: status %d body %s, want %d", tc.condition, status, body, http.StatusOK)
			continue
		}
		if got := decodeQueryRowIDs(t, body); fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: rows %v, want %v", tc.condition, got, tc.want)
		}
	}

	refused := []struct {
		condition, federated, want string
	}{
		{`{"a":"note","v":"in:[]"}`, "",
			"operator 'in' on attribute 'note' takes a JSON array of 1 to 1000 values; the array is empty"},
		{`{"a":"seen","v":"lte:$now"}`, `{"enabled":true}`,
			"the $now operand on datetime attribute 'seen' is not supported on a federated query"},
	}
	for _, tc := range refused {
		status, body := postAdvancedQuery(t, srv.URL, wide.Name, tc.condition, tc.federated)
		if status != http.StatusBadRequest || !strings.Contains(decodeErrorMessage(t, body), tc.want) {
			t.Errorf("%s (federated=%q): status %d body %s, want %d naming %q",
				tc.condition, tc.federated, status, body, http.StatusBadRequest, tc.want)
		}
	}
}

func postAdvancedQuery(t *testing.T, baseURL, schema, condition, federated string) (int, string) {
	t.Helper()
	payload := fmt.Sprintf(`{"schema_name":%q,"condition":%s,"page":1,"items_per_page":100`, schema, condition)
	if federated != "" {
		payload += `,"federated":` + federated
	}
	payload += "}"
	resp, err := http.Post(baseURL+"/api/v1/advanced_query", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("advanced query request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read advanced query response: %v", err)
	}
	return resp.StatusCode, string(body)
}

func decodeQueryRowIDs(t *testing.T, body string) []string {
	t.Helper()
	var result struct {
		Data []struct {
			RowID string `json:"row_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("decode advanced query response %s: %v", body, err)
	}
	ids := make([]string, 0, len(result.Data))
	for _, r := range result.Data {
		ids = append(ids, r.RowID)
	}
	sort.Strings(ids)
	return ids
}

func decodeErrorMessage(t *testing.T, body string) string {
	t.Helper()
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decode error response %s: %v", body, err)
	}
	return resp.Error
}
