package httpapi

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var dateResponseRowID = uuid.MustParse("01890000-0000-7000-8000-000000000591")

// dateResponseRecord carries the dates the read path returns for a
// bigint-bound date at both ends of int64 and the first year past 9999, next
// to one inside 0000 to 9999, at the top level and nested.
func dateResponseRecord() *forma.DataRecord {
	return &forma.DataRecord{
		SchemaName: "lead",
		RowID:      dateResponseRowID,
		Attributes: map[string]any{
			"max":     time.UnixMilli(math.MaxInt64).UTC(),
			"min":     time.UnixMilli(math.MinInt64).UTC(),
			"y10000":  time.UnixMilli(253402300800000).UTC(),
			"inRange": time.UnixMilli(1704164645123).UTC(),
			"nested":  map[string]any{"list": []any{time.UnixMilli(math.MaxInt64).UTC()}},
		},
	}
}

// dateResponseWant is dateResponseRecord's attributes as a client reads them:
// the exact epoch milliseconds where RFC3339 has no four-digit year, RFC3339
// otherwise.
var dateResponseWant = map[string]any{
	"max":     "9223372036854775807",
	"min":     "-9223372036854775808",
	"y10000":  "253402300800000",
	"inRange": "2024-01-02T03:04:05.123Z",
	"nested":  map[string]any{"list": []any{"9223372036854775807"}},
}

// dateResponseRoute is one success route and where its body carries records.
type dateResponseRoute struct {
	name    string
	manager func(record *forma.DataRecord) *mockEntityManager
	method  string
	target  string
	body    string
	status  int
	// records names the body's record list ("data", "successful"), or ""
	// for a body that is the record itself.
	records string
}

var dateResponseRoutes = []dateResponseRoute{
	{"create single", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{batchCreateResult: &forma.BatchResult{Successful: []*forma.DataRecord{r}, TotalCount: 1}}
	}, http.MethodPost, "/api/v1/lead", `{"title":"x"}`, http.StatusCreated, ""},
	{"create array", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{batchCreateResult: &forma.BatchResult{Successful: []*forma.DataRecord{r}, TotalCount: 1}}
	}, http.MethodPost, "/api/v1/lead", `[{"title":"x"}]`, http.StatusCreated, "successful"},
	{"get", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{getResult: r}
	}, http.MethodGet, "/api/v1/lead/" + dateResponseRowID.String(), "", http.StatusOK, ""},
	{"update", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{updateResult: r}
	}, http.MethodPut, "/api/v1/lead/" + dateResponseRowID.String(), `{"title":"y"}`, http.StatusOK, ""},
	{"query", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{r}, TotalRecords: 1}}
	}, http.MethodGet, "/api/v1/lead", "", http.StatusOK, "data"},
	{"advanced query", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{advancedResult: &forma.QueryResult{Data: []*forma.DataRecord{r}, TotalRecords: 1}}
	}, http.MethodPost, "/api/v1/advanced_query", `{"schema_name":"lead","condition":{"a":"status","v":"equals:hot"}}`, http.StatusOK, "data"},
	{"search", func(r *forma.DataRecord) *mockEntityManager {
		return &mockEntityManager{crossSchemaResult: &forma.QueryResult{Data: []*forma.DataRecord{r}, TotalRecords: 1}}
	}, http.MethodGet, "/api/v1/search?schemas=lead&q=x", "", http.StatusOK, "data"},
}

func serveDateResponseRoute(t *testing.T, route dateResponseRoute, record *forma.DataRecord) *httptest.ResponseRecorder {
	t.Helper()
	server := NewServer(route.manager(record), Options{})
	req := httptest.NewRequest(route.method, route.target, strings.NewReader(route.body))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	return rec
}

// TestResponsesRenderDatesOutsideRFC3339Years is #591's HTTP regression: a
// date bound to a bigint column keeps the full int64 epoch-ms range, and every
// response carrying one failed to encode after its 2xx had been written, so
// the client read a committed status with a truncated body. Every route that
// answers with records now answers well-formed JSON carrying the exact value.
func TestResponsesRenderDatesOutsideRFC3339Years(t *testing.T) {
	for _, route := range dateResponseRoutes {
		t.Run(route.name, func(t *testing.T) {
			rec := serveDateResponseRoute(t, route, dateResponseRecord())
			if rec.Code != route.status {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, route.status, rec.Body.String())
			}
			attributes := decodeResponseAttributes(t, rec.Body.Bytes(), route.records)
			for name, want := range dateResponseWant {
				got, _ := json.Marshal(attributes[name])
				wantJSON, _ := json.Marshal(want)
				if string(got) != string(wantJSON) {
					t.Errorf("attribute %s = %s, want %s", name, got, wantJSON)
				}
			}
		})
	}
}

// decodeResponseAttributes returns the first record's attributes from a
// response body, failing the test if the body is not one well-formed JSON
// value.
func decodeResponseAttributes(t *testing.T, body []byte, records string) map[string]any {
	t.Helper()
	if !json.Valid(body) {
		t.Fatalf("response body is not well-formed JSON: %q", body)
	}
	type record struct {
		Attributes map[string]any `json:"attributes"`
	}
	if records == "" {
		var single record
		if err := json.Unmarshal(body, &single); err != nil {
			t.Fatalf("decode record body %s: %v", body, err)
		}
		return single.Attributes
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode response body %s: %v", body, err)
	}
	var list []record
	if err := json.Unmarshal(envelope[records], &list); err != nil || len(list) != 1 {
		t.Fatalf("decode %s of %s: %v (%d records)", records, body, err, len(list))
	}
	return list[0].Attributes
}

// TestUnencodableSuccessBodyAnswersRedacted500: a success body that fails to
// encode must not commit its 2xx first. Before #591 writeJSON wrote the status
// and then streamed the encoder into the response, so the client read 201 or
// 200 with a truncated body. The body is now encoded before the status, and a
// failure is a read-side consistency error: the redacted 500 with an
// error_id. A date no int64 of epoch milliseconds names is the one value the
// record renderer refuses; the read path cannot produce it, so it stands in
// for any body that fails to encode.
func TestUnencodableSuccessBodyAnswersRedacted500(t *testing.T) {
	unencodable := dateResponseRecord()
	unencodable.Attributes["beyond"] = time.UnixMilli(math.MaxInt64).Add(time.Millisecond)

	for _, route := range dateResponseRoutes {
		t.Run(route.name, func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			restore := zap.ReplaceGlobals(zap.New(core))
			defer restore()

			rec := serveDateResponseRoute(t, route, unencodable)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
			}
			var resp APIResponse
			dec := json.NewDecoder(rec.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&resp); err != nil {
				t.Fatalf("500 body is not one redacted APIResponse: %v", err)
			}
			if dec.More() {
				t.Fatalf("500 body carries more than one JSON value")
			}
			if resp.Success || resp.Error != "internal error" || resp.ErrorClass != errorClassInternal || resp.ErrorID == "" {
				t.Fatalf("500 body = %+v, want the redacted internal error with an error_id", resp)
			}
			assertUnencodableLogged(t, logs.AllUntimed(), resp.ErrorID, route.status)
		})
	}
}

// assertUnencodableLogged checks the operator's copy of an unencodable
// success body: one Errorw line carrying the body's error_id, the status the
// success would have answered (a create's write has committed) and the
// encoder's reason.
func assertUnencodableLogged(t *testing.T, entries []observer.LoggedEntry, errorID string, intended int) {
	t.Helper()
	if len(entries) != 1 || entries[0].Message != "success response not encodable" {
		t.Fatalf("logged %+v, want one Errorw line \"success response not encodable\"", entries)
	}
	fields := entries[0].ContextMap()
	if fields["error_id"] != errorID {
		t.Errorf("logged error_id %v, want the body's %s", fields["error_id"], errorID)
	}
	if fields["intended_status"] != int64(intended) {
		t.Errorf("logged intended_status %v (%T), want %d", fields["intended_status"], fields["intended_status"], intended)
	}
	if reason, _ := fields["error"].(string); !strings.Contains(reason, "cannot be rendered as epoch milliseconds") {
		t.Errorf("logged error %q does not carry the encoder's reason", reason)
	}
}
