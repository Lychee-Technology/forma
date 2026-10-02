//go:build e2e

package production

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
)

// dateRenderRows are e2e_wide rows whose dates lie outside the RFC3339 years
// 0000 to 9999, as the epoch-millisecond strings the write path accepts and
// the API renders them as (#591): touched (datetime) and joined (date) are
// bound to bigint_03/bigint_02 with unix_ms and keep the full int64, and
// seen/born are unbound, whose float64 image is exact at these magnitudes.
// The second row's seen is inside 9999 and keeps its RFC3339 rendering.
var dateRenderRows = []map[string]string{
	{
		"touched": "9223372036854775807",  // MaxInt64, year 292278994
		"joined":  "-9223372036854775808", // MinInt64, year -292275055
		"seen":    "253402300800000",      // 10000-01-01T00:00:00Z
	},
	{
		"touched": "253402300800000", // 10000-01-01T00:00:00Z
		"joined":  "-62167219200001", // the last millisecond of year -1
		"born":    "-62167219200001",
		"seen":    "2024-01-02T03:04:05.123Z",
	},
}

// TestHTTPDateOutsideRFC3339YearsRoundTrip is #591's acceptance probe: a
// bigint-bound date at MaxInt64, MinInt64 or year 10000 is stored exactly
// (#582), and every response that carried it failed to encode after its 201
// or 200 had been written, so the client read a committed status with a
// truncated body. The create echo, an unfiltered read by row id and an
// unfiltered list now answer well-formed JSON carrying the exact value, and
// sending that value back on an update stores the same instant.
func TestHTTPDateOutsideRFC3339YearsRoundTrip(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()

	rowIDs := make([]string, len(dateRenderRows))
	for i, dates := range dateRenderRows {
		payload := map[string]any{"title": fmt.Sprintf("http-date-render-%d", i)}
		for attr, literal := range dates {
			payload[attr] = literal
		}
		status, body := dateRenderDo(t, http.MethodPost, srv.URL+"/api/v1/"+wide.Name, payload)
		if status != http.StatusCreated {
			t.Fatalf("create row %d: status %d body %s, want %d", i, status, body, http.StatusCreated)
		}
		created := dateRenderDecodeRecord(t, "create", body)
		assertDateRenderAttrs(t, "create", created, dates)
		rowIDs[i] = created.RowID
	}

	for i, rowID := range rowIDs {
		status, body := dateRenderDo(t, http.MethodGet, srv.URL+"/api/v1/"+wide.Name+"/"+rowID, nil)
		if status != http.StatusOK {
			t.Fatalf("get row %d: status %d body %s, want %d", i, status, body, http.StatusOK)
		}
		assertDateRenderAttrs(t, "get", dateRenderDecodeRecord(t, "get", body), dateRenderRows[i])
	}

	status, body := dateRenderDo(t, http.MethodGet, srv.URL+"/api/v1/"+wide.Name+"?items_per_page=100", nil)
	if status != http.StatusOK {
		t.Fatalf("list: status %d body %s, want %d", status, body, http.StatusOK)
	}
	listed := dateRenderDecodeList(t, body)
	for i, rowID := range rowIDs {
		record, ok := listed[rowID]
		if !ok {
			t.Fatalf("list response is missing row %d (%s): %s", i, rowID, body)
		}
		assertDateRenderAttrs(t, "list", record, dateRenderRows[i])
	}

	assertDateRenderUpdateRoundTrip(t, env, srv.URL, wide.Name, rowIDs)
}

// assertDateRenderUpdateRoundTrip sends each row's dates back exactly as the
// API rendered them and checks the update answers them unchanged and the
// stored instants are the original epoch milliseconds.
func assertDateRenderUpdateRoundTrip(t *testing.T, env *Env, baseURL, schema string, rowIDs []string) {
	t.Helper()
	for i, rowID := range rowIDs {
		payload := map[string]any{}
		for attr, literal := range dateRenderRows[i] {
			payload[attr] = literal
		}
		status, body := dateRenderDo(t, http.MethodPut, baseURL+"/api/v1/"+schema+"/"+rowID, payload)
		if status != http.StatusOK {
			t.Fatalf("update row %d: status %d body %s, want %d", i, status, body, http.StatusOK)
		}
		assertDateRenderAttrs(t, "update", dateRenderDecodeRecord(t, "update", body), dateRenderRows[i])

		id := uuid.MustParse(rowID)
		got, err := env.EntityManager().Get(context.Background(), &forma.QueryRequest{SchemaName: schema, RowID: &id})
		if err != nil {
			t.Fatalf("get row %d after update: %v", i, err)
		}
		for attr, literal := range dateRenderRows[i] {
			want, err := strconv.ParseInt(literal, 10, 64)
			if err != nil {
				continue // an RFC3339 control value, compared over HTTP above
			}
			stored, ok := got.Attributes[attr].(time.Time)
			if !ok || stored.UnixMilli() != want {
				t.Errorf("row %d %s stored as %v (%T), want epoch ms %d", i, attr, got.Attributes[attr], got.Attributes[attr], want)
			}
		}
	}
}

type dateRenderRecord struct {
	RowID      string         `json:"row_id"`
	Attributes map[string]any `json:"attributes"`
}

func dateRenderDo(t *testing.T, method, url string, payload map[string]any) (int, string) {
	t.Helper()
	var reqBody io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("encode %s payload: %v", method, err)
		}
		reqBody = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		t.Fatalf("build %s request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s request: %v", method, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s response: %v", method, err)
	}
	return resp.StatusCode, string(body)
}

// dateRenderDecodeRecord decodes a body that must be exactly one well-formed
// record: a truncated body, the pre-#591 failure, does not decode.
func dateRenderDecodeRecord(t *testing.T, op, body string) dateRenderRecord {
	t.Helper()
	var record dateRenderRecord
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&record); err != nil || dec.More() || record.RowID == "" {
		t.Fatalf("%s response is not one well-formed record (err %v): %s", op, err, body)
	}
	return record
}

func dateRenderDecodeList(t *testing.T, body string) map[string]dateRenderRecord {
	t.Helper()
	var list struct {
		Data []dateRenderRecord `json:"data"`
	}
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&list); err != nil || dec.More() {
		t.Fatalf("list response is not one well-formed result (err %v): %s", err, body)
	}
	byRowID := make(map[string]dateRenderRecord, len(list.Data))
	for _, record := range list.Data {
		byRowID[record.RowID] = record
	}
	return byRowID
}

func assertDateRenderAttrs(t *testing.T, op string, record dateRenderRecord, want map[string]string) {
	t.Helper()
	for attr, literal := range want {
		if got := record.Attributes[attr]; got != literal {
			t.Errorf("%s row %s: %s rendered as %#v (%T), want %q", op, record.RowID, attr, got, got, literal)
		}
	}
}
