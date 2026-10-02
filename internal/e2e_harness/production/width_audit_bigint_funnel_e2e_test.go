//go:build e2e

package production

import (
	"context"
	"encoding/json"
	"fmt"
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

// TestIntegerWidthAuditAgreesWithBigintWriteFunnel pins the #590 contract
// on the live HTTP surface: eav_data keeps only the float64 image of an
// EAV-only bigint, which is exact within +-2^53, so the funnel admits exactly
// that range (the #612 review had refused only the slice from 2^63-512 up,
// whose image is 2^63). The HTTP surface decodes with UseNumber, so the exact
// literal reaches the funnel. What it accepts, the census must never flag;
// what it refuses is a 400 that names the value's float64 image; and a
// stored image past 2^53 (a row from before this change) is what the census
// reports, while the OLTP read still returns the value the table holds. The
// funnel judges the literal, not its image: a fraction whose image is the
// whole 2^53 is refused as non-integral, and an exponent spelling of 2^53+1
// is refused as that value (#590 review).
func TestIntegerWidthAuditAgreesWithBigintWriteFunnel(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()

	// total (attr 15) is the EAV-only bigint.
	for _, tc := range []struct{ lit, image string }{
		{"9007199254740993", "9007199254740992"},
		{"-9007199254740993", "-9007199254740992"},
		{"9223372036854775807", "9223372036854775808"},
		{"9223372036854775296", "9223372036854775808"},
		{"1000000000000000000", "1000000000000000000"},
		{"9.007199254740993e15", "9007199254740992"},
	} {
		status, body := postWideTotal(t, srv.URL, wide.Name, tc.lit)
		if status != http.StatusBadRequest || !strings.Contains(body, "float64 image "+tc.image) ||
			!strings.Contains(body, "allowed [-9007199254740992, 9007199254740992]") {
			t.Fatalf("create total=%s: status %d body %s, want %d naming float64 image %s and the allowed range",
				tc.lit, status, body, http.StatusBadRequest, tc.image)
		}
	}
	for _, lit := range []string{"9007199254740991.5", "-9007199254740991.5", "4503599627370496.5"} {
		status, body := postWideTotal(t, srv.URL, wide.Name, lit)
		want := "non-integral value " + lit + " does not fit declared type bigint"
		if status != http.StatusBadRequest || !strings.Contains(body, want) {
			t.Fatalf("create total=%s: status %d body %s, want %d naming %q", lit, status, body, http.StatusBadRequest, want)
		}
	}
	accepted := map[string]string{}
	for _, lit := range []string{"9007199254740992", "-9007199254740992", "9007199254740991", "9.007199254740992e15"} {
		status, body := postWideTotal(t, srv.URL, wide.Name, lit)
		if status != http.StatusCreated {
			t.Fatalf("create total=%s: status %d body %s, want %d", lit, status, body, http.StatusCreated)
		}
		accepted[decodeCreatedRowID(t, body)] = lit
	}

	// A legacy image past 2^53: written as an admitted value, then rewritten
	// underneath the funnel the way a pre-#590 write left it.
	status, body := postWideTotal(t, srv.URL, wide.Name, "1")
	if status != http.StatusCreated {
		t.Fatalf("create legacy seed: status %d body %s", status, body)
	}
	legacyRowID := decodeCreatedRowID(t, body)
	env.ExecSQL(ctx, "UPDATE "+env.Tables.EAVData+" SET value_numeric = 9007199254740994 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 15",
		wide.ID, uuid.MustParse(legacyRowID))

	tables := widthaudit.Tables{EAV: env.Tables.EAVData, ChangeLog: env.Tables.ChangeLog}
	findings, err := widthaudit.Census(ctx, env.Pool, tables, widthaudit.Targets(env.Metadata))
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	flaggedLegacy := false
	for _, f := range findings {
		if lit, ok := accepted[f.RowID.String()]; ok {
			t.Fatalf("census flags the row the funnel accepted (total=%s): %+v", lit, f)
		}
		if f.RowID.String() == legacyRowID {
			flaggedLegacy = true
			if class := f.Classify(0); class != widthaudit.ClassBigIntOutOfContract {
				t.Fatalf("legacy image classified %v, want ClassBigIntOutOfContract: %+v", class, f)
			}
			if f.StoredValue != "9007199254740994" {
				t.Fatalf("legacy finding stored value %q, want 9007199254740994", f.StoredValue)
			}
		}
	}
	if !flaggedLegacy {
		t.Fatalf("census did not report the legacy image past 2^53: %+v", findings)
	}

	// The OLTP read returns the legacy image as the value the table holds.
	legacyUUID := uuid.MustParse(legacyRowID)
	got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: wide.Name, RowID: &legacyUUID})
	if err != nil {
		t.Fatalf("get legacy row: %v", err)
	}
	if total, _ := got.Attributes["total"].(int64); total != 9007199254740994 {
		t.Fatalf("legacy row total = %v (%T), want int64 9007199254740994", got.Attributes["total"], got.Attributes["total"])
	}
}

// TestBigintLegacyImageRepairByUpdate pins the repair the migration guide
// prescribes for a stored image the #590 funnel refuses (#590 review): a PUT
// that names the attribute rewrites it whatever the image, because the update
// never decodes a stored value it replaces, and the census then reports the
// row clean. A PUT that does not name it carries the image into the write: a
// whole image past 2^53 is refused as a 400 naming it, and one the read
// cannot decode (a fraction, a number past int64) fails as a server error.
func TestBigintLegacyImageRepairByUpdate(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()
	tables := widthaudit.Tables{EAV: env.Tables.EAVData, ChangeLog: env.Tables.ChangeLog}

	for _, image := range []string{"9007199254740994", "1000.5", "9223372036854775808"} {
		status, body := postWideTotal(t, srv.URL, wide.Name, "1")
		if status != http.StatusCreated {
			t.Fatalf("create seed for image %s: status %d body %s", image, status, body)
		}
		rowID := uuid.MustParse(decodeCreatedRowID(t, body))
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EAVData+" SET value_numeric = "+image+" WHERE schema_id = $1 AND row_id = $2 AND attr_id = 15",
			wide.ID, rowID)
		if !censusFlagsRow(t, ctx, env, tables, rowID) {
			t.Fatalf("census does not report the planted image %s", image)
		}

		status, body = putWide(t, srv.URL, wide.Name, rowID, `{"title":"unrelated"}`)
		if image == "9007199254740994" {
			if status != http.StatusBadRequest || !strings.Contains(body, "total") || !strings.Contains(body, "float64 image "+image) {
				t.Fatalf("PUT title over image %s: status %d body %s, want %d naming total and the image", image, status, body, http.StatusBadRequest)
			}
		} else if status < http.StatusInternalServerError {
			t.Fatalf("PUT title over unreadable image %s: status %d body %s, want a server error", image, status, body)
		}

		status, body = putWide(t, srv.URL, wide.Name, rowID, `{"total":5}`)
		if status != http.StatusOK {
			t.Fatalf("PUT total over image %s: status %d body %s, want %d", image, status, body, http.StatusOK)
		}
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: wide.Name, RowID: &rowID})
		if err != nil {
			t.Fatalf("get repaired row (image %s): %v", image, err)
		}
		if total, _ := got.Attributes["total"].(int64); total != 5 {
			t.Fatalf("repaired total = %v (%T), want int64 5", got.Attributes["total"], got.Attributes["total"])
		}
		if censusFlagsRow(t, ctx, env, tables, rowID) {
			t.Fatalf("census still reports the row repaired over image %s", image)
		}
	}
}

func censusFlagsRow(t *testing.T, ctx context.Context, env *Env, tables widthaudit.Tables, rowID uuid.UUID) bool {
	t.Helper()
	findings, err := widthaudit.Census(ctx, env.Pool, tables, widthaudit.Targets(env.Metadata))
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	for _, f := range findings {
		if f.RowID == rowID {
			return true
		}
	}
	return false
}

func putWide(t *testing.T, baseURL, schema string, rowID uuid.UUID, payload string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, baseURL+"/api/v1/"+schema+"/"+rowID.String(), strings.NewReader(payload))
	if err != nil {
		t.Fatalf("build update request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("update request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read update response: %v", err)
	}
	return resp.StatusCode, string(body)
}

func decodeCreatedRowID(t *testing.T, body string) string {
	t.Helper()
	var created struct {
		RowID string `json:"row_id"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil || created.RowID == "" {
		t.Fatalf("decode create response %s: %v", body, err)
	}
	return created.RowID
}

func postWideTotal(t *testing.T, baseURL, schema, literal string) (int, string) {
	t.Helper()
	return postWide(t, baseURL, schema, fmt.Sprintf(`{"title":"width-audit-bigint-%s","total":%s}`, literal, literal))
}

func postWide(t *testing.T, baseURL, schema, payload string) (int, string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/api/v1/"+schema, "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read create response: %v", err)
	}
	return resp.StatusCode, string(body)
}
