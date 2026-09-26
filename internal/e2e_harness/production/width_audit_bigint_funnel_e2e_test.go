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
// reports, while the OLTP read still returns the value the table holds.
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
	} {
		status, body := postWideTotal(t, srv.URL, wide.Name, tc.lit)
		if status != http.StatusBadRequest || !strings.Contains(body, "float64 image "+tc.image) ||
			!strings.Contains(body, "allowed [-9007199254740992, 9007199254740992]") {
			t.Fatalf("create total=%s: status %d body %s, want %d naming float64 image %s and the allowed range",
				tc.lit, status, body, http.StatusBadRequest, tc.image)
		}
	}
	accepted := map[string]string{}
	for _, lit := range []string{"9007199254740992", "-9007199254740992", "9007199254740991"} {
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
	payload := fmt.Sprintf(`{"title":"width-audit-bigint-%s","total":%s}`, literal, literal)
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
