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

	"github.com/lychee-technology/forma/internal/httpapi"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// TestIntegerWidthAuditAgreesWithBigintWriteFunnel pins the #612 review
// finding: eav_data keeps only the float64 image of an EAV-only bigint, and
// every int64 from 2^63-512 up has the image 2^63. The funnel used to admit
// such a value through the exact sidecar, and the census then reported the
// stored row as a bigint out of contract. The HTTP surface decodes with
// UseNumber, so the exact literal reaches the funnel; what it accepts, the
// census must never flag.
func TestIntegerWidthAuditAgreesWithBigintWriteFunnel(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()

	// total (attr 15) is the EAV-only bigint.
	for _, lit := range []string{"9223372036854775807", "9223372036854775296"} {
		status, body := postWideTotal(t, srv.URL, wide.Name, lit)
		if status != http.StatusBadRequest || !strings.Contains(body, "float64 image 9223372036854775808") {
			t.Fatalf("create total=%s: status %d body %s, want %d naming the float64 image",
				lit, status, body, http.StatusBadRequest)
		}
	}
	status, body := postWideTotal(t, srv.URL, wide.Name, "9223372036854775295")
	if status != http.StatusCreated {
		t.Fatalf("create total=9223372036854775295: status %d body %s, want %d", status, body, http.StatusCreated)
	}
	var created struct {
		RowID string `json:"row_id"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil || created.RowID == "" {
		t.Fatalf("decode create response %s: %v", body, err)
	}

	tables := widthaudit.Tables{EAV: env.Tables.EAVData, ChangeLog: env.Tables.ChangeLog}
	findings, err := widthaudit.Census(ctx, env.Pool, tables, widthaudit.Targets(env.Metadata))
	if err != nil {
		t.Fatalf("census: %v", err)
	}
	for _, f := range findings {
		if f.RowID.String() == created.RowID {
			t.Fatalf("census flags the row the funnel accepted: %+v", f)
		}
	}
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
