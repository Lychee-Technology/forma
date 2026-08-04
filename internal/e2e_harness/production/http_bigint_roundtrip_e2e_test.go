//go:build e2e

package production

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lychee-technology/forma/internal/httpapi"
)

// TestHTTPBigintRoundTripAbove2p53 is #282's acceptance probe: an integer
// payload above 2^53 must survive the full HTTP surface — UseNumber decode,
// JSON-schema validation, the exact-int64 transform sidecar, bound-column
// storage — and read back over HTTP as the same literal. Pre-#284 the default
// decoder rounded it at the door regardless of #205's exact write path.
func TestHTTPBigintRoundTripAbove2p53(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()

	literals := []string{
		"9007199254740993",     // 2^53+1: first integer float64 cannot represent
		"9223372036854775807",  // MaxInt64
		"-9223372036854775807", // -MaxInt64 (pre-#205 rounded to MinInt64)
	}
	for _, lit := range literals {
		t.Run(lit, func(t *testing.T) {
			body := fmt.Sprintf(`{"title":"http-bigint-%s","amount":%s}`, lit, lit)
			resp, err := http.Post(srv.URL+"/api/v1/"+wide.Name, "application/json", bytes.NewReader([]byte(body)))
			if err != nil {
				t.Fatalf("create request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("create status = %d, want %d", resp.StatusCode, http.StatusCreated)
			}
			var created struct {
				RowID string `json:"row_id"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
				t.Fatalf("decode create response: %v", err)
			}
			if created.RowID == "" {
				t.Fatal("create response carries no row_id")
			}

			getResp, err := http.Get(srv.URL + "/api/v1/" + wide.Name + "/" + created.RowID)
			if err != nil {
				t.Fatalf("get request: %v", err)
			}
			defer getResp.Body.Close()
			if getResp.StatusCode != http.StatusOK {
				t.Fatalf("get status = %d, want %d", getResp.StatusCode, http.StatusOK)
			}
			var record struct {
				Attributes map[string]any `json:"attributes"`
			}
			dec := json.NewDecoder(getResp.Body)
			dec.UseNumber()
			if err := dec.Decode(&record); err != nil {
				t.Fatalf("decode get response: %v", err)
			}
			got, ok := record.Attributes["amount"]
			if !ok {
				t.Fatalf("get response missing amount attribute: %v", record.Attributes)
			}
			if want := json.Number(lit); got != want {
				t.Fatalf("amount round-tripped as %#v (%T), want %#v", got, got, want)
			}
		})
	}
}
