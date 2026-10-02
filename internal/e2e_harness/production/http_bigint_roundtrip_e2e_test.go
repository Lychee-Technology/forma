//go:build e2e

package production

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
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
		"-9223372036854775808", // MinInt64: admitted, unlike the integers just below it
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

// TestHTTPBigintOutsideInt64Refused is the #617 review F1 acceptance probe:
// an integer outside int64 sent to the bound bigint (`amount`, bigint_01) is
// a 400 naming the literal, on create and on update, and the refused update
// leaves the stored row as it was. Below MinInt64 the literal's float64 image
// is -2^63, which is MinInt64 exactly, and the funnel used to store that in
// its place: 201 on create, 200 on update, MinInt64 read back.
func TestHTTPBigintOutsideInt64Refused(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	srv := httptest.NewServer(httpapi.NewServer(env.EntityManager(), httpapi.Options{}).Handler())
	defer srv.Close()

	status, body := postWide(t, srv.URL, wide.Name, `{"title":"http-bigint-outside","amount":5}`)
	if status != http.StatusCreated {
		t.Fatalf("create seed: status %d body %s, want %d", status, body, http.StatusCreated)
	}
	rowID := uuid.MustParse(decodeCreatedRowID(t, body))

	for _, lit := range []string{"-9223372036854775809", "-9.223372036854775809e18", "9223372036854775808"} {
		want := "value " + lit + " out of range for declared type bigint (allowed [-9223372036854775808, 9223372036854775807])"
		status, body := postWide(t, srv.URL, wide.Name, fmt.Sprintf(`{"title":"http-bigint-%s","amount":%s}`, lit, lit))
		if status != http.StatusBadRequest || !strings.Contains(body, want) {
			t.Errorf("create amount=%s: status %d body %s, want %d naming %q", lit, status, body, http.StatusBadRequest, want)
		}
		status, body = putWide(t, srv.URL, wide.Name, rowID, fmt.Sprintf(`{"amount":%s}`, lit))
		if status != http.StatusBadRequest || !strings.Contains(body, want) {
			t.Errorf("update amount=%s: status %d body %s, want %d naming %q", lit, status, body, http.StatusBadRequest, want)
		}
	}

	got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: wide.Name, RowID: &rowID})
	if err != nil {
		t.Fatalf("get seed row: %v", err)
	}
	if amount, _ := got.Attributes["amount"].(int64); amount != 5 {
		t.Fatalf("amount after the refused updates = %v (%T), want int64 5", got.Attributes["amount"], got.Attributes["amount"])
	}
}
