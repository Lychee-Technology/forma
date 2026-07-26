//go:build e2e

package production

import (
	"context"
	"errors"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
)

// TestEntityManagerEnforcesJSONSchema is the harness's proof that its
// EntityManager carries a live JSON Schema validator (#314).
//
// It exists because the seam is silent when broken. The harness used to pass
// nil for the validator, which switches write-path validation off entirely —
// so every fixture payload in this package was vacuously valid and the whole
// suite asserted nothing about the constraints production enforces. Nothing
// went red; the coverage simply was not there.
//
// e2e_wide declares "rank": {"minimum": -32768, "maximum": 32767}. 40000
// violates the maximum. The DB column would reject it too, which is why the
// assertion is on the message rather than only on failure: "schema validation
// failed" is produced by schemavalidate.Validate and by nothing else, so it
// proves the rejection happened before the write rather than inside it.
func TestEntityManagerEnforcesJSONSchema(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	err := env.ApplyEvents(ctx, CreateEvent(wide, map[string]any{
		"title": "over-the-declared-maximum",
		"rank":  40000,
	}))
	if err == nil {
		t.Fatalf("create with rank=40000 succeeded; e2e_wide declares maximum 32767, so the validator is not wired")
	}
	if !strings.Contains(err.Error(), "schema validation failed") {
		t.Fatalf("create failed without reaching the schema validator: %v", err)
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("schema violation must wrap forma.ErrInvalidInput so the HTTP boundary answers 4xx, got: %v", err)
	}
}
