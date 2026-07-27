package redact

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// The canary follows #301's construction (internal/httpapi/canaries_test.go):
// the value carries a ';' so a truncation regression leaves a distinct tail,
// and the value, head, and tail are asserted separately.
const (
	errCanaryHead = "SUPERSECRET"
	errCanaryTail = "CANARY-TAIL"
	errCanary     = errCanaryHead + ";" + errCanaryTail
)

func TestError_NilAndCleanErrorsPassThroughUnchanged(t *testing.T) {
	if got := Error(nil); got != nil {
		t.Fatalf("Error(nil) = %v, want nil", got)
	}
	clean := errors.New("IO Error: read_parquet failed on base/schema_7/x.parquet")
	if got := Error(clean); got != clean {
		t.Fatalf("clean error must be returned as the same instance, got %v", got)
	}
}

func TestError_ScrubsPasswordFromMessage(t *testing.T) {
	orig := fmt.Errorf(
		`IO Error: Unable to connect to Postgres at "host=h port=5432 user=u password='%s' dbname=d": connection refused`,
		errCanary)
	got := Error(orig)

	msg := got.Error()
	// Positive precondition: the scrub must not have eaten the diagnosis.
	if !strings.Contains(msg, "Unable to connect to Postgres") || !strings.Contains(msg, "host=h") {
		t.Fatalf("diagnosis prose must survive redaction: %q", msg)
	}
	if !strings.Contains(msg, "password="+Placeholder) {
		t.Fatalf("password value must be replaced by placeholder: %q", msg)
	}
	for _, frag := range []string{errCanary, errCanaryHead, errCanaryTail} {
		if strings.Contains(msg, frag) {
			t.Fatalf("fragment %q survived redaction: %q", frag, msg)
		}
	}
}

func TestError_PreservesWrappedChainIdentity(t *testing.T) {
	sentinel := errors.New("sentinel: federated read failed")
	orig := fmt.Errorf(`attach failed: "host=h user=u password='%s' dbname=d": %w`, errCanary, sentinel)
	got := Error(orig)

	if !errors.Is(got, sentinel) {
		t.Fatal("errors.Is must resolve the wrapped sentinel through the scrub wrapper")
	}
	if unwrapped := errors.Unwrap(got); unwrapped != orig {
		t.Fatalf("Unwrap must return the original error, got %v", unwrapped)
	}
}
