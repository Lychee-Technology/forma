package forma

import (
	"errors"
	"fmt"
	"testing"
)

// resolvePublic mirrors the boundary's resolution: the outermost PublicError in
// preorder left-first DFS wins.
func resolvePublic(t *testing.T, err error) (string, bool) {
	t.Helper()
	var pub PublicError
	if !errors.As(err, &pub) {
		return "", false
	}
	return pub.PublicMessage(), true
}

func mustPublish(t *testing.T, err error, want string) {
	t.Helper()
	got, ok := resolvePublic(t, err)
	if !ok {
		t.Fatalf("expected a PublicError in chain of %v, found none", err)
	}
	if got != want {
		t.Fatalf("PublicMessage() = %q, want %q", got, want)
	}
}

func TestConstructorsRenderSentinelSuffixedError(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		sentinel error
		wantErr  string
		wantPub  string
	}{
		{"invalid input", InvalidInputf("schema name is required"), ErrInvalidInput,
			"schema name is required: invalid input", "schema name is required"},
		{"not found", NotFoundf("schema not found: %s", "lead"), ErrNotFound,
			"schema not found: lead: not found", "schema not found: lead"},
		{"conflict", Conflictf("the write conflicts with a row that already exists"), ErrConflict,
			"the write conflicts with a row that already exists: conflict",
			"the write conflicts with a row that already exists"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.wantErr {
				t.Fatalf("Error() = %q, want %q", got, tc.wantErr)
			}
			if !errors.Is(tc.err, tc.sentinel) {
				t.Fatalf("errors.Is(err, sentinel) = false, want true")
			}
			mustPublish(t, tc.err, tc.wantPub)
		})
	}
}

func TestConstructorMatchesOnlyItsOwnSentinel(t *testing.T) {
	err := InvalidInputf("x")
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
		t.Fatalf("InvalidInputf matched a foreign sentinel")
	}
}

func TestWithOperatorDetailWithholdsDetailFromPublication(t *testing.T) {
	detail := errors.New(`IO Error: reading base/schema_22/CANARY-KEY.parquet`)
	err := WithOperatorDetail(NotFoundf("schema not found"), fmt.Errorf("schema id %d", 402))

	if want := "schema not found: not found: schema id 402"; err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
	mustPublish(t, err, "schema not found")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("detail attachment broke errors.Is on the sentinel")
	}
	if !HasOperatorDetail(err) {
		t.Fatalf("HasOperatorDetail = false after WithOperatorDetail")
	}

	err = WithOperatorDetail(InvalidInputf("bad template"), detail)
	if msg, _ := resolvePublic(t, err); msg != "bad template" {
		t.Fatalf("operator detail leaked into publication: %q", msg)
	}
}

func TestWithOperatorDetailKeepsDetailInChain(t *testing.T) {
	sentinelDetail := fmt.Errorf("cause: %w", ErrNoParquetPaths)
	err := WithOperatorDetail(InvalidInputf("bad filter"), sentinelDetail)
	if !errors.Is(err, ErrNoParquetPaths) {
		t.Fatalf("detail left the chain: errors.Is cannot see it")
	}
}

func TestWithOperatorDetailDegenerateInputs(t *testing.T) {
	carrier := InvalidInputf("x")
	if got := WithOperatorDetail(carrier, nil); got != carrier {
		t.Fatalf("nil detail must return err unchanged")
	}
	plain := errors.New("operator error")
	if got := WithOperatorDetail(plain, errors.New("d")); got != plain {
		t.Fatalf("an error without a PublicError must pass through unchanged")
	}
	if got := WithOperatorDetail(nil, errors.New("d")); got != nil {
		t.Fatalf("nil err must return nil")
	}
	if HasOperatorDetail(carrier) {
		t.Fatalf("HasOperatorDetail = true for a carrier without detail")
	}
	if HasOperatorDetail(nil) {
		t.Fatalf("HasOperatorDetail = true for nil")
	}
}

func TestWrapPublicfPrefixesBothMessages(t *testing.T) {
	leaf := InvalidInputf("row id is required for update operation")
	err := WrapPublicf(leaf, "operation[%d]", 3)

	wantErr := "operation[3]: row id is required for update operation: invalid input"
	if err.Error() != wantErr {
		t.Fatalf("Error() = %q, want %q", err.Error(), wantErr)
	}
	mustPublish(t, err, "operation[3]: row id is required for update operation")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("wrap broke errors.Is on the sentinel")
	}
}

func TestWrapPublicfDegradesToPlainWrapForOperatorErrors(t *testing.T) {
	opErr := errors.New("load record: connection refused")
	err := WrapPublicf(opErr, "operation[%d]: failed to load record", 1)

	want := fmt.Errorf("operation[1]: failed to load record: %w", opErr)
	if err.Error() != want.Error() {
		t.Fatalf("Error() = %q, want %q", err.Error(), want.Error())
	}
	if _, ok := resolvePublic(t, err); ok {
		t.Fatalf("an operator error gained a publication by being wrapped")
	}
	if !errors.Is(err, opErr) {
		t.Fatalf("degraded wrap must still unwrap to the cause")
	}
}

func TestWrapPublicfNilReturnsNil(t *testing.T) {
	if got := WrapPublicf(nil, "prefix"); got != nil {
		t.Fatalf("WrapPublicf(nil) = %v, want nil", got)
	}
}

func TestPublicationSurvivesReWrapping(t *testing.T) {
	leaf := InvalidInputf("invalid value for attribute 'age' (attrID=2): cannot convert string to float64")
	err := WrapPublicf(leaf, "operation[0]: failed to transform data to persistent record")
	err = fmt.Errorf("batch create: %w", err)
	err = fmt.Errorf("manager: %w", err)

	mustPublish(t, err,
		"operation[0]: failed to transform data to persistent record: "+
			"invalid value for attribute 'age' (attrID=2): cannot convert string to float64")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("re-wrapping broke errors.Is")
	}
}

// The boundary resolves the first PublicError in preorder left-first DFS order.
// These cases pin the multi-cause semantics the conversion relies on.
func TestResolutionOrderOnMultiCauseChains(t *testing.T) {
	opErr := errors.New(`template: s3path:1:2: function "Bad" not defined`)

	t.Run("carrier joined before an operator cause wins", func(t *testing.T) {
		err := fmt.Errorf("render: %w: %w", InvalidInputf("the template is not renderable"), opErr)
		mustPublish(t, err, "the template is not renderable")
	})

	t.Run("carrier behind an operator cause is still found", func(t *testing.T) {
		err := errors.Join(opErr, InvalidInputf("bad filter"))
		mustPublish(t, err, "bad filter")
	})

	t.Run("first of two joined carriers wins", func(t *testing.T) {
		err := errors.Join(InvalidInputf("first"), InvalidInputf("second"))
		mustPublish(t, err, "first")
	})

	t.Run("outermost wrap wins over the leaf", func(t *testing.T) {
		err := WrapPublicf(InvalidInputf("leaf"), "outer")
		mustPublish(t, err, "outer: leaf")
	})
}
