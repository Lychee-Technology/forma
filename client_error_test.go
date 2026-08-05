package forma

import (
	"errors"
	"fmt"
	"testing"
)

// resolvePublic is the boundary's resolution: the canonical
// branch-provenance-bound walk, not a raw errors.As — the raw form finds a
// PublicError anywhere in the tree, which is exactly the borrow the resolver
// exists to reject (#362/#363 reviews).
func resolvePublic(t *testing.T, err error) (string, bool) {
	t.Helper()
	return ResolvePublicMessage(err)
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

// foreignPublication stands in for a type outside this module that satisfies
// PublicError without being built by the constructors — no guarantee its text
// was authored for a caller.
type foreignPublication struct{ msg string }

func (f *foreignPublication) Error() string         { return f.msg }
func (f *foreignPublication) PublicMessage() string { return f.msg }

// The decorators must not adopt a publication the branch-provenance rule
// would reject (#363 review, P1): wrapping a mixed tree whose sentinel branch
// is bare and whose publication branch is foreign must degrade exactly as the
// unwrapped tree would — no publication.
func TestDecoratorsDoNotAdoptForeignPublications(t *testing.T) {
	mixed := errors.Join(
		fmt.Errorf("bad input: %w", ErrInvalidInput),
		&foreignPublication{msg: "manifests/lead/22.json"})

	t.Run("WrapPublicf degrades to a plain wrap", func(t *testing.T) {
		err := WrapPublicf(mixed, "operation[%d]", 0)
		if _, ok := resolvePublic(t, err); ok {
			t.Fatalf("a wrap over a foreign publication still published: %v", err)
		}
		want := fmt.Errorf("operation[0]: %w", mixed)
		if err.Error() != want.Error() {
			t.Fatalf("Error() = %q, want plain-wrap %q", err.Error(), want.Error())
		}
	})

	t.Run("WithOperatorDetail passes through unchanged", func(t *testing.T) {
		if got := WithOperatorDetail(mixed, errors.New("detail")); got != mixed {
			t.Fatalf("an error without a qualifying publication must pass through, got %v", got)
		}
	})

	t.Run("a real carrier in the mixed tree still qualifies", func(t *testing.T) {
		legit := errors.Join(&foreignPublication{msg: "manifests/lead/22.json"},
			InvalidInputf("bad filter"))
		err := WrapPublicf(legit, "operation[%d]", 0)
		mustPublish(t, err, "operation[0]: bad filter")
	})
}

// asProvidedCarrier exposes a PublicError through the As(any) bool protocol
// rather than by direct implementation — the other half of errors.As node
// matching, which the resolver must honour (#363 review, P2).
type asProvidedCarrier struct{ inner error }

func (a *asProvidedCarrier) Error() string { return "as-provided: " + a.inner.Error() }
func (a *asProvidedCarrier) Unwrap() error { return a.inner }

func (a *asProvidedCarrier) As(target any) bool {
	if p, ok := target.(*PublicError); ok {
		*p = &foreignPublication{msg: "published via As"}
		return true
	}
	return false
}

func TestAsProvidedPublicationResolves(t *testing.T) {
	// The node's own subtree carries the sentinel, so it qualifies under the
	// branch-provenance rule even though the PublicError arrives via As.
	err := fmt.Errorf("ctx: %w", &asProvidedCarrier{inner: InvalidInputf("leaf")})
	if msg, ok := ResolvePublicMessage(err); !ok || msg != "published via As" {
		t.Fatalf("As-provided publication not resolved: msg=%q ok=%v", msg, ok)
	}

	// Without a sentinel in its subtree the same node must not publish.
	bare := fmt.Errorf("ctx: %w", &asProvidedCarrier{inner: errors.New("operator")})
	if msg, ok := ResolvePublicMessage(bare); ok {
		t.Fatalf("sentinel-less As-provided node published %q", msg)
	}
}
