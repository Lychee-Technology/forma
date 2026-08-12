package internal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

// publicMessageOf resolves the deliberately published message the HTTP
// boundary would emit for err (#313), or fails the test if err publishes
// nothing.
func publicMessageOf(t *testing.T, err error) string {
	t.Helper()
	var pub forma.PublicError
	if !errors.As(err, &pub) {
		t.Fatalf("error publishes no client message: %v", err)
	}
	return pub.PublicMessage()
}

// The operation index is the piece of a batch error the caller cannot recover
// from anywhere else in the response; it must survive into the published
// message, not just into Error().
func TestBatchCreateAtomicPublishesOperationIndex(t *testing.T) {
	ctx := context.Background()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), nil, registry, createTestConfig(), nil)

	req := &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-pub-1"),
			},
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
			},
		},
	}

	_, err = em.BatchCreate(ctx, req)
	if err == nil {
		t.Fatalf("expected the nil-data guard to fail the atomic batch")
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("guard error lost its sentinel: %v", err)
	}
	if got, want := publicMessageOf(t, err), "operation[1]: data is required for create operation"; got != want {
		t.Fatalf("published message = %q, want %q", got, want)
	}
}

func TestSortErrorsPublishTheirMessages(t *testing.T) {
	t.Run("leaf names attribute and schema", func(t *testing.T) {
		req := &forma.QueryRequest{
			SchemaName: "e2e_wide",
			Sort:       []forma.OrderBy{{Attribute: "ghost"}},
		}
		_, err := buildAttributeOrders(req, buildSortTestCache())
		if err == nil {
			t.Fatalf("expected unknown-attribute error, got nil")
		}
		want := "cannot sort by unknown attribute 'ghost' in schema 'e2e_wide'"
		if got := publicMessageOf(t, err); got != want {
			t.Fatalf("published message = %q, want %q", got, want)
		}
	})

	t.Run("wrap prefixes the offending entry", func(t *testing.T) {
		req := &forma.QueryRequest{
			Sort: []forma.OrderBy{{Attribute: "name", SortOrder: "sideways"}},
		}
		_, err := resolveSortKeys(req)
		if err == nil {
			t.Fatalf("expected invalid sort_order error, got nil")
		}
		want := "sort entry for attribute 'name': invalid sort_order 'sideways': expected 'asc' or 'desc'"
		if got := publicMessageOf(t, err); got != want {
			t.Fatalf("published message = %q, want %q", got, want)
		}
	})

	t.Run("legacy shared direction publishes verbatim", func(t *testing.T) {
		// The legacy surface (#296) publishes the leaf message with no entry
		// prefix — there is no per-entry attribute to name — and the operator
		// wrap buildAttributeOrders adds must stay out of the body.
		req := &forma.QueryRequest{
			SchemaName: "e2e_wide",
			SortBy:     []string{"rank"},
			SortOrder:  forma.SortOrder("descending"),
		}
		_, err := buildAttributeOrders(req, buildSortTestCache())
		if err == nil {
			t.Fatalf("expected invalid sort_order error, got nil")
		}
		want := "invalid sort_order 'descending': expected 'asc' or 'desc'"
		if got := publicMessageOf(t, err); got != want {
			t.Fatalf("published message = %q, want %q", got, want)
		}
	})
}

// The service-wiring guards are operator faults, not caller faults: they no
// longer carry forma.ErrInvalidInput, so the boundary answers 500 rather than
// blaming the request (#313).
func TestQueryServiceWiringGuardsAreNotClientErrors(t *testing.T) {
	s := &entityQueryService{}
	_, err := s.Query(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected the config guard to fail")
	}
	if errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("internal wiring guard still classified as caller fault: %v", err)
	}
	if _, alsoErr := s.CrossSchemaSearch(context.Background(), nil); alsoErr == nil {
		t.Fatalf("expected the cross-schema config guard to fail")
	} else if errors.Is(alsoErr, forma.ErrInvalidInput) {
		t.Fatalf("internal wiring guard still classified as caller fault: %v", alsoErr)
	}
}

// Phase context stays operator-only (#362 review, P2): a batch failure whose
// leaf publishes must reach the caller as index + leaf, without the service's
// internal phase names ("failed to get schema", "failed to transform …").
func TestBatchCreateAtomicPublishesWithoutPhaseNames(t *testing.T) {
	ctx := context.Background()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), nil, registry, createTestConfig(), nil)

	req := &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "nosuchschema"},
				Type:             forma.OperationCreate,
				Data:             map[string]any{"name": "x"},
			},
		},
	}

	_, err = em.BatchCreate(ctx, req)
	if err == nil {
		t.Fatalf("expected the unknown schema to fail the atomic batch")
	}
	if !errors.Is(err, forma.ErrNotFound) {
		t.Fatalf("registry miss lost its sentinel: %v", err)
	}
	if got, want := publicMessageOf(t, err), "operation[0]: schema not found: nosuchschema"; got != want {
		t.Fatalf("published message = %q, want %q", got, want)
	}
	if !strings.Contains(err.Error(), "failed to get schema") {
		t.Fatalf("the operator copy lost the phase context: %v", err)
	}
}
