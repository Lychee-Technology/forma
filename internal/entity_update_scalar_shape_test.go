package internal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/httpapi"
)

// scalarContainers are the containers #696 refuses where a single value
// belongs, empty or not, a singleton array included.
var scalarContainers = []struct {
	kind  string
	value any
}{
	{"an empty object", map[string]any{}},
	{"an empty array", []any{}},
	{"an array", []any{int64(5)}},
	{"an object", map[string]any{"x": int64(5)}},
}

// An update that gives a scalar attribute an object or array is refused as
// invalid input naming the attribute, its type and what was sent, on every
// update surface, for an EAV scalar, a column-bound one and a nested one in
// both spellings, under every required policy, and the stored row is left
// as it was. This manager has no validator, so no JSON Schema enforcement is
// involved: the refusal is the write's own (#696).
func TestUpdateGivingAScalarAContainerIsRefusedOnEverySurface(t *testing.T) {
	ctx := context.Background()
	positions := []struct {
		name, attr string
		updates    func(value any) map[string]any
	}{
		{"EAV", "total", func(v any) map[string]any { return map[string]any{"total": v} }},
		{"bound", "amount", func(v any) map[string]any { return map[string]any{"amount": v} }},
		{"nested", "contact.total", func(v any) map[string]any { return map[string]any{"contact": map[string]any{"total": v}} }},
		{"literal", "contact.total", func(v any) map[string]any { return map[string]any{"contact.total": v} }},
	}
	seed := map[string]any{
		"total": int64(1), "amount": int64(1), "totals": []any{int64(1)},
		"contact": map[string]any{"total": int64(1), "name": "kept"},
	}
	for _, surface := range updateSurfaces {
		for _, policy := range allRequiredPolicies {
			for _, position := range positions {
				for _, container := range scalarContainers {
					name := surface.name + "/" + string(policy) + "/" + position.name + "/" + container.kind
					t.Run(name, func(t *testing.T) {
						row := seedBigintRow(t, policy, seed, nil)
						before := row.repo.records[bigintDestSchema][row.rowID]
						_, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, position.updates(container.value)))
						assertShapeRefused(t, err, position.attr, "declared type bigint takes a single value, got "+container.kind)
						if row.repo.records[bigintDestSchema][row.rowID] != before {
							t.Fatalf("refused update stored a new row")
						}
					})
				}
			}
		}
	}
}

// A list takes an array of single values: an object at the list, or as one
// of its elements, is refused like a container at a scalar, while an array
// and the empty list that clears it are written as before.
func TestUpdateGivingAListAnObjectIsRefused(t *testing.T) {
	ctx := context.Background()
	seed := map[string]any{"amount": int64(1), "totals": []any{int64(1), int64(2)}}
	for _, surface := range updateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			for reason, updates := range map[string]map[string]any{
				"declared type list takes an array of single bigint values, got an empty object": {"totals": map[string]any{}},
				`list element at array indices "1" takes a single bigint value, got an object`:   {"totals": []any{int64(3), map[string]any{"x": int64(1)}}},
			} {
				row := seedBigintRow(t, forma.RequiredPolicyOptional, seed, nil)
				before := row.repo.records[bigintDestSchema][row.rowID]
				_, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, updates))
				assertShapeRefused(t, err, "totals", reason)
				if row.repo.records[bigintDestSchema][row.rowID] != before {
					t.Fatalf("refused update stored a new row")
				}
			}
			for _, totals := range [][]any{{int64(7)}, {}} {
				row := seedBigintRow(t, forma.RequiredPolicyOptional, seed, nil)
				if _, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, map[string]any{"totals": totals})); err != nil {
					t.Fatalf("update totals %v: %v", totals, err)
				}
				assertEqualValue(t, "totals", totals, row.stored(t)["totals"])
				assertEqualValue(t, "amount", int64(1), row.stored(t)["amount"])
			}
		})
	}
}

// assertShapeRefused checks that err is the published shape refusal of attr,
// naming reason, as Update and the atomic batch return it and as the
// best-effort batch reports it.
func assertShapeRefused(t *testing.T, err error, attr, reason string) {
	t.Helper()
	wants := []string{"invalid value for attribute '" + attr + "'", reason}
	message := ""
	var reported bestEffortFailure
	switch {
	case errors.As(err, &reported):
		message = reported.failure.Error
	case errors.Is(err, forma.ErrInvalidInput):
		published, ok := forma.ResolvePublicMessage(err)
		if !ok {
			t.Fatalf("expected a published message, got %v", err)
		}
		message = published
	default:
		t.Fatalf("expected the update to be refused as invalid input, got %v", err)
	}
	for _, want := range wants {
		if !strings.Contains(message, want) {
			t.Fatalf("expected the refusal to name %q, got %q", want, message)
		}
	}
}

// Over HTTP the refusal is a 400 whose body names the attribute, its type
// and what was sent, for the shapes that used to answer 200 (#696).
func TestUpdateRouteAnswers400ForAContainerAtAScalar(t *testing.T) {
	for body, want := range map[string]string{
		`{"total": {}}`:             "invalid value for attribute 'total' (attrID=30): declared type bigint takes a single value, got an empty object",
		`{"amount": []}`:            "invalid value for attribute 'amount' (attrID=33): declared type bigint takes a single value, got an empty array",
		`{"contact.total": [5]}`:    "invalid value for attribute 'contact.total' (attrID=34): declared type bigint takes a single value, got an array",
		`{"contact": {"name": {}}}`: "invalid value for attribute 'contact.name' (attrID=35): declared type text takes a single value, got an empty object",
	} {
		row := seedBigintRow(t, forma.RequiredPolicyOptional, map[string]any{"total": int64(1), "amount": int64(1)}, nil)
		before := row.repo.records[bigintDestSchema][row.rowID]
		handler := httpapi.NewServer(row.em, httpapi.Options{}).Handler()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/bigint_dest/"+row.rowID.String(), strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("PUT %s: expected 400, got %d: %s", body, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("PUT %s: the body must name the attribute and its type, got %s", body, rec.Body.String())
		}
		if row.repo.records[bigintDestSchema][row.rowID] != before {
			t.Fatalf("PUT %s: refused update stored a new row", body)
		}
	}
}
