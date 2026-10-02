package internal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
)

var bothRequiredPolicies = []forma.RequiredPolicy{forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent}

// An update's required policy is judged on the row it writes: an update that
// names a required nested attribute satisfies it in either spelling, and the
// update keeps the attributes it does not name, the required attribute's
// sibling included (#590 review).
func TestUpdateNamingRequiredNestedAttributeKeepsSiblings(t *testing.T) {
	ctx := context.Background()
	updates := []struct {
		name    string
		updates map[string]any
		want    int64
	}{
		{"nested", map[string]any{"contact": map[string]any{"total": int64(5)}}, 5},
		{"literal", map[string]any{"contact.total": int64(5)}, 5},
		{"both spellings", map[string]any{"contact": map[string]any{"total": int64(5)}, "contact.total": int64(6)}, 6},
	}
	for _, surface := range updateSurfaces {
		for _, policy := range bothRequiredPolicies {
			for _, tc := range updates {
				t.Run(surface.name+"/"+string(policy)+"/"+tc.name, func(t *testing.T) {
					row := seedBigintRow(t, policy, map[string]any{
						"amount": int64(1), "contact": map[string]any{"total": int64(1), "name": "kept"},
					}, nil)
					got, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, tc.updates))
					if err != nil {
						t.Fatalf("update %v: %v", tc.updates, err)
					}
					for name, attrs := range map[string]map[string]any{"answered": got, "stored": row.stored(t)} {
						assertEqualValue(t, name+" contact.total", tc.want, valueAtPath(attrs, "contact.total"))
						assertEqualValue(t, name+" contact.name", "kept", valueAtPath(attrs, "contact.name"))
						assertEqualValue(t, name+" amount", int64(1), attrs["amount"])
					}
				})
			}
		}
	}
}

// The negative half: what the written row lacks is still missing, whichever
// spelling the update uses, and a value at some other path does not stand in
// for the required attribute.
func TestUpdateLeavingRequiredNestedAttributeMissingFails(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		policy  forma.RequiredPolicy
		seed    map[string]any
		updates map[string]any
	}{
		{"nested sibling into absent parent", forma.RequiredPolicyIfParentPresent,
			map[string]any{"amount": int64(1)}, map[string]any{"contact": map[string]any{"name": "x"}}},
		{"literal sibling into absent parent", forma.RequiredPolicyIfParentPresent,
			map[string]any{"amount": int64(1)}, map[string]any{"contact.name": "x"}},
		{"empty parent into absent parent", forma.RequiredPolicyIfParentPresent,
			map[string]any{"amount": int64(1)}, map[string]any{"contact": map[string]any{}}},
	}
	// A value at contact.total that writes no record there replaces the
	// stored one without standing in for it.
	for _, policy := range bothRequiredPolicies {
		for name, replacement := range map[string]any{
			"object replacing the value":       map[string]any{"total": int64(5)},
			"empty object replacing the value": map[string]any{},
		} {
			cases = append(cases, struct {
				name    string
				policy  forma.RequiredPolicy
				seed    map[string]any
				updates map[string]any
			}{name, policy,
				map[string]any{"contact": map[string]any{"total": int64(1), "name": "kept"}},
				map[string]any{"contact": map[string]any{"total": replacement}}})
		}
	}
	for _, surface := range updateSurfaces {
		for _, tc := range cases {
			t.Run(surface.name+"/"+string(tc.policy)+"/"+tc.name, func(t *testing.T) {
				row := seedBigintRow(t, tc.policy, tc.seed, nil)
				before := row.repo.records[bigintDestSchema][row.rowID]
				_, err := surface.update(ctx, row.em, bigintDestUpdate(row.rowID, tc.updates))
				assertMissingRequired(t, err, "contact.total")
				if row.repo.records[bigintDestSchema][row.rowID] != before {
					t.Fatalf("failed update stored a new row")
				}
			})
		}
	}
}

func assertMissingRequired(t *testing.T, err error, attr string) {
	t.Helper()
	want := "missing required attribute '" + attr + "'"
	var reported bestEffortFailure
	if errors.As(err, &reported) {
		if !strings.Contains(reported.failure.Error, want) {
			t.Fatalf("expected the reported failure to name %q, got %q", want, reported.failure.Error)
		}
		return
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput naming %q, got %v", want, err)
	}
	msg, ok := forma.ResolvePublicMessage(err)
	if !ok || !strings.Contains(msg, want) {
		t.Fatalf("expected the published message to name %q, got %q (%v)", want, msg, err)
	}
}
