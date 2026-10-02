package transform

// The write's required policy is judged on what the write stores
// (requireWrittenAttributes), read off the same walk that produces the
// records: an attribute is present in either spelling, a losing spelling
// writes nothing, and a parent is present where a winning claim writes
// beneath it or the caller sent it explicitly.

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// writtenRequiredRegistry declares a nested object and an array of objects;
// only required carries policy.
func writtenRequiredRegistry(required string, policy forma.RequiredPolicy) *stubSchemaRegistry {
	cache := forma.SchemaAttributeCache{
		"contact.total":    {AttributeID: 1, ValueType: forma.ValueTypeBigInt},
		"contact.name":     {AttributeID: 2, ValueType: forma.ValueTypeText},
		"order.items.name": {AttributeID: 3, ValueType: forma.ValueTypeText},
		"order.items.qty":  {AttributeID: 4, ValueType: forma.ValueTypeBigInt},
	}
	meta := cache[required]
	meta.RequiredPolicy = policy
	cache[required] = meta
	return &stubSchemaRegistry{schemaID: 502, schemaName: "written_required", cache: cache}
}

var bothRequiredPolicies = []forma.RequiredPolicy{forma.RequiredPolicyAlways, forma.RequiredPolicyIfParentPresent}

type writtenRequiredCase struct {
	name string
	data map[string]any
	// missing is the attribute the write must report missing; "" means the
	// required policy passes.
	missing string
}

func runWrittenRequiredCases(t *testing.T, required string, policy forma.RequiredPolicy, cases []writtenRequiredCase) {
	t.Helper()
	tr := NewTransformer(writtenRequiredRegistry(required, policy))
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%s", policy, tc.name), func(t *testing.T) {
			_, err := tr.ToAttributes(context.Background(), 502, uuid.Must(uuid.NewV7()), tc.data)
			if tc.missing == "" {
				if err != nil {
					require.NotContains(t, err.Error(), "missing required attribute")
				}
				return
			}
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.ErrorContains(t, err, "missing required attribute '"+tc.missing+"'")
		})
	}
}

func TestToAttributesRequiredNestedAttributeInEitherSpelling(t *testing.T) {
	for _, policy := range bothRequiredPolicies {
		runWrittenRequiredCases(t, "contact.total", policy, []writtenRequiredCase{
			{"nested", map[string]any{"contact": map[string]any{"total": 5}}, ""},
			{"literal", map[string]any{"contact.total": 5}, ""},
			{"literal beside nested sibling", map[string]any{"contact": map[string]any{"name": "x"}, "contact.total": 5}, ""},
			{"both spellings", map[string]any{"contact": map[string]any{"total": 5}, "contact.total": 6}, ""},
			{"nested sibling only", map[string]any{"contact": map[string]any{"name": "x"}}, "contact.total"},
			{"literal sibling only", map[string]any{"contact.name": "x"}, "contact.total"},
			{"empty parent", map[string]any{"contact": map[string]any{}}, "contact.total"},
			{"object where the value belongs", map[string]any{"contact": map[string]any{"total": map[string]any{"x": 1}}}, "contact.total"},
			{"array where the value belongs", map[string]any{"contact": map[string]any{"total": []any{}}}, "contact.total"},
			{"literal name nested under the parent", map[string]any{"contact": map[string]any{"contact.total": 5}}, "contact.total"},
		})
	}
}

// Without the parent, only required_always is enforced: a write that names
// some other path, or none, leaves contact absent.
func TestToAttributesRequiredNestedAttributeWithoutParent(t *testing.T) {
	cases := func(missing string) []writtenRequiredCase {
		return []writtenRequiredCase{
			{"nothing", map[string]any{}, missing},
			{"unrelated", map[string]any{"order": map[string]any{"items": []any{map[string]any{"name": "x"}}}}, missing},
			{"undefined lookalike", map[string]any{"contact.totals": 5}, missing},
			{"null parent", map[string]any{"contact": nil}, missing},
			{"array parent", map[string]any{"contact": []any{}}, missing},
		}
	}
	runWrittenRequiredCases(t, "contact.total", forma.RequiredPolicyAlways, cases("contact.total"))
	runWrittenRequiredCases(t, "contact.total", forma.RequiredPolicyIfParentPresent, cases(""))
}

// Within an array of objects the attribute is required at every element the
// write stores, and a spelling's element is an element only where it writes.
func TestToAttributesRequiredArrayAttributePerElement(t *testing.T) {
	nested := func(items ...any) map[string]any {
		return map[string]any{"order": map[string]any{"items": items}}
	}
	for _, policy := range bothRequiredPolicies {
		runWrittenRequiredCases(t, "order.items.qty", policy, []writtenRequiredCase{
			{"every element", nested(map[string]any{"qty": 1}, map[string]any{"qty": 2}), ""},
			{"literal element", map[string]any{"order.items": []any{map[string]any{"qty": 1}}}, ""},
			{"literal empty element", map[string]any{"order.items": []any{map[string]any{}}}, "order.items.qty"},
			{"one element short", nested(map[string]any{"qty": 1}, map[string]any{"name": "b"}), "order.items.qty"},
			{"literal fills one of two", mergedPayload(
				nested(map[string]any{"name": "a"}, map[string]any{"name": "b"}),
				map[string]any{"order.items": []any{map[string]any{"qty": 1}}}), "order.items.qty"},
			{"losing element", mergedPayload(
				nested(map[string]any{"qty": 1}, map[string]any{"qty": 2}),
				map[string]any{"order.items": []any{map[string]any{"qty": 5}}}), ""},
		})
	}
	runWrittenRequiredCases(t, "order.items.qty", forma.RequiredPolicyIfParentPresent, []writtenRequiredCase{
		{"literal empty array", map[string]any{"order.items": []any{}}, ""},
	})
}

func mergedPayload(payloads ...map[string]any) map[string]any {
	merged := make(map[string]any)
	for _, payload := range payloads {
		for key, value := range payload {
			merged[key] = value
		}
	}
	return merged
}

// Of two spellings of one attribute the literal key is written (#312), and
// the policy that passed sees the same value the row stores.
func TestToAttributesRequiredSeesTheWinningSpelling(t *testing.T) {
	tr := NewTransformer(writtenRequiredRegistry("contact.total", forma.RequiredPolicyAlways))

	attrs, err := tr.ToAttributes(context.Background(), 502, uuid.Must(uuid.NewV7()), map[string]any{
		"contact":       map[string]any{"total": 5, "name": "kept"},
		"contact.total": 6,
	})
	require.NoError(t, err)
	values := make(map[int16]any, len(attrs))
	for _, attr := range attrs {
		values[attr.AttrID] = attr.Value
	}
	require.Equal(t, map[int16]any{1: int64(6), 2: "kept"}, values)
}
