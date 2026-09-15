package transform

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// deepPayload builds a root map whose innermost object sits levels container
// edges below the root, every edge through the key "a". levels == 0 is the
// root itself.
func deepPayload(levels int) map[string]any {
	var value any = 1
	for range levels {
		value = map[string]any{"a": value}
	}
	root := map[string]any{"a": value}
	// The root map is depth 0; the loop above put the leaf under levels
	// further maps, so the innermost map is at depth levels.
	return root
}

// cyclicMapPayload is the shape from #406: a map that contains itself.
func cyclicMapPayload() map[string]any {
	m := map[string]any{}
	m["self"] = m
	return m
}

// cyclicSlicePayload cycles through a []any rather than a map, so the dotted
// name never grows — only the array position does. Both walkers must count
// that as depth.
func cyclicSlicePayload() map[string]any {
	s := []any{nil}
	s[0] = s
	return map[string]any{"list": s}
}

// requireTooDeep asserts the shared abort shape: an ErrInvalidInput carrier
// whose published message names the cap and the top-level key beneath which
// the runaway branch starts. Only the top-level key is named because at depth
// 1000 the full dotted path is a multi-kilobyte string.
func requireTooDeep(t *testing.T, err error, topLevelKey string) {
	t.Helper()
	require.Error(t, err)
	require.True(t, errors.Is(err, forma.ErrInvalidInput), "want ErrInvalidInput carrier, got %v", err)
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "carrier must publish: %v", err)
	require.Contains(t, msg, "payload nesting exceeds 1000 levels")
	require.Contains(t, msg, `beneath attribute "`+topLevelKey+`"`)
}

// TestNormalizeRejectsPayloadDeeperThanCap pins the cap's exact edge: 1000
// container edges below the root is accepted, 1001 is refused. Without the cap
// the deep payload normalizes fine, so this test fails by the error being nil.
func TestNormalizeRejectsPayloadDeeperThanCap(t *testing.T) {
	_, err := NormalizeDottedKeys(deepPayload(1000), dottedCache(), nil)
	require.NoError(t, err)

	_, err = NormalizeDottedKeys(deepPayload(1001), dottedCache(), nil)
	requireTooDeep(t, err, "a")
}

// TestNormalizeRejectsCyclicMap is the crash from #406: before the cap this
// call exhausts the stack fatally, before any validation runs. The keys are
// unknown to the schema on purpose — rejection must not depend on the schema
// knowing anything about the runaway branch.
func TestNormalizeRejectsCyclicMap(t *testing.T) {
	_, err := NormalizeDottedKeys(cyclicMapPayload(), dottedCache(), nil)
	requireTooDeep(t, err, "self")
}

// TestNormalizeRejectsCyclicSlice covers the array-shaped cycle, where the
// attribute name stays fixed and only the array nesting grows.
func TestNormalizeRejectsCyclicSlice(t *testing.T) {
	_, err := NormalizeDottedKeys(cyclicSlicePayload(), dottedCache(), nil)
	requireTooDeep(t, err, "list")
}

// deepAttributeRegistry knows the one attribute deepPayload(levels) spells
// out — "a" repeated levels+1 times — so the flattener's only possible
// objection to the accepted edge is the cap itself, not "attribute is not
// defined".
func deepAttributeRegistry(levels int) *stubSchemaRegistry {
	name := strings.TrimSuffix(strings.Repeat("a.", levels+1), ".")
	return &stubSchemaRegistry{
		schemaID:   300,
		schemaName: "deep",
		cache:      forma.SchemaAttributeCache{name: {AttributeID: 1, ValueType: forma.ValueTypeNumeric}},
	}
}

func flatten(t *testing.T, registry *stubSchemaRegistry, payload map[string]any) error {
	t.Helper()
	_, err := NewTransformer(registry).ToAttributes(
		context.Background(), registry.schemaID, uuid.Must(uuid.NewV7()), payload)
	return err
}

// TestFlattenRejectsPayloadDeeperThanCap pins the same edge on the writer's
// own recursion, which runs with the validator unconfigured and so is the
// first walk over caller nesting on that path.
func TestFlattenRejectsPayloadDeeperThanCap(t *testing.T) {
	require.NoError(t, flatten(t, deepAttributeRegistry(1000), deepPayload(1000)))

	err := flatten(t, deepAttributeRegistry(1001), deepPayload(1001))
	requireTooDeep(t, err, "a")
}

// TestFlattenRejectsCyclicMap is #406 with no validator in front: the map
// case recurses regardless of what the cache knows.
func TestFlattenRejectsCyclicMap(t *testing.T) {
	err := flatten(t, deepAttributeRegistry(0), cyclicMapPayload())
	requireTooDeep(t, err, "self")
}

// TestFlattenRejectsCyclicSlice is the shape where path stays fixed at
// ["list"] and only indices grow — the cap must count array nesting.
func TestFlattenRejectsCyclicSlice(t *testing.T) {
	err := flatten(t, deepAttributeRegistry(0), cyclicSlicePayload())
	requireTooDeep(t, err, "list")
}
