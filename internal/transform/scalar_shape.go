package transform

import (
	"fmt"
	"strings"
	"sync"

	"github.com/lychee-technology/forma"
)

// An attribute's own position holds one value of its type, or for a list an
// array of them. The write path decomposes a JSON object or array into the
// positions beneath it, so a container sent where a single value belongs
// loses its shape: {} and [] write no record at all (an EAV attribute
// vanishes, a bound column keeps its old value), and a non-empty array
// writes element records under an attribute that has no elements (#696).
// That is a storage invariant, not a schema constraint: it holds whether
// JSON Schema validation is strict, report-only or not configured, and it is
// judged before the walk descends, where the container is still whole.
//
// The rule is positional. A path is an attribute's position when its dotted
// name is an attribute of the schema, whichever spelling reached it (nested
// keys or a literal dotted key) and however many arrays of objects lie
// around it: items[0].name is one, items is not.
//
//   - An object, empty or not, is refused at every attribute's position: no
//     value type, list elements included, stores an object there.
//   - An array, empty or not, is refused at a non-list attribute whose
//     schema document declares it a non-array type (SchemaShapes.Declared).
//     Metadata alone cannot tell such an attribute from a primitive array
//     recorded under its element type before #204 (contact.phones as text),
//     which stores its elements at array indices and is still read and
//     written that way. So an attribute the document declares an array
//     keeps that behavior, and so does one the document does not resolve (a
//     $ref the parser left open, a union type it drops, a registry without
//     documents): there is no evidence it is a scalar. A list's own array
//     and its elements keep their rules (populateTypedValue, emptyList).
//
// Only the JSON shapes the walk decomposes (map[string]any and []any) are
// containers here. Any other Go value goes to the scalar conversion as
// before, which accepts what it supports ([]byte as text, a fmt.Stringer)
// and refuses the rest.

// SchemaShapes answers what a schema's document declares at an attribute's
// own position. Its document is read from the registry once, on the first
// question, which only a container at an attribute's position asks; the zero
// value knows no document.
type SchemaShapes struct {
	lookup *shapeLookup
}

type shapeLookup struct {
	once       sync.Once
	registry   forma.SchemaRegistry
	schemaID   int16
	properties map[string]*forma.PropertySchema
}

// SchemaShapesOf answers the shapes registry's document declares for
// schemaID. A registry that cannot answer the document knows none.
func SchemaShapesOf(registry forma.SchemaRegistry, schemaID int16) SchemaShapes {
	if registry == nil {
		return SchemaShapes{}
	}
	return SchemaShapes{lookup: &shapeLookup{registry: registry, schemaID: schemaID}}
}

// DeclaredShape is what a schema document declares at an attribute's own
// position, as far as the write can rely on it (SchemaShapes.Declared).
type DeclaredShape uint8

const (
	// ShapeUnresolved means the document gives no evidence: it does not
	// resolve the attribute, leaves its type open, or the attribute lies
	// inside an array of objects in a schema whose registry derived no array
	// ancestry, whose rows a read rebuilds by inference (rebuildInferred),
	// reading a member of an array of objects as an array of its own.
	ShapeUnresolved DeclaredShape = iota
	// ShapeScalar is a type that is not an array, at a position where the
	// write's own read cannot answer an array either.
	ShapeScalar
	// ShapeArray is an array: under element-typed metadata, a primitive
	// array recorded before #204.
	ShapeArray
)

// Declared answers what the document declares at the attribute name, whose
// metadata is meta.
func (s SchemaShapes) Declared(name string, meta forma.AttributeMetadata) DeclaredShape {
	if s.lookup == nil {
		return ShapeUnresolved
	}
	s.lookup.once.Do(func() {
		_, doc, err := s.lookup.registry.GetSchemaByID(s.lookup.schemaID)
		if err == nil {
			s.lookup.properties = doc.Properties
		}
	})
	prop, crossesArray := declaredProperty(name, s.lookup.properties)
	switch {
	case prop == nil:
		return ShapeUnresolved
	case prop.Type == "array" || prop.Items != nil:
		return ShapeArray
	case prop.Type == "" || crossesArray && meta.ArrayAncestors == nil:
		return ShapeUnresolved
	default:
		return ShapeScalar
	}
}

// declaredProperty walks name through the document's properties, an array's
// items holding the next segment, and answers the property declared at it,
// or nil when the document does not resolve it, and whether an ancestor is
// an array.
func declaredProperty(name string, properties map[string]*forma.PropertySchema) (*forma.PropertySchema, bool) {
	segments := strings.Split(name, ".")
	crossesArray := false
	for _, segment := range segments[:len(segments)-1] {
		prop := properties[segment]
		if prop == nil {
			return nil, false
		}
		if prop.Type == "array" || prop.Items != nil {
			if prop.Items == nil {
				return nil, false
			}
			crossesArray = true
			prop = prop.Items
		}
		properties = prop.Properties
	}
	return properties[segments[len(segments)-1]], crossesArray
}

// scalarShapeRefusal answers the published refusal of value at path when
// value is a JSON object or array the attribute at path cannot store, or nil
// (see above).
func scalarShapeRefusal(path []string, value any, cache forma.SchemaAttributeCache, shapes SchemaShapes) error {
	var isArray bool
	switch value.(type) {
	case map[string]any:
	case []any:
		isArray = true
	default:
		return nil
	}
	name := strings.Join(path, ".")
	meta, ok := cache[name]
	if !ok {
		return nil
	}
	if isArray && (meta.ValueType == forma.ValueTypeList || shapes.Declared(name, meta) != ShapeScalar) {
		return nil
	}
	expected := fmt.Sprintf("declared type %s takes a single value", meta.ValueType)
	if meta.ValueType == forma.ValueTypeList {
		expected = fmt.Sprintf("declared type list takes an array of %s values", meta.EffectiveItemsType())
	}
	return conversionRefusal(name, meta, fmt.Errorf(
		"%s, got %s; an object or array, empty or not, cannot be stored where a single value belongs "+
			"(to keep the current value, omit the key)",
		expected, containerKind(value)))
}

// containerKind describes a JSON object or array.
func containerKind(value any) string {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 0 {
			return "an empty object"
		}
		return "an object"
	case []any:
		switch len(v) {
		case 0:
			return "an empty array"
		case 1:
			return "an array of 1 element"
		default:
			return fmt.Sprintf("an array of %d elements", len(v))
		}
	default:
		return fmt.Sprintf("%T", value)
	}
}
