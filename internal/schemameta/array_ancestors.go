package schemameta

import (
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// withArrayAncestors returns cache with every attribute's ArrayAncestors
// derived from the schema document's properties (#623). The attributes file
// names an attribute inside an array of objects by its dotted path alone, so
// only the document says which of its ancestors are arrays.
//
// It derives them for every attribute or for none: a rebuild reads each row
// against one rule, and a row whose attributes disagreed on whether a shared
// ancestor is an array would read it both ways. When the document does not
// resolve an attribute's ancestors, such as an ancestor behind another
// file's $ref (visit's contact), cache is returned unchanged and the schema's
// rows rebuild by row evidence, as they did before #623.
func withArrayAncestors(schemaName string, cache forma.SchemaAttributeCache, properties map[string]*forma.PropertySchema) forma.SchemaAttributeCache {
	names := make([]string, 0, len(cache))
	for name := range cache {
		names = append(names, name)
	}
	sort.Strings(names)

	derived := make(forma.SchemaAttributeCache, len(cache))
	for _, name := range names {
		ancestors, ok := arrayAncestorsOf(name, properties)
		if !ok {
			zap.S().Infow("array ancestors not derived: the schema document does not resolve an attribute's ancestors, "+
				"so an object or array nested inside an array of objects does not rebuild in its written shape",
				"schema", schemaName, "attribute", name)
			return cache
		}
		meta := cache[name]
		meta.ArrayAncestors = ancestors
		derived[name] = meta
	}
	return derived
}

// arrayAncestorsOf walks name's ancestors through the document and returns
// the ones declared as arrays, outermost first, and false when the document
// does not say what an ancestor is: it is missing, or declares neither an
// object nor an array of objects, as a $ref the parser did not resolve does
// not. An array's items hold the next segment, since an array index is not
// part of an attribute's name (requirement.areas.city is the city of
// requirement.areas[i]). The leaf itself is not looked up, so a list or a
// $ref'd scalar needs nothing more.
func arrayAncestorsOf(name string, properties map[string]*forma.PropertySchema) ([]string, bool) {
	segments := strings.Split(name, ".")
	ancestors := []string{}
	for i, segment := range segments[:len(segments)-1] {
		prop := properties[segment]
		switch {
		case prop == nil:
			return nil, false
		case prop.Type == "array" || prop.Items != nil:
			if !declaresObject(prop.Items) {
				return nil, false
			}
			ancestors = append(ancestors, strings.Join(segments[:i+1], "."))
			properties = prop.Items.Properties
		case declaresObject(prop):
			properties = prop.Properties
		default:
			return nil, false
		}
	}
	return ancestors, true
}

// declaresObject covers a "type" the parser keeps, which is a single string,
// and a union such as ["object", "null"], which it drops but whose properties
// it parses.
func declaresObject(prop *forma.PropertySchema) bool {
	return prop != nil && (prop.Type == "object" || prop.Properties != nil)
}
