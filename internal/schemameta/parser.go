package schemameta

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// parseAttributeMetadata converts raw JSON metadata into forma.AttributeMetadata structs reused
// by loaders, registries, and repositories. The source argument is used for readable errors:
// every error ends with "in <source>", so callers return it unwrapped rather than repeating
// the attribute name or the file it came from.
func parseAttributeMetadata(attrName string, attrData map[string]any, source string) (forma.AttributeMetadata, error) {
	meta := forma.AttributeMetadata{AttributeName: attrName}

	id, err := parseAttributeID(attrData["attributeID"], attrName, source)
	if err != nil {
		return forma.AttributeMetadata{}, err
	}
	meta.AttributeID = id

	valueType, ok := attrData["valueType"].(string)
	if !ok || valueType == "" {
		return forma.AttributeMetadata{}, fmt.Errorf("invalid or missing valueType for attribute %s in %s", attrName, source)
	}
	meta.ValueType = forma.ValueType(valueType)

	if itemsType, ok := attrData["items_type"].(string); ok && itemsType != "" {
		if forma.ValueType(itemsType) == forma.ValueTypeList {
			return forma.AttributeMetadata{}, fmt.Errorf(
				"invalid items_type 'list' for attribute %s in %s: nested lists are not supported", attrName, source)
		}
		meta.ItemsType = forma.ValueType(itemsType)
	}

	requiredPolicy, _, err := parseRequiredPolicy(attrName, attrData, source)
	if err != nil {
		return forma.AttributeMetadata{}, err
	}
	meta.RequiredPolicy = requiredPolicy
	policy := meta.EffectiveRequiredPolicy()
	meta.Required = policy == forma.RequiredPolicyAlways || policy == forma.RequiredPolicyIfParentPresent

	binding, err := extractMainColumnBinding(attrName, attrData, source)
	if err != nil {
		return forma.AttributeMetadata{}, err
	}

	meta.ColumnBinding = binding

	if rawRetired, exists := attrData["retired"]; exists {
		retired, ok := rawRetired.(bool)
		if !ok {
			return forma.AttributeMetadata{}, fmt.Errorf(
				"invalid retired flag for attribute %s in %s: must be a boolean", attrName, source)
		}
		meta.Retired = retired
	}

	return meta, nil
}

func extractMainColumnBinding(attrName string, attrData map[string]any, source string) (*forma.MainColumnBinding, error) {
	rawValue, exists := attrData["column_binding"]
	if !exists {
		return nil, nil
	}

	raw, ok := rawValue.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid column_binding format for attribute %s in %s", attrName, source)
	}
	// A typed-nil map (Go-constructed input only; JSON null fails the assertion
	// above) proceeds and fails the col_name check — strict, not silently ignored.
	return parseBindingObject(attrName, raw, source)
}

func parseBindingObject(attrName string, raw map[string]any, source string) (*forma.MainColumnBinding, error) {
	colName, _ := raw["col_name"].(string)
	encoding, _ := raw["encoding"].(string)

	if colName == "" {
		return nil, fmt.Errorf("invalid columnName in columnBinding for attribute %s in %s", attrName, source)
	}
	return &forma.MainColumnBinding{
		ColumnName: forma.MainColumn(colName),
		Encoding:   normalizeColumnEncoding(encoding),
	}, nil
}

func normalizeColumnEncoding(raw string) forma.MainColumnEncoding {
	if raw == "" {
		return forma.MainColumnEncodingDefault
	}
	return forma.MainColumnEncoding(strings.TrimSpace(raw))
}
