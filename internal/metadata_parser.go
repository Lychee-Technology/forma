package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// parseAttributeMetadata converts raw JSON metadata into forma.AttributeMetadata structs reused
// by loaders, registries, and repositories. The source argument is used for readable errors.
func parseAttributeMetadata(attrName string, attrData map[string]any, source string) (forma.AttributeMetadata, error) {
	meta := forma.AttributeMetadata{AttributeName: attrName}

	id, ok := attrData["attributeID"].(float64)
	if !ok {
		return forma.AttributeMetadata{}, fmt.Errorf("invalid or missing attributeID for attribute %s in %s", attrName, source)
	}
	meta.AttributeID = int16(id)

	valueType, ok := attrData["valueType"].(string)
	if !ok || valueType == "" {
		return forma.AttributeMetadata{}, fmt.Errorf("invalid or missing valueType for attribute %s in %s", attrName, source)
	}
	meta.ValueType = forma.ValueType(valueType)

	binding, err := extractMainColumnBinding(attrName, attrData, source)
	if err != nil {
		return forma.AttributeMetadata{}, err
	}

	meta.ColumnBinding = binding

	return meta, nil
}

func extractMainColumnBinding(attrName string, attrData map[string]any, source string) (*forma.MainColumnBinding, error) {
	if raw, ok := attrData["column_binding"].(map[string]any); ok {
		return parseBindingObject(attrName, raw, source)
	}

	return nil, nil
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
