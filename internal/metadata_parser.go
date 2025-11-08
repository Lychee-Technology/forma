package internal

import (
	"fmt"
	"strings"
)

// parseAttributeMetadata converts raw JSON metadata into AttributeMetadata structs reused
// by loaders, registries, and repositories. The source argument is used for readable errors.
func parseAttributeMetadata(attrName string, attrData map[string]any, source string) (AttributeMetadata, error) {
	meta := AttributeMetadata{AttributeName: attrName}

	id, ok := attrData["attributeID"].(float64)
	if !ok {
		return AttributeMetadata{}, fmt.Errorf("invalid or missing attributeID for attribute %s in %s", attrName, source)
	}
	meta.AttributeID = int16(id)

	valueType, ok := attrData["valueType"].(string)
	if !ok || valueType == "" {
		return AttributeMetadata{}, fmt.Errorf("invalid or missing valueType for attribute %s in %s", attrName, source)
	}
	meta.ValueType = ValueType(valueType)

	binding, err := extractMainColumnBinding(attrName, attrData, source)
	if err != nil {
		return AttributeMetadata{}, err
	}

	// Always populate Storage metadata
	storage := buildStorageMetadata(meta.ValueType, binding)
	meta.Storage = &storage

	return meta, nil
}

func extractMainColumnBinding(attrName string, attrData map[string]any, source string) (*MainColumnBinding, error) {
	if raw, ok := attrData["column_binding"].(map[string]any); ok {
		return parseBindingObject(attrName, raw, source)
	}

	return nil, nil
}

func parseBindingObject(attrName string, raw map[string]any, source string) (*MainColumnBinding, error) {
	colName, _ := raw["col_name"].(string)
	encoding, _ := raw["encoding"].(string)

	if colName == "" {
		return nil, fmt.Errorf("invalid columnName in columnBinding for attribute %s in %s", attrName, source)
	}
	return &MainColumnBinding{
		ColumnName: MainColumn(colName),
		Encoding:   normalizeColumnEncoding(encoding),
	}, nil
}

func normalizeMainColumnType(raw string) ValueType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "text":
		return ValueTypeText
	case "smallint", "int2", "int16":
		return ValueTypeSmallInt
	case "integer", "int", "int4", "int32":
		return ValueTypeInteger
	case "bigint", "int8", "int64":
		return ValueTypeBigInt
	case "double", "double precision", "float8":
		return ValueTypeNumeric
	case "date":
		return ValueTypeDate
	case "datetime", "timestamp", "timestamptz":
		return ValueTypeDateTime
	case "uuid":
		return ValueTypeUUID
	case "bool", "boolean":
		return ValueTypeBool
	default:
		return ValueType(raw)
	}
}

func normalizeColumnEncoding(raw string) MainColumnEncoding {
	if raw == "" {
		return MainColumnEncodingDefault
	}
	return MainColumnEncoding(strings.TrimSpace(raw))
}
