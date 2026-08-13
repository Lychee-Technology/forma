package transform

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/numutil"
	"github.com/lychee-technology/forma/internal/schemameta"
	"go.uber.org/zap"
)

// AttributeConverter provides conversion between model.EntityAttribute and model.EAVRecord
type AttributeConverter struct {
	registry      forma.SchemaRegistry
	relationRoots RelationRootsLookup
}

// NewAttributeConverter creates a new AttributeConverter instance
func NewAttributeConverter(registry forma.SchemaRegistry) *AttributeConverter {
	return &AttributeConverter{
		registry: registry,
	}
}

// SetRelationRoots installs the relation-root lookup consulted by the
// required-policy check in FromEAVRecords. A nil lookup, or one that answers an
// empty set, leaves enforcement exactly as it was.
func (c *AttributeConverter) SetRelationRoots(lookup RelationRootsLookup) {
	c.relationRoots = lookup
}

// relationRootsFor resolves the relation roots of schemaID, translating the ID
// to the schema name the lookup is keyed by.
func (c *AttributeConverter) relationRootsFor(schemaID int16) (RelationRoots, error) {
	if c.relationRoots == nil {
		return nil, nil
	}
	schemaName, _, err := c.registry.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve schema name for id %d: %w", schemaID, err)
	}
	return c.relationRoots(schemaName), nil
}

// ToEAVRecord converts an model.EntityAttribute to an model.EAVRecord
func (c *AttributeConverter) ToEAVRecord(attr model.EntityAttribute, rowID uuid.UUID) (model.EAVRecord, error) {
	record := model.EAVRecord{
		SchemaID:     attr.SchemaID,
		RowID:        rowID,
		AttrID:       attr.AttrID,
		ArrayIndices: attr.ArrayIndices,
	}

	if attr.Value == nil {
		return record, nil
	}

	switch attr.ValueType {
	case forma.ValueTypeText:
		if strVal, ok := attr.Value.(string); ok {
			record.ValueText = &strVal
		} else {
			return record, fmt.Errorf("value type mismatch: expected string for text type")
		}

	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		numVal, err := toFloat64ForEAV(attr.Value)
		if err == nil {
			numVal, err = finiteForEAV(numVal)
		}
		if err != nil {
			return record, fmt.Errorf("convert to numeric: %w", err)
		}
		record.ValueNumeric = &numVal
		if attr.ValueType == forma.ValueTypeBigInt {
			if exact, ok := toInt64ExactForEAV(attr.Value); ok {
				record.ValueInt64 = &exact
			}
		}

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		timeVal, err := toTimeForEAV(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to time: %w", err)
		}
		unixMillis := timeToUnixMillisFloat64(timeVal)
		record.ValueNumeric = &unixMillis
		exactMs := timeVal.UnixMilli()
		record.ValueInt64 = &exactMs

	case forma.ValueTypeUUID:
		if uuidVal, ok := attr.Value.(uuid.UUID); ok {
			strVal := uuidVal.String()
			record.ValueText = &strVal
		} else {
			return record, fmt.Errorf("value type mismatch: expected uuid.UUID for uuid type")
		}

	case forma.ValueTypeBool:
		boolVal, err := toBoolForEAV(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to bool: %w", err)
		}
		floatBool := boolToFloat64(boolVal)
		record.ValueNumeric = &floatBool

	default:
		return record, fmt.Errorf("unsupported value type: %s", attr.ValueType)
	}

	return record, nil
}

// FromEAVRecord converts an model.EAVRecord to an model.EntityAttribute
func (c *AttributeConverter) FromEAVRecord(record model.EAVRecord, valueType forma.ValueType) (model.EntityAttribute, error) {
	attr := model.EntityAttribute{
		SchemaID:     record.SchemaID,
		RowID:        record.RowID,
		AttrID:       record.AttrID,
		ArrayIndices: record.ArrayIndices,
		ValueType:    valueType,
	}

	var err error
	attr.Value, err = extractValueFromEAVRecord(record, valueType)
	if err != nil {
		return attr, err
	}

	return attr, nil
}

// ToEAVRecords converts a slice of EntityAttributes to EAVRecords
func (c *AttributeConverter) ToEAVRecords(attributes []model.EntityAttribute, rowID uuid.UUID) ([]model.EAVRecord, error) {
	records := make([]model.EAVRecord, 0, len(attributes))
	for _, attr := range attributes {
		record, err := c.ToEAVRecord(attr, rowID)
		if err != nil {
			return nil, fmt.Errorf("convert attribute attrID=%d: %w", attr.AttrID, err)
		}
		records = append(records, record)
	}
	return records, nil
}

// FromEAVRecords converts a slice of EAVRecords to EntityAttributes
func (c *AttributeConverter) FromEAVRecords(records []model.EAVRecord) ([]model.EntityAttribute, error) {
	if len(records) == 0 {
		return []model.EntityAttribute{}, nil
	}

	// Get schema metadata to determine value types
	schemaID := records[0].SchemaID
	cache, idToName, err := schemameta.GetSchemaMetadata(c.registry, schemaID)
	if err != nil {
		return nil, err
	}

	presentAttrIndices := make(map[string]map[string]struct{}, len(records))

	attributes := make([]model.EntityAttribute, 0, len(records))
	skippedAttrIDs := make(map[int16]struct{})
	for _, record := range records {
		attrName, ok := idToName[record.AttrID]
		if !ok {
			// #294 tolerate-and-preserve: this attrID was removed by schema
			// evolution. Skip it on read — the schema can no longer address
			// it — but never treat it as an error: the row must stay readable
			// and updatable, and the stored EAV rows are preserved untouched
			// (replaceEAVAttributes scopes its delete to current-schema
			// attrIDs) so re-adding the attribute restores the values.
			skippedAttrIDs[record.AttrID] = struct{}{}
			continue
		}
		indexSet := presentAttrIndices[attrName]
		if indexSet == nil {
			indexSet = make(map[string]struct{})
			presentAttrIndices[attrName] = indexSet
		}
		indexSet[record.ArrayIndices] = struct{}{}

		meta := cache[attrName]
		vt := meta.ValueType
		if vt == forma.ValueTypeList {
			// List elements are stored one row per element; type each element
			// by the declared items type so downstream conversion (including
			// ToEAVRecord) sees a scalar type, never the container type (#204).
			vt = meta.EffectiveItemsType()
		}
		attr, err := c.FromEAVRecord(record, vt)
		if err != nil {
			return nil, fmt.Errorf("convert record attrID=%d: %w", record.AttrID, err)
		}
		attributes = append(attributes, attr)
	}
	if len(skippedAttrIDs) > 0 {
		ids := make([]int16, 0, len(skippedAttrIDs))
		for id := range skippedAttrIDs {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		zap.S().Warnw("skipped EAV records for attribute ids not in metadata cache (removed by schema evolution; rows preserved, #294)",
			"schemaID", schemaID, "rowID", records[0].RowID, "attrIDs", ids)
	}

	if err := c.checkRequiredAttributes(schemaID, cache, presentAttrIndices); err != nil {
		return nil, err
	}

	return attributes, nil
}

// checkRequiredAttributes enforces each attribute's required policy against the
// attribute names the records actually carried, per array-index context.
func (c *AttributeConverter) checkRequiredAttributes(
	schemaID int16,
	cache forma.SchemaAttributeCache,
	presentAttrIndices map[string]map[string]struct{},
) error {
	relationRoots, err := c.relationRootsFor(schemaID)
	if err != nil {
		return fmt.Errorf("resolve relation roots for required-policy check: %w", err)
	}

	missingRequired := make(map[int16]string)
	for attrName, metadata := range cache {
		// #314/#315: relation-root data is derived on read and never
		// schema-validated on write — since #318 the whole subtree is stripped
		// from the payload before validation — so required policies beneath a
		// root must follow the same rule, otherwise expanding a root's
		// attributes (#315 resolved contactSnapshot's $ref) turns payloads #314
		// ruled acceptable into 400s.
		//
		// The carve-out belongs to this check, not to the read path: ToAttributes
		// reaches FromEAVRecords on every create and update (transformer.go). The
		// write path's own required check (validateRequiredAttributesFromInput,
		// transformer.go) has none, so a required_always beneath a root still
		// rejects the stripped payload there. Documented in docs/error-handling.md.
		if relationRoots.Covers(attrName) {
			continue
		}
		switch metadata.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways:
			if isRequiredAttributeMissing(attrName, presentAttrIndices, true) {
				missingRequired[metadata.AttributeID] = attrName
			}
		case forma.RequiredPolicyIfParentPresent:
			if isRequiredAttributeMissing(attrName, presentAttrIndices, false) {
				missingRequired[metadata.AttributeID] = attrName
			}
		}
	}
	if len(missingRequired) == 0 {
		return nil
	}

	zap.S().Infow("missing EAV records for attrIDs.", "idToName", missingRequired)
	names := make([]string, 0, len(missingRequired))
	idsByName := make(map[string]int16, len(missingRequired))
	for id, name := range missingRequired {
		names = append(names, name)
		idsByName[name] = id
	}
	// Name the alphabetically first missing attribute, not whichever the map
	// yields first, so the same drift produces the same error on every run.
	missingAttrName := slices.Min(names)

	// Plain error, deliberately. FromEAVRecords is not write-only: the read
	// path rebuilds already-stored records through it
	// (persistent_record.go's FromPersistentRecord), so a persisted row
	// missing a required EAV row reaches here too. Wrapping
	// forma.ErrInvalidInput here — as an earlier #301 sweep did — made the
	// HTTP boundary answer that persisted-drift case with a verbatim 400,
	// inverting the split AGENTS.md and this repo's error-handling doc
	// draw: write validation carries the sentinel, read-path consistency
	// failures stay plain and operator-visible.
	//
	// The write path's 400 does not depend on this wrap. It has its own
	// write-only validator, validateRequiredAttributesFromInput
	// (transformer.go), which ToAttributes runs against the caller's input
	// before flattening and which does carry the sentinel.
	return fmt.Errorf("missing required attribute '%s' (attrID=%d) in EAV records",
		missingAttrName, idsByName[missingAttrName])
}

// Helper functions for conversion
//
// The numeric coercion helpers (toFloat64ForEAV, parseTrimmedFloat64) live in
// numeric_conversion.go alongside the finiteForEAV guard they feed.

func toTimeForEAV(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		timeValue, err := derefPointer(v, "time")
		if err != nil {
			return time.Time{}, err
		}
		return timeValue, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBoolForEAV(value any) (bool, error) {
	switch v := value.(type) {
	case string:
		return isTrueStringForEAV(v), nil
	case *string:
		str, err := derefPointer(v, "string")
		if err != nil {
			return false, err
		}
		return isTrueStringForEAV(str), nil
	case bool:
		return v, nil
	case *bool:
		booleanValue, err := derefPointer(v, "bool")
		if err != nil {
			return false, err
		}
		return booleanValue, nil
	case int:
		return boolFromPositive(v), nil
	case *int:
		num, err := derefPointer(v, "int")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int16:
		return boolFromPositive(v), nil
	case *int16:
		num, err := derefPointer(v, "int16")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int32:
		return boolFromNonZero(v), nil
	case *int32:
		num, err := derefPointer(v, "int32")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case int64:
		return boolFromNonZero(v), nil
	case *int64:
		num, err := derefPointer(v, "int64")
		if err != nil {
			return false, err
		}
		return boolFromPositive(num), nil
	case float32:
		return finiteFloat64ToBool(float64(v))
	case *float32:
		num, err := derefPointer(v, "float32")
		if err != nil {
			return false, err
		}
		return finiteFloat64ToBool(float64(num))
	case float64:
		return finiteFloat64ToBool(v)
	case *float64:
		num, err := derefPointer(v, "float64")
		if err != nil {
			return false, err
		}
		return finiteFloat64ToBool(num)
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return false, fmt.Errorf("cannot convert json.Number %q to bool: %w", v.String(), err)
		}
		if err := finiteBoolInput(f); err != nil {
			return false, err
		}
		return f != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// finiteFloat64ToBool is toBoolForEAV's float path: reject non-finite, then
// apply the existing threshold coercion unchanged.
func finiteFloat64ToBool(value float64) (bool, error) {
	if err := finiteBoolInput(value); err != nil {
		return false, err
	}
	return float64ToBool(value), nil
}

func derefPointer[T any](value *T, typeName string) (T, error) {
	if value == nil {
		var zero T
		return zero, fmt.Errorf("nil %s pointer", typeName)
	}
	return *value, nil
}

func boolFromPositive[T ~int | ~int16 | ~int32 | ~int64](value T) bool {
	return value > 0
}

func boolFromNonZero[T ~int32 | ~int64](value T) bool {
	return value != 0
}

func isTrueStringForEAV(value string) bool {
	return strings.ToLower(value) == "true" || value == "1"
}

func toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		epoch, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return time.UnixMilli(epoch), nil
		}

		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02",
			"2006-01",
		}
		for _, format := range formats {
			if parsed, err := time.Parse(format, v); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("unsupported time format: %s", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		if err := finiteBoolInput(v); err != nil {
			return false, err
		}
		return v != 0, nil
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return false, fmt.Errorf("cannot convert json.Number %q to bool: %w", v.String(), err)
		}
		if err := finiteBoolInput(f); err != nil {
			return false, err
		}
		return f != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// ToFloat64Ok is an exported helper that behaves like the legacy optimizer helper:
// it returns (float64, bool) where bool indicates success.
func ToFloat64(v any) (float64, bool) {
	return numutil.ToFloat64(v)
}
