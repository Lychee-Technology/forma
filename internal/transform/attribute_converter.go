package transform

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
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

// ToEAVRecord converts an model.EntityAttribute to an model.EAVRecord.
//
// Its conversion failures are plain errors; the numeric and bool cases name the
// attribute by AttrID, the rest take their identity from ToEAVRecords' wrap.
// AttrID is the EAV key and the only identity this layer holds — the attribute
// name would need a metadata cache it does not take. The caller-facing carrier
// that names the attribute and wraps forma.ErrInvalidInput is raised earlier,
// at populateTypedValue (typed_value.go); reaching an error here on the write
// path means the value got past it, so it is operator-visible by design.
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
			return record, fmt.Errorf("convert to numeric for attrID %d: %w", attr.AttrID, err)
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
		exactMs, err := epochMillisOf(timeVal)
		if err != nil {
			return record, fmt.Errorf("convert to time for attrID %d: %w", attr.AttrID, err)
		}
		setEpochMillis(&record, exactMs)

	case forma.ValueTypeUUID:
		if uuidVal, ok := attr.Value.(uuid.UUID); ok {
			strVal := uuidVal.String()
			record.ValueText = &strVal
		} else {
			return record, fmt.Errorf("value type mismatch: expected uuid.UUID for uuid type")
		}

	case forma.ValueTypeBool:
		boolVal, err := boolFromAny(attr.Value)
		if err != nil {
			return record, fmt.Errorf("convert to bool for attrID %d: %w", attr.AttrID, err)
		}
		floatBool := boolToFloat64(boolVal)
		record.ValueNumeric = &floatBool

	default:
		return record, fmt.Errorf("unsupported value type: %s", attr.ValueType)
	}

	return record, nil
}

// FromEAVRecord converts an model.EAVRecord to an model.EntityAttribute.
//
// An extraction failure is a read-path consistency error (docs/error-handling.md):
// plain, operator-visible, and wrapped here with the record's full identity.
// This is the one hop that holds the row, so it is where the row gets named;
// without it a list or query enrichment read cannot say which row is corrupt
// (#405). The attribute name is added by fromEAVRecords, the hop that has it.
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
		return attr, fmt.Errorf("record %s: %w", eavRecordIdentity(record), err)
	}

	return attr, nil
}

// eavRecordIdentity renders the EAV key of a record for error messages:
// schema, row, attrID, and the array indices when the record is a list
// element (a scalar's indices are empty and are left out).
func eavRecordIdentity(record model.EAVRecord) string {
	identity := fmt.Sprintf("schema=%d row=%s attrID=%d", record.SchemaID, record.RowID, record.AttrID)
	if record.ArrayIndices != "" {
		identity += " arrayIndices=" + record.ArrayIndices
	}
	return identity
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

// FromEAVRecords converts a slice of EAVRecords to EntityAttributes.
//
// It resolves the relation roots for the required-policy check itself, which
// suits the read path (FromPersistentRecord), where this is the only
// resolution on the call. The write path's ToAttributes has already resolved
// them for its own input-side check and hands that snapshot to
// fromEAVRecords directly, so one write consults the registry once (#389).
func (c *AttributeConverter) FromEAVRecords(records []model.EAVRecord) ([]model.EntityAttribute, error) {
	if len(records) == 0 {
		return []model.EntityAttribute{}, nil
	}
	relationRoots, err := c.relationRootsFor(records[0].SchemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve relation roots for required-policy check: %w", err)
	}
	return c.fromEAVRecords(records, relationRoots)
}

// fromEAVRecords is FromEAVRecords with the relation roots already resolved.
func (c *AttributeConverter) fromEAVRecords(records []model.EAVRecord, relationRoots RelationRoots) ([]model.EntityAttribute, error) {
	if len(records) == 0 {
		return []model.EntityAttribute{}, nil
	}

	// Get schema metadata to determine value types
	schemaID := records[0].SchemaID
	cache, idToName, err := schemameta.GetSchemaMetadata(c.registry, schemaID)
	if err != nil {
		return nil, fmt.Errorf("load schema metadata for schema %d: %w", schemaID, err)
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
			// FromEAVRecord already names schema, row, and attrID; this hop
			// adds the attribute name, which only it has resolved.
			return nil, fmt.Errorf("convert attribute '%s': %w", attrName, err)
		}
		attributes = append(attributes, attr)
	}
	if len(skippedAttrIDs) > 0 {
		ids := make([]int16, 0, len(skippedAttrIDs))
		for id := range skippedAttrIDs {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		logSkippedAttrIDs(schemaID, records[0].RowID, ids)
	}

	if err := c.checkRequiredAttributes(cache, presentAttrIndices, relationRoots); err != nil {
		return nil, fmt.Errorf("required-policy check for schema %d row %s: %w", schemaID, records[0].RowID, err)
	}

	return attributes, nil
}
