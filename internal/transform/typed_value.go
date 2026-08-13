package transform

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
)

func populateTypedValue(attr *model.EAVRecord, attrName string, value any, meta forma.AttributeMetadata) (bool, error) {
	// The converter's own text is published: it describes the caller's value
	// ("cannot convert string to float64") and is the whole point of the 400.
	handleConversionError := func(err error) (bool, error) {
		return false, forma.InvalidInputf(
			"invalid value for attribute '%s' (attrID=%d): %v",
			attrName, meta.AttributeID, err)
	}

	switch meta.ValueType {
	case forma.ValueTypeUUID:
		uuidVal, isUUID := toUUID(value)
		if !isUUID {
			return handleConversionError(fmt.Errorf("invalid UUID value: %v", value))
		}
		strVal := uuidVal.String()
		attr.ValueText = &strVal
	case forma.ValueTypeText:
		strVal, err := toString(value)
		if err != nil {
			return handleConversionError(err)
		}
		attr.ValueText = &strVal
	case forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		numVal, err := numutil.Float64(value)
		if err != nil {
			return handleConversionError(err)
		}
		if numVal, err = finiteForEAV(numVal); err != nil {
			return handleConversionError(err)
		}
		attr.ValueNumeric = &numVal
		if meta.ValueType == forma.ValueTypeBigInt {
			if exact, ok := numutil.Int64Exact(value); ok {
				attr.ValueInt64 = &exact
			}
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		timeVal, err := toTime(value)
		if err != nil {
			return handleConversionError(err)
		}
		unixMillis := timeToUnixMillisFloat64(timeVal)
		attr.ValueNumeric = &unixMillis
		exactMs := timeVal.UnixMilli()
		attr.ValueInt64 = &exactMs
	case forma.ValueTypeBool:
		boolVal, err := toBool(value)
		if err != nil {
			return handleConversionError(err)
		}
		floatBool := boolToFloat64(boolVal)
		attr.ValueNumeric = &floatBool
	case forma.ValueTypeList:
		// flattenToAttributes already decomposed the array into one call per
		// element with ArrayIndices set; type the single scalar element by the
		// declared items type. Parquet LIST reconstruction is positional, so
		// only flat (single-index) lists are representable across tiers (#204).
		if attr.ArrayIndices == "" {
			return handleConversionError(fmt.Errorf("list attribute requires an array value, got scalar %v", value))
		}
		if strings.Contains(attr.ArrayIndices, ",") {
			return handleConversionError(fmt.Errorf("multi-dimensional array not supported for list attribute (indices %q)", attr.ArrayIndices))
		}
		elemMeta := meta
		elemMeta.ValueType = meta.EffectiveItemsType()
		if elemMeta.ValueType == forma.ValueTypeList {
			return handleConversionError(fmt.Errorf("items_type 'list' is not supported"))
		}
		return populateTypedValue(attr, attrName, value, elemMeta)
	default:
		return handleConversionError(fmt.Errorf("unsupported value type '%s'", meta.ValueType))
	}

	return true, nil
}

func toString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// toInt64ExactForEAV mirrors numutil.Int64Exact but also accepts the pointer
// shapes ToEAVRecord tolerates for numeric values (#282): every pointer type
// toFloat64ForEAV dereferences must also reach the exact path, or the value
// silently rides the float64 fallback and rounds above 2^53. *float32 is
// deliberately absent: numutil.Int64Exact has no float32 case, and a float32
// cannot hold an integer wide enough to need the exact sidecar.
func toInt64ExactForEAV(value any) (int64, bool) {
	switch p := value.(type) {
	case *int64:
		return derefInt64Exact(p)
	case *int:
		return derefInt64Exact(p)
	case *int16:
		return derefInt64Exact(p)
	case *int32:
		return derefInt64Exact(p)
	case *float64:
		return derefInt64Exact(p)
	case *string:
		return derefInt64Exact(p)
	default:
		return numutil.Int64Exact(value)
	}
}

// derefInt64Exact dereferences a pointer shape and delegates to
// numutil.Int64Exact; a nil pointer reports not-exact so the caller keeps the
// existing nil handling of the float64 path.
func derefInt64Exact[T any](p *T) (int64, bool) {
	if p == nil {
		return 0, false
	}
	return numutil.Int64Exact(*p)
}
