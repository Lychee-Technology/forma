package transform

type numericScalar interface {
	~float64 | ~float32 | ~int | ~int16 | ~int32 | ~int64
}

func requiredFloat64FromPointer[T numericScalar](value *T, typeName string) (float64, error) {
	scalar, err := derefPointer(value, typeName)
	if err != nil {
		return 0, err
	}
	return toFloat64(scalar)
}

func optionalFloat64FromPointer[T numericScalar](value *T) (float64, bool, error) {
	if value == nil {
		return 0, true, nil
	}
	parsed, err := toFloat64(*value)
	if err != nil {
		return 0, false, err
	}
	return parsed, false, nil
}

func optionalPointerValue[T any](value *T) (T, bool) {
	if value == nil {
		var zero T
		return zero, true
	}
	return *value, false
}
