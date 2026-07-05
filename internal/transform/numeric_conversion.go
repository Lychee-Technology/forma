package transform

import "github.com/lychee-technology/forma/internal/numutil"

func requiredFloat64FromPointer[T numutil.NumericScalar](value *T, typeName string) (float64, error) {
	scalar, err := derefPointer(value, typeName)
	if err != nil {
		return 0, err
	}
	return numutil.Float64(scalar)
}
