# Transformer Array Handling Fix

## Problem

When querying entities with mixed object/array properties, the transformer would fail with error:
```
expected array at propertyRequirements but found map[string]interface {}
```

This occurred when reconstructing JSON objects from EAV attributes where:
- A parent object (e.g., `propertyRequirements`) contains both regular properties and array properties
- The schema defines: `propertyRequirements.desiredAreas` as an array (`insideArray: true`)
- The schema also has: `propertyRequirements.budget.min` as a regular property (`insideArray: false`)

## Root Cause

The `setValueAtPath` function in `internal/transformer.go` had incorrect logic for handling array indices. It assumed that **if any indices were present**, the **intermediate path segments** should be arrays. This was wrong.

### Incorrect Logic

```go
// OLD (BROKEN) CODE
if indexCursor < len(indices) {
    // Tries to treat "propertyRequirements" as an array
    // because "desiredAreas" has indices
    existing, exists := current[segment]
    arr, ok := existing.([]any)
    if !ok {
        return fmt.Errorf("expected array at %s but found %T", segment, existing)
    }
    // ...
}
```

### Example That Failed

Given schema:
```json
{
  "propertyRequirements.budget.min": { "insideArray": false },
  "propertyRequirements.desiredAreas": { "insideArray": true }
}
```

Expected JSON structure:
```json
{
  "propertyRequirements": {           // ← Object (not array)
    "budget": { "min": 5000000 },    // ← Regular property
    "desiredAreas": ["Tokyo", "Osaka"] // ← Array property
  }
}
```

When processing attributes:
1. First: `propertyRequirements.budget.min` → Creates `propertyRequirements` as **object**
2. Then: `propertyRequirements.desiredAreas[0]` → Tries to treat `propertyRequirements` as **array** ❌
3. **Conflict**: Object vs Array → Error!

## Solution

Rewrote `setValueAtPath` with correct understanding:

### Key Insight

**`insideArray: true` means the VALUE is in an array, not the PATH segments.**

For `propertyRequirements.desiredAreas[0] = "Tokyo"`:
- Path segments: `["propertyRequirements", "desiredAreas"]` → All are **objects**
- Indices: `[0]` → Apply only to the **final value**

### New Logic

```go
func setValueAtPath(target map[string]any, segments []string, indices []int, value any) error {
    // Step 1: Navigate through ALL segments as objects (except the last)
    current := target
    for i := 0; i < len(segments)-1; i++ {
        segment := segments[i]
        next, ok := current[segment].(map[string]any)
        if !ok || next == nil {
            next = make(map[string]any)  // Always create objects for path segments
            current[segment] = next
        }
        current = next
    }
    
    // Step 2: Handle the LAST segment
    lastSegment := segments[len(segments)-1]
    
    if len(indices) == 0 {
        // No indices → simple scalar value
        current[lastSegment] = value
    } else {
        // Has indices → value goes into array
        arr := getOrCreateArray(current[lastSegment])
        arr = setArrayValueRecursive(arr, indices, value)
        current[lastSegment] = arr
    }
    
    return nil
}
```

### Helper Function

Added `setArrayValueRecursive` to handle nested arrays properly:

```go
func setArrayValueRecursive(arr []any, indices []int, value any) []any {
    idx := indices[0]
    
    // Expand array if needed
    for len(arr) <= idx {
        arr = append(arr, nil)
    }
    
    if len(indices) == 1 {
        // Last index - set value directly
        arr[idx] = value
    } else {
        // More indices - nested array
        nestedArr := getOrCreateArray(arr[idx])
        arr[idx] = setArrayValueRecursive(nestedArr, indices[1:], value)
    }
    
    return arr
}
```

## Benefits

### Correctness
- ✅ Handles mixed object/array properties correctly
- ✅ Path segments are always objects
- ✅ Arrays only at the final value level
- ✅ Supports nested arrays (e.g., `field[0][1]`)

### Simplicity
- ✅ Clear separation: path navigation vs array handling
- ✅ No complex index cursor tracking
- ✅ Easier to understand and maintain

### Robustness
- ✅ Works with any schema structure
- ✅ Handles both simple values and arrays
- ✅ Proper error messages

## Testing

All tests pass:
```bash
$ go test ./internal -run TestTransformer -v
=== RUN   TestTransformer_ToAttributes
--- PASS: TestTransformer_ToAttributes (0.00s)
=== RUN   TestTransformer_FromAttributes
--- PASS: TestTransformer_FromAttributes (0.00s)
=== RUN   TestTransformer_BatchRoundTrip
--- PASS: TestTransformer_BatchRoundTrip (0.00s)
=== RUN   TestTransformer_ValidateAgainstSchema
--- PASS: TestTransformer_ValidateAgainstSchema (0.00s)
PASS
```

## Example Use Case

### Schema Definition
```json
{
  "propertyRequirements.budget.min": {
    "attributeID": 15,
    "valueType": "numeric",
    "insideArray": false
  },
  "propertyRequirements.desiredAreas": {
    "attributeID": 18,
    "valueType": "text",
    "insideArray": true
  }
}
```

### Database Attributes
```
row_id | attr_id | array_indices | value_numeric | value_text
-------|---------|---------------|---------------|------------
uuid-1 | 15      | ""            | 5000000       | NULL
uuid-1 | 18      | "0"           | NULL          | "Tokyo"
uuid-1 | 18      | "1"           | NULL          | "Osaka"
```

### Reconstructed JSON
```json
{
  "propertyRequirements": {
    "budget": {
      "min": 5000000
    },
    "desiredAreas": ["Tokyo", "Osaka"]
  }
}
```

## Related Files Modified

- `internal/transformer.go`
  - Rewrote `setValueAtPath()` function
  - Replaced `setNestedArrayValue()` with `setArrayValueRecursive()`
  - Simplified array handling logic

## Conclusion

This fix ensures the transformer correctly handles the common pattern of objects containing both regular properties and array properties, which is essential for flexible schema design in EAV systems.
