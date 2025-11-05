# Attribute Name to ID Mapping Fix

## Problem

When querying the API with attribute name filters like:
```bash
curl "http://localhost:8080/api/v1/lead?attr_name=equals:personalInfo.name.family&attr_value=contains:Suzuki"
```

The server would crash with error:
```
ERROR: invalid input syntax for type smallint: "personalInfo.name.family" (SQLSTATE 22P02)
```

## Root Cause

The API handler was passing attribute **names** (strings) directly to the database layer, which expected attribute **IDs** (smallint integers). The query builder would try to use the attribute name where an `attr_id` column (of type smallint) was expected.

### Code Flow

1. **API Layer** (`handlers.go`): Receives `attr_name=personalInfo.name.family`
2. **Utils** (`utils.go`): `buildFilters()` creates filter with attribute name as value
3. **Repository** (`postgres_repository.go`): Maps to `attr_id` column, tries to use string in smallint column
4. **PostgreSQL**: Rejects invalid type → Error

## Solution

Modified the `buildFilters()` function to convert attribute names to IDs using the `MetadataCache`:

### Implementation Steps

1. **Added `metadataCache` to Server struct** (`main.go`)
   - Server now holds reference to metadata cache
   - Passed from factory during initialization

2. **Updated `buildFilters()` signature** (`utils.go`)
   - Added `metadataCache *internal.MetadataCache` parameter
   - Added special handling for `attr_name` parameter

3. **Implemented name-to-ID conversion** (`utils.go`)
   ```go
   if key == "attr_name" {
       // Parse attribute name from query param
       filterType, attrName, err := parseExpression(values[0])
       
       // Convert name to ID using metadata cache
       if schemaName != "" && metadataCache != nil {
           meta, ok := metadataCache.GetAttributeMeta(schemaName, attrName)
           if !ok {
               return nil, fmt.Errorf("attribute not found: %s in schema %s", attrName, schemaName)
           }
           // Use attribute ID instead of name
           filters["attr_name"] = forma.Filter{
               Field: forma.FilterFieldAttributeName,
               Type:  filterType,
               Value: meta.AttributeID,  // ← int16 instead of string
           }
       }
   }
   ```

4. **Updated all `buildFilters()` call sites** (`handlers.go`)
   - `handleQuery()`: Pass `s.metadataCache`
   - `handleSearch()`: Pass `s.metadataCache`

5. **Updated factory to return metadata cache** (`factory.go`)
   - Changed return type: `(forma.EntityManager, *internal.MetadataCache)`
   - Main function receives both values

## Benefits

### Correctness
- ✅ Attribute names are now properly converted to IDs
- ✅ Type-safe database queries (int16 for attr_id column)
- ✅ Clear error messages if attribute doesn't exist

### Performance
- ✅ O(1) lookup from in-memory cache
- ✅ No additional database queries needed
- ✅ Validation happens at API layer (fail fast)

### User Experience
- ✅ API remains unchanged - users still use attribute names
- ✅ Better error messages: "attribute not found: X in schema Y"
- ✅ Consistent behavior across all query endpoints

## Example

### Before (Broken)
```bash
curl "http://localhost:8080/api/v1/lead?attr_name=equals:personalInfo.name.family&attr_value=contains:Suzuki"
# Error: invalid input syntax for type smallint: "personalInfo.name.family"
```

### After (Fixed)
```bash
curl "http://localhost:8080/api/v1/lead?attr_name=equals:personalInfo.name.family&attr_value=contains:Suzuki"
# Success: Attribute name converted to ID=6, query executes correctly
```

### SQL Query Generated

**Before:**
```sql
WHERE attr_id = 'personalInfo.name.family'  -- ❌ Type error!
```

**After:**
```sql
WHERE attr_id = 6  -- ✅ Correct int16 value
AND value_text ILIKE '%Suzuki%'
```

## Testing

All tests pass:
```bash
$ go build ./...  # Success
$ go test ./internal/... # All pass
```

## Related Files Modified

- `cmd/server/main.go` - Added metadataCache to Server struct
- `cmd/server/factory.go` - Return metadataCache from NewEntityManager
- `cmd/server/utils.go` - Convert attr_name to attr_id in buildFilters
- `cmd/server/handlers.go` - Pass metadataCache to buildFilters calls

## Future Enhancements

1. **Support multiple attribute filters**: Currently handles single attr_name/attr_value pair
2. **Type-aware value columns**: Use metadata to select correct value_* column based on attribute type
3. **Batch attribute lookups**: Optimize for queries with many attribute filters

## Conclusion

This fix completes the metadata loading optimization by ensuring attribute names are properly converted to IDs at the API boundary, maintaining type safety throughout the query pipeline while keeping the API user-friendly with human-readable attribute names.
