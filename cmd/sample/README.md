# CSV Import Benchmark Tool

A command-line tool for importing CSV data into Forma schemas with support for custom field mappings and data transformations.

## Features

- **CSV to Schema Mapping**: Map CSV columns to nested schema paths (e.g., `contact.name`, `items[0].id`)
- **Built-in Transformers**: Type conversions (int, float, bool, date, datetime), string operations (split, trim, case), enum validation
- **Custom Transformers**: Define your own transformation functions
- **Error Handling**: Detailed error reporting with row numbers, column names, and error reasons
- **Dry-Run Mode**: Validate CSV data without writing to database
- **Batch Processing**: Configurable batch size for efficient bulk imports

## Usage

```bash
# Build the tool
go build ./cmd/benchmark/

# Dry-run mode (validate CSV without database)
./benchmark -csv data.csv -schema-dir ./cmd/benchmark/schemas -dry-run

# Import with database connection
./benchmark -csv data.csv -db "postgres://user:pass@localhost:5432/dbname" -schema-dir ./cmd/benchmark/schemas

# With custom batch size
./benchmark -csv data.csv -db $DATABASE_URL -batch-size 500
```

## Command Line Options

| Flag          | Default     | Description                                                 |
| ------------- | ----------- | ----------------------------------------------------------- |
| `-csv`        | (required)  | Path to CSV file to import                                  |
| `-schema-dir` | `./schemas` | Directory containing schema files                           |
| `-schema`     | `watch`     | Target schema name                                          |
| `-batch-size` | `100`       | Batch size for import operations                            |
| `-db`         | -           | PostgreSQL connection URL (or set DATABASE_URL env)         |
| `-dry-run`    | `false`     | Parse CSV and validate mappings without writing to database |
| `-verbose`    | `false`     | Enable verbose logging                                      |

## Watch Schema

The default schema is `watch` for luxury watch marketplace data.

### CSV Format for Watch Schema

```csv
id,name,price,brand,model,ref,mvmt,casem,bracem,yop,cond,sex,size,condition
0,"Audemars Piguet Royal Oak...","$43,500",Audemars Piguet,Royal Oak,26237ST.OO.1000ST.01,Automatic,Steel,Steel,2019,Unworn,Men's watch/Unisex,42 mm,
```

### Field Mappings

| CSV Column | Schema Path        | Transformer         | Description                                 |
| ---------- | ------------------ | ------------------- | ------------------------------------------- |
| `id`       | `id`               | ToInt() (required)  | Watch identifier                            |
| `name`     | `name`             | String (required)   | Watch name/title                            |
| `price`    | `price`            | ToPrice()           | Numeric price (null for "Price on request") |
| `price`    | `priceDisplay`     | String              | Original price text                         |
| `brand`    | `brand`            | String (required)   | Watch brand                                 |
| `model`    | `model`            | String              | Model name                                  |
| `ref`      | `reference`        | String              | Reference number                            |
| `mvmt`     | `movement`         | String              | Movement type                               |
| `casem`    | `caseMaterial`     | String              | Case material                               |
| `bracem`   | `braceletMaterial` | String              | Bracelet material                           |
| `yop`      | `yearOfProduction` | ToYear()            | Production year                             |
| `yop`      | `yearApproximate`  | IsYearApproximate() | Whether year is approximate                 |
| `cond`     | `condition`        | Enum()              | Condition (Unworn/New/Very good/Good/Fair)  |
| `sex`      | `gender`           | String              | Target gender                               |
| `size`     | `size`             | String              | Watch size                                  |

## Mapper Interface

The mapper interface allows you to define how CSV columns map to schema fields:

```go
// FieldMapper defines the interface for transforming CSV values
type FieldMapper interface {
    Map(csvValue string) (any, error)
}

// CSVToSchemaMapper defines the complete mapping configuration
type CSVToSchemaMapper interface {
    SchemaName() string
    Mappings() []FieldMapping
    MapRecord(csvRecord map[string]string) (map[string]any, error)
}
```

## Built-in Transformers

### Type Conversions
- `Identity()` - Pass through string unchanged
- `ToString()` - Ensure string type
- `ToInt()` - Convert to integer
- `ToInt64()` - Convert to int64
- `ToFloat64()` - Convert to float64
- `ToBool()` - Convert to boolean (accepts: true/false, 1/0, yes/no)
- `ToPrice()` - Parse price strings (e.g., "$43,500" → 43500.0, "Price on request" → nil)
- `ToYear()` - Parse year strings (e.g., "2022 (Approximation)" → 2022, "Unknown" → nil)
- `IsYearApproximate()` - Check if year is approximate

### Date/Time
- `ToDate(layout)` - Parse date with custom layout (e.g., `"2006-01-02"`)
- `ToDateTime(layout)` - Parse datetime with custom layout
- `ToDateTimeISO8601()` - Parse ISO8601 datetime (auto-detects multiple formats)

### String Operations
- `Trim()` - Trim whitespace
- `ToLower()` - Convert to lowercase
- `ToUpper()` - Convert to uppercase
- `Split(separator)` - Split string into array

### Validation
- `Enum(values...)` - Validate value is one of allowed values

### Defaults
- `Default(value)` - Use default value if empty
- `DefaultWith(value, mapper)` - Use default value if empty, otherwise apply mapper

### Custom
- `Custom(func)` - Use a custom transformation function

## Creating a Mapper

Use the fluent builder API to create mappers:

```go
mapper := NewMapperBuilder("watch").
    // Required fields
    RequiredWith("id", "id", ToInt()).
    Required("name", "name").
    Required("brand", "brand").
    
    // Price - parse and also store original
    MapWith("price", "price", ToPrice()).
    Map("price", "priceDisplay").
    
    // Model info
    Map("model", "model").
    Map("ref", "reference").
    
    // Year with approximation detection
    MapWith("yop", "yearOfProduction", ToYear()).
    MapWith("yop", "yearApproximate", IsYearApproximate()).
    
    // Enum validation
    MapWith("cond", "condition", Enum("Unworn", "New", "Very good", "Good", "Fair")).
    
    // Custom transformer
    MapWith("status", "status", Custom(func(v string) (any, error) {
        switch v {
        case "1": return "active", nil
        case "0": return "inactive", nil
        default: return v, nil
        }
    })).
    
    Build()
```

## Schema Path Syntax

The mapper supports nested paths using dot notation and array indices:

| Path                     | Description                  |
| ------------------------ | ---------------------------- |
| `name`                   | Top-level field              |
| `contact.name`           | Nested object field          |
| `items[0]`               | First element of array       |
| `items[0].id`            | Field in first array element |
| `specs.dimensions.width` | Deeply nested field          |

## Error Handling

The importer provides detailed error information for each failed row:

```
[ERROR] row 3, column "id" -> path "id": value "" - required field is empty
[ERROR] row 4, column "cond" -> path "condition": value "invalid" - invalid value "invalid": must be one of [Unworn New Very good Good Fair]
[ERROR] row 5, column "price" -> path "price": value "abc" - invalid price format
```

Error details include:
- **Row Number**: CSV row number (1-based, including header)
- **CSV Column**: The CSV column name that caused the error
- **Schema Path**: The target schema path
- **Raw Value**: The original CSV value
- **Reason**: Description of the error

## Import Result

After import, a summary is displayed:

```
Import Summary
==================================================
  Total rows:     49
  Successful:     49
  Failed:         0
  Duration:       292.791µs
```

## Adding New Schema Mappers

To add support for a new schema:

1. Create schema files in `cmd/benchmark/schemas/`:
   - `{schema_name}.json` - JSON Schema definition
   - `{schema_name}_attributes.json` - Attribute metadata

2. Create a mapper function:

```go
func NewProductMapper() CSVToSchemaMapper {
    return NewMapperBuilder("product").
        Required("sku", "sku").
        Required("name", "name").
        MapWith("price", "price", ToFloat64()).
        // ... more mappings
        Build()
}
```

3. Register it in `main.go`:

```go
switch *schemaName {
case "watch":
    mapper = NewWatchMapper()
case "product":
    mapper = NewProductMapper()
default:
    logger.Fatalf("Unknown schema: %s", *schemaName)
}
```

## License

See [LICENSE](../../LICENSE) file.
