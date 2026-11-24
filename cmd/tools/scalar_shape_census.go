package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/transform"
)

// scalar-shape-census (#696) finds eav_data records that hold array elements
// under an attribute that is not a list. Report-only update validation let
// an update give a scalar a non-empty array, which the write stored as
// element records at array indices the attribute has none of; since #696
// the write refuses it, and an update that keeps such a stored value is
// refused as stored state until the attribute is rewritten.
//
// A non-empty array_indices is no anomaly by itself: a member of an array of
// objects carries one index per array around it, and a primitive array
// recorded under its element type before #204 carries its own. So a record
// is judged against its attribute's array ancestry and its schema document,
// with the write's own rule (transform.SchemaShapes.Declared):
//
//   - verified: the document declares the attribute a non-array type, and
//     the record carries more indices than the attribute has arrays of
//     objects around it, which the registry knows (ArrayAncestors) or the
//     document shows to be none;
//   - pre-#204 primitive array: the document declares the attribute an
//     array, so extra indices are its elements (not reported);
//   - unverified: the document does not say what the attribute is, or the
//     attribute lies inside an array of objects in a schema whose registry
//     derived no ancestry, whose rows hold such indices legitimately.
//
// The census reads and reports; it never writes and repairs nothing. A row
// is repaired by an update that writes the attribute (docs/error-handling.md).

type shapeCensusClass string

const (
	shapeVerified   shapeCensusClass = "verified"
	shapeUnverified shapeCensusClass = "unverified"
)

// shapeCensusSchema is one schema the census judges: its id in eav_data,
// its active attributes with array ancestry, and its document's shapes.
type shapeCensusSchema struct {
	id     int16
	name   string
	cache  forma.SchemaAttributeCache
	shapes transform.SchemaShapes
}

// shapeCensusFinding is one attribute's records of one index depth that the
// census reports, with one example record named by row and indices only:
// the stored value is never printed.
type shapeCensusFinding struct {
	class          shapeCensusClass
	schema         string
	schemaID       int16
	attr           string
	attrID         int16
	declared       forma.ValueType
	depth          int
	ancestors      int
	records        int64
	rows           int64
	exampleRow     string
	exampleIndices string
}

func (f shapeCensusFinding) String() string {
	return fmt.Sprintf("%s: schema=%s schema_id=%d attr_id=%d attribute=%s declared=%s indices_per_record=%d array_ancestors=%d records=%d rows=%d example_row=%s example_array_indices=%q",
		f.class, f.schema, f.schemaID, f.attrID, f.attr, f.declared, f.depth, f.ancestors, f.records, f.rows, f.exampleRow, f.exampleIndices)
}

// scalarShapeCensusQuery groups every indexed record by attribute and index
// depth. A depth is one more than the commas of array_indices.
const scalarShapeCensusQuery = `
SELECT e.schema_id, e.attr_id,
	length(e.array_indices) - length(replace(e.array_indices, ',', '')) + 1 AS depth,
	COUNT(*) AS record_count, COUNT(DISTINCT e.row_id) AS row_count,
	MIN(e.row_id::text) AS example_row, MIN(e.array_indices) AS example_indices
FROM %s AS e
WHERE e.array_indices <> ''
GROUP BY e.schema_id, e.attr_id, depth
ORDER BY e.schema_id, e.attr_id, depth`

// runScalarShapeCensusQuery answers the census's findings, verified first,
// in (schema_id, attr_id, depth) order. Records of a list, of an attribute
// the schemas do not name, or of a schema not given are not this census's.
func runScalarShapeCensusQuery(ctx context.Context, q toolDBPool, eavTable string, schemas []shapeCensusSchema) ([]shapeCensusFinding, error) {
	byID := make(map[int16]shapeCensusSchema, len(schemas))
	attrs := make(map[int16]map[int16]string, len(schemas))
	for _, schema := range schemas {
		byID[schema.id] = schema
		attrs[schema.id] = make(map[int16]string, len(schema.cache))
		for name, meta := range schema.cache {
			attrs[schema.id][meta.AttributeID] = name
		}
	}
	rows, err := q.Query(ctx, fmt.Sprintf(scalarShapeCensusQuery, quoteIdentifier(eavTable)))
	if err != nil {
		return nil, fmt.Errorf("query indexed records of %s: %w", eavTable, err)
	}
	defer rows.Close()
	var findings []shapeCensusFinding
	for rows.Next() {
		var f shapeCensusFinding
		if err := rows.Scan(&f.schemaID, &f.attrID, &f.depth, &f.records, &f.rows, &f.exampleRow, &f.exampleIndices); err != nil {
			return nil, fmt.Errorf("scan indexed records of %s: %w", eavTable, err)
		}
		schema, known := byID[f.schemaID]
		name, named := attrs[f.schemaID][f.attrID]
		if !known || !named {
			continue
		}
		meta := schema.cache[name]
		f.schema, f.attr, f.declared, f.ancestors = schema.name, name, meta.ValueType, len(meta.ArrayAncestors)
		if f.class = classifyShapeRecords(schema.shapes.Declared(name, meta), meta, f.depth); f.class != "" {
			findings = append(findings, f)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate indexed records of %s: %w", eavTable, err)
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].class == shapeVerified && findings[j].class != shapeVerified })
	return findings, nil
}

// classifyShapeRecords judges records of depth indices under meta's
// attribute; "" is no finding. Only a known ancestry bounds the depth: an
// attribute without one is ShapeScalar only when the document shows no
// array of objects around it.
func classifyShapeRecords(shape transform.DeclaredShape, meta forma.AttributeMetadata, depth int) shapeCensusClass {
	switch {
	case meta.ValueType == forma.ValueTypeList || depth <= len(meta.ArrayAncestors):
		return ""
	case shape == transform.ShapeScalar:
		return shapeVerified
	case shape == transform.ShapeArray:
		return ""
	default:
		return shapeUnverified
	}
}

// loadShapeCensusSchemas loads every schema the registry table lists the
// way the server does (schemameta.NewFileSchemaRegistryContext): the
// attribute cache with array ancestry, and the document.
func loadShapeCensusSchemas(ctx context.Context, pool *pgxpool.Pool, schemaTable, schemaDir string) ([]shapeCensusSchema, error) {
	registry, err := schemameta.NewFileSchemaRegistryContext(ctx, pool, schemaTable, schemaDir)
	if err != nil {
		return nil, fmt.Errorf("load schema registry: %w", err)
	}
	var schemas []shapeCensusSchema
	for _, name := range registry.ListSchemas() {
		id, cache, err := registry.GetSchemaAttributeCacheByName(name)
		if err != nil {
			return nil, fmt.Errorf("load attributes of schema %s: %w", name, err)
		}
		schemas = append(schemas, shapeCensusSchema{id: id, name: name, cache: cache, shapes: transform.SchemaShapesOf(registry, id)})
	}
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].id < schemas[j].id })
	return schemas, nil
}

// shapeCensusFound is the exit status of a census that verified a finding.
type shapeCensusFound struct{ verified int }

func (e *shapeCensusFound) Error() string {
	return fmt.Sprintf("scalar-shape-census found %d attribute(s) with array elements stored under a scalar", e.verified)
}

func (e *shapeCensusFound) ExitCode() int { return 2 }

func runScalarShapeCensus(ctx context.Context, args []string) error {
	return runScalarShapeCensusOut(ctx, args, os.Stdout)
}

func runScalarShapeCensusOut(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("scalar-shape-census", flag.ContinueOnError)
	flags.SetOutput(out)
	flags.Usage = func() {
		fmt.Fprintln(out, "Usage: forma-tools scalar-shape-census [options]")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Options:")
		flags.PrintDefaults()
	}
	portDefault, portErr := bootstrap.EnvInt("DB_PORT", 5432)
	var pg postgresFlags
	pg.register(flags, postgresFlagOptions{
		hostFlag: "db-host", portFlag: "db-port", userFlag: "db-user", passwordFlag: "db-password",
		databaseFlag: "db-name", sslModeFlag: "db-ssl-mode",
		hostDefault: bootstrap.Env("DB_HOST", "localhost"), portDefault: portDefault,
		userDefault: bootstrap.Env("DB_USER", "postgres"), passwordDefault: bootstrap.Env("DB_PASSWORD", "postgres"),
		databaseDefault: bootstrap.Env("DB_NAME", "forma"), sslModeDefault: bootstrap.Env("DB_SSL_MODE", "disable"),
		hostUsage: "database host", portUsage: "database port", userUsage: "database user",
		passwordUsage: "database password", databaseUsage: "database name", sslModeUsage: "database sslmode",
	})
	var schemaRegistry schemaRegistryFlags
	schemaRegistry.register(flags, true)
	eavTable := flags.String("eav-table", bootstrap.Env("EAV_TABLE", "eav_data_dev"), "EAV data table to census")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
	}
	if err := schemaRegistry.validate(true); err != nil {
		return err
	}
	pool, err := toolPostgresPoolFn(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 2,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	schemas, err := loadShapeCensusSchemas(ctx, pool, schemaRegistry.table, schemaRegistry.dir)
	if err != nil {
		return err
	}
	findings, err := runScalarShapeCensusQuery(ctx, pool, *eavTable, schemas)
	if err != nil {
		return err
	}
	return reportShapeCensus(out, *eavTable, len(schemas), findings)
}

// reportShapeCensus prints the findings and answers the exit status: 2 when
// one is verified, success otherwise, an unverified finding included.
func reportShapeCensus(out io.Writer, eavTable string, schemaCount int, findings []shapeCensusFinding) error {
	verified := 0
	for _, f := range findings {
		if f.class == shapeVerified {
			verified++
		}
	}
	fmt.Fprintf(out, "scalar-shape-census of %s over %d schema(s): %d verified, %d unverified\n",
		eavTable, schemaCount, verified, len(findings)-verified)
	for _, f := range findings {
		fmt.Fprintf(out, "- %s\n", f)
	}
	if verified > 0 {
		return &shapeCensusFound{verified: verified}
	}
	return nil
}
