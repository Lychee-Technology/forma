package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
// recorded under its element type before #204 carries one more of its own.
// So a record is judged against how many indices its attribute legitimately
// carries: what its schema document declares under every reading
// (transform.SchemaShapes, the write's own reading), or the registry's array
// ancestry where that says more (ArrayAncestors).
//
//   - verified: the record carries more indices than any reading of the
//     document admits, and every reading declares the innermost level a
//     single value, so the extra ones index
//     an array stored where a single value belongs: under a scalar, or as an
//     element of a pre-#204 primitive array;
//   - unverified: it carries more, but the document does not say what the
//     attribute's value is there (no document, a $ref to a file the registry
//     does not serve, a position it leaves open), or only some reading of
//     it admits that many: a parent declared an object in one alternative
//     and an array of objects in another, which a record's indices do not
//     tell apart;
//   - no finding: it carries no more (a member at its array ancestry's
//     depth, a pre-#204 array's own elements), or it is a list's.
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
// census reports, an attribute/depth group, with one example record named by
// row and indices only: the stored value is never printed.
type shapeCensusFinding struct {
	class          shapeCensusClass
	schema         string
	schemaID       int16
	attr           string
	attrID         int16
	declared       forma.ValueType
	depth          int
	legitimate     int
	records        int64
	rows           int64
	exampleRow     string
	exampleIndices string
}

func (f shapeCensusFinding) String() string {
	return fmt.Sprintf("%s: schema=%s schema_id=%d attr_id=%d attribute=%s declared=%s indices_per_record=%d legitimate_indices=%d records=%d rows=%d example_row=%s example_array_indices=%q",
		f.class, f.schema, f.schemaID, f.attrID, f.attr, f.declared, f.depth, f.legitimate, f.records, f.rows, f.exampleRow, f.exampleIndices)
}

// scalarShapeCensusQuery groups every indexed record by attribute and index
// depth. A depth is one more than the commas of array_indices. The example
// is one record of the group, the smallest by row and then by indices: a
// row_id's text has a fixed length and no space, so the smallest of row and
// indices joined by a space is that record's.
const scalarShapeCensusQuery = `
SELECT e.schema_id, e.attr_id,
	length(e.array_indices) - length(replace(e.array_indices, ',', '')) + 1 AS depth,
	COUNT(*) AS record_count, COUNT(DISTINCT e.row_id) AS row_count,
	MIN(e.row_id::text || ' ' || e.array_indices) AS example
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
		var example string
		if err := rows.Scan(&f.schemaID, &f.attrID, &f.depth, &f.records, &f.rows, &example); err != nil {
			return nil, fmt.Errorf("scan indexed records of %s: %w", eavTable, err)
		}
		f.exampleRow, f.exampleIndices, _ = strings.Cut(example, " ")
		schema, known := byID[f.schemaID]
		name, named := attrs[f.schemaID][f.attrID]
		if !known || !named {
			continue
		}
		meta := schema.cache[name]
		f.schema, f.attr, f.declared = schema.name, name, meta.ValueType
		if f.class, f.legitimate = classifyShapeRecords(schema.shapes.Declared(name), meta, f.depth); f.class != "" {
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
// attribute, whose document declares declared, and answers how many indices
// its records legitimately carry under every reading of the document; ""
// is no finding. A depth only some reading admits, where a parent is an
// object in one alternative and an array of objects in another, is
// unverified: the record's indices do not say which the row took.
func classifyShapeRecords(declared transform.Declaration, meta forma.AttributeMetadata, depth int) (shapeCensusClass, int) {
	sure, most, exact := declared.Indices()
	ancestry := len(meta.ArrayAncestors)
	legitimate := max(sure, ancestry)
	switch {
	case meta.ValueType == forma.ValueTypeList || depth <= legitimate:
		return "", legitimate
	case exact && depth > max(most, ancestry):
		return shapeVerified, legitimate
	default:
		return shapeUnverified, legitimate
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

// shapeCensusFound is the exit status of a census that verified a finding:
// how many attributes, and how many attribute/depth groups of them.
type shapeCensusFound struct{ attributes, groups int }

func (e *shapeCensusFound) Error() string {
	return fmt.Sprintf("scalar-shape-census found arrays stored where a single value belongs in %d attribute(s) (%d attribute/depth group(s))",
		e.attributes, e.groups)
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
		userDefault: bootstrap.Env("DB_USER", "postgres"), passwordDefault: "",
		databaseDefault: bootstrap.Env("DB_NAME", "forma"), sslModeDefault: bootstrap.Env("DB_SSL_MODE", "disable"),
		hostUsage: "database host", portUsage: "database port", userUsage: "database user",
		passwordUsage: "database password (default: $DB_PASSWORD, else postgres)", databaseUsage: "database name", sslModeUsage: "database sslmode",
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
	// The password's fallback is resolved only now, so no help or usage
	// text prints it as the flag's default (#696 review).
	if pg.password == "" {
		pg.password = bootstrap.Env("DB_PASSWORD", "postgres")
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
// one is verified, success otherwise, an unverified finding included. Each
// finding is one attribute/depth group; an attribute with records at two
// depths is two.
func reportShapeCensus(out io.Writer, eavTable string, schemaCount int, findings []shapeCensusFinding) error {
	verified := 0
	attributes := make(map[[2]int16]struct{})
	for _, f := range findings {
		if f.class == shapeVerified {
			verified++
			attributes[[2]int16{f.schemaID, f.attrID}] = struct{}{}
		}
	}
	fmt.Fprintf(out, "scalar-shape-census of %s over %d schema(s): %d verified, %d unverified attribute/depth group(s); %d attribute(s) verified\n",
		eavTable, schemaCount, verified, len(findings)-verified, len(attributes))
	for _, f := range findings {
		fmt.Fprintf(out, "- %s\n", f)
	}
	if verified > 0 {
		return &shapeCensusFound{attributes: len(attributes), groups: verified}
	}
	return nil
}
