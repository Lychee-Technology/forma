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

	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
)

var runValidateSchemaConsistencyOutFn = runValidateSchemaConsistencyOut

type schemaConsistencyValidator struct {
	pool        toolDBPool
	schemaDir   string
	schemaTable string
	eavTable    string
	out         io.Writer
}

type validationIssue struct {
	category string
	details  string
}

func runValidateSchemaConsistency(ctx context.Context, args []string) error {
	return runValidateSchemaConsistencyOutFn(ctx, args, os.Stdout)
}

func runValidateSchemaConsistencyOut(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("validate-schema-consistency", flag.ContinueOnError)
	flags.SetOutput(out)
	flags.Usage = func() {
		fmt.Fprintln(out, "Usage: forma-tools validate-schema-consistency [options]")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Options:")
		flags.PrintDefaults()
	}

	var pg postgresFlags
	pg.register(flags, postgresFlagOptions{
		hostFlag:        "db-host",
		portFlag:        "db-port",
		userFlag:        "db-user",
		passwordFlag:    "db-password",
		databaseFlag:    "db-name",
		sslModeFlag:     "db-ssl-mode",
		hostDefault:     bootstrap.Env("DB_HOST", "localhost"),
		portDefault:     bootstrap.EnvInt("DB_PORT", 5432),
		userDefault:     bootstrap.Env("DB_USER", "postgres"),
		passwordDefault: bootstrap.Env("DB_PASSWORD", "postgres"),
		databaseDefault: bootstrap.Env("DB_NAME", "forma"),
		sslModeDefault:  bootstrap.Env("DB_SSL_MODE", "disable"),
		hostUsage:       "database host",
		portUsage:       "database port",
		userUsage:       "database user",
		passwordUsage:   "database password",
		databaseUsage:   "database name",
		sslModeUsage:    "database sslmode",
	})

	var schemaRegistry schemaRegistryFlags
	schemaRegistry.register(flags, true)
	eavTable := flags.String("eav-table", bootstrap.Env("EAV_TABLE", "eav_data_dev"), "EAV data table to validate")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if err := schemaRegistry.validate(true); err != nil {
		return err
	}

	pool, err := buildSchemaConsistencyPool(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 4,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	validator := schemaConsistencyValidator{
		pool:        pool,
		schemaDir:   schemaRegistry.dir,
		schemaTable: schemaRegistry.table,
		eavTable:    *eavTable,
		out:         out,
	}
	return validator.run(ctx)
}

func (v schemaConsistencyValidator) run(ctx context.Context) error {
	loader := schemameta.NewMetadataLoader(v.pool, v.schemaTable, v.schemaDir)
	cache, err := loader.LoadMetadata(ctx)
	if err != nil {
		return fmt.Errorf("load schema metadata: %w", err)
	}

	issues, err := v.collectIssues(ctx, cache)
	if err != nil {
		return err
	}

	if len(issues) == 0 {
		fmt.Fprintf(v.out, "schema consistency checks passed for %d schema(s)\n", len(cache.ListSchemas()))
		return nil
	}

	for _, issue := range issues {
		fmt.Fprintf(v.out, "- %s: %s\n", issue.category, issue.details)
	}
	return fmt.Errorf("schema consistency validation failed with %d issue(s)", len(issues))
}

func (v schemaConsistencyValidator) collectIssues(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	var issues []validationIssue

	unknownIssues, err := v.checkUnknownAttributeIDs(ctx, cache)
	if err != nil {
		return nil, err
	}
	issues = append(issues, unknownIssues...)

	storageIssues, err := v.checkStorageMismatches(ctx, cache)
	if err != nil {
		return nil, err
	}
	issues = append(issues, storageIssues...)

	sort.Slice(issues, func(i, j int) bool {
		if issues[i].category == issues[j].category {
			return issues[i].details < issues[j].details
		}
		return issues[i].category < issues[j].category
	})
	return issues, nil
}

func (v schemaConsistencyValidator) checkUnknownAttributeIDs(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	knownAttrIDs := make(map[int16]map[int16]struct{})
	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		ids := make(map[int16]struct{}, len(schemaCache))
		for _, meta := range schemaCache {
			ids[meta.AttributeID] = struct{}{}
		}
		knownAttrIDs[schemaID] = ids
	}

	query := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id, COUNT(*) AS record_count
FROM %s AS e
GROUP BY e.schema_id, e.attr_id
ORDER BY e.schema_id, e.attr_id`, quoteIdentifier(v.eavTable))

	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query unknown attribute ids: %w", err)
	}
	defer rows.Close()

	var issues []validationIssue
	for rows.Next() {
		var schemaID, attrID int16
		var count int64
		if err := rows.Scan(&schemaID, &attrID, &count); err != nil {
			return nil, fmt.Errorf("scan unknown attribute ids: %w", err)
		}
		if ids, ok := knownAttrIDs[schemaID]; ok {
			if _, exists := ids[attrID]; exists {
				continue
			}
		}
		issues = append(issues, validationIssue{
			category: "unknown attribute IDs in " + v.eavTable,
			details:  fmt.Sprintf("schema_id=%d attr_id=%d rows=%d", schemaID, attrID, count),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unknown attribute ids: %w", err)
	}
	return issues, nil
}

func (v schemaConsistencyValidator) checkStorageMismatches(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	textBacked := make(map[int16]map[int16]struct{})
	numericBacked := make(map[int16]map[int16]struct{})

	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		for _, meta := range schemaCache {
			if attributeUsesNumericColumn(meta) {
				if numericBacked[schemaID] == nil {
					numericBacked[schemaID] = make(map[int16]struct{})
				}
				numericBacked[schemaID][meta.AttributeID] = struct{}{}
			} else {
				if textBacked[schemaID] == nil {
					textBacked[schemaID] = make(map[int16]struct{})
				}
				textBacked[schemaID][meta.AttributeID] = struct{}{}
			}
		}
	}

	textQuery := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id, COUNT(*) AS record_count
FROM %s AS e WHERE e.value_text IS NOT NULL
GROUP BY e.schema_id, e.attr_id
ORDER BY e.schema_id, e.attr_id`, quoteIdentifier(v.eavTable))
	textIssues, err := v.scanStorageIssueRows(ctx, textQuery, func(schemaID, attrID int16, count int64) *validationIssue {
		if ids, ok := numericBacked[schemaID]; ok {
			if _, exists := ids[attrID]; exists {
				return &validationIssue{
					category: "numeric/date/bool attributes stored in value_text",
					details:  fmt.Sprintf("schema_id=%d attr_id=%d rows=%d", schemaID, attrID, count),
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	numericQuery := fmt.Sprintf(`
SELECT e.schema_id, e.attr_id, COUNT(*) AS record_count
FROM %s AS e WHERE e.value_numeric IS NOT NULL
GROUP BY e.schema_id, e.attr_id
ORDER BY e.schema_id, e.attr_id`, quoteIdentifier(v.eavTable))
	numericIssues, err := v.scanStorageIssueRows(ctx, numericQuery, func(schemaID, attrID int16, count int64) *validationIssue {
		if ids, ok := textBacked[schemaID]; ok {
			if _, exists := ids[attrID]; exists {
				return &validationIssue{
					category: "text/uuid/list attributes stored in value_numeric",
					details:  fmt.Sprintf("schema_id=%d attr_id=%d rows=%d", schemaID, attrID, count),
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return append(textIssues, numericIssues...), nil
}

func (v schemaConsistencyValidator) scanStorageIssueRows(ctx context.Context, query string, classify func(schemaID, attrID int16, count int64) *validationIssue) ([]validationIssue, error) {
	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query storage mismatches: %w", err)
	}
	defer rows.Close()

	var issues []validationIssue
	for rows.Next() {
		var schemaID, attrID int16
		var count int64
		if err := rows.Scan(&schemaID, &attrID, &count); err != nil {
			return nil, fmt.Errorf("scan storage mismatches: %w", err)
		}
		if issue := classify(schemaID, attrID, count); issue != nil {
			issues = append(issues, *issue)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate storage mismatches: %w", err)
	}
	return issues, nil
}

// attributeUsesNumericColumn classifies which eav_data column backs the
// attribute. List attributes store one element per row typed by items_type,
// so they classify by the element type, never the container type (#204).
func attributeUsesNumericColumn(meta forma.AttributeMetadata) bool {
	vt := meta.ValueType
	if vt == forma.ValueTypeList {
		vt = meta.EffectiveItemsType()
	}
	return valueTypeUsesNumericColumn(vt)
}

func valueTypeUsesNumericColumn(valueType forma.ValueType) bool {
	switch valueType {
	case forma.ValueTypeNumeric,
		forma.ValueTypeInteger,
		forma.ValueTypeBigInt,
		forma.ValueTypeSmallInt,
		forma.ValueTypeBool,
		forma.ValueTypeDate,
		forma.ValueTypeDateTime:
		return true
	default:
		return false
	}
}
