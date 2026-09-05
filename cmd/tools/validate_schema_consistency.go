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

// issueSeverity separates findings that must block a pre-flight from findings
// the operator only needs to know about. severityError is the zero value, so
// every existing construction site stays a failure by default.
type issueSeverity int

const (
	severityError issueSeverity = iota
	severityInfo
)

type validationIssue struct {
	category string
	details  string
	severity issueSeverity
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

	failures, notices := partitionIssues(issues)
	v.report(failures, notices, len(cache.ListSchemas()))
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("schema consistency validation failed with %d issue(s)", len(failures))
}

// partitionIssues splits collectIssues' sorted output into the two report
// blocks while preserving order inside each.
func partitionIssues(issues []validationIssue) (failures, notices []validationIssue) {
	for _, issue := range issues {
		if issue.severity == severityInfo {
			notices = append(notices, issue)
			continue
		}
		failures = append(failures, issue)
	}
	return failures, notices
}

// report prints failures first, then the informational block. Informational
// findings are EAV rows #294 preserves under a retired attribute: a supported
// steady state, so they are surfaced for awareness and never gate the exit
// code (#341).
func (v schemaConsistencyValidator) report(failures, notices []validationIssue, schemaCount int) {
	if len(failures) == 0 {
		fmt.Fprintf(v.out, "schema consistency checks passed for %d schema(s)", schemaCount)
		if len(notices) > 0 {
			fmt.Fprintf(v.out, ", %d informational finding(s)", len(notices))
		}
		fmt.Fprintln(v.out)
	}
	for _, issue := range failures {
		fmt.Fprintf(v.out, "- %s: %s\n", issue.category, issue.details)
	}
	if len(notices) == 0 {
		return
	}
	fmt.Fprintln(v.out, "informational (not a failure):")
	for _, issue := range notices {
		fmt.Fprintf(v.out, "- %s: %s\n", issue.category, issue.details)
	}
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

	listIssues, err := v.checkScalarRowsUnderListAttrs(ctx, cache)
	if err != nil {
		return nil, err
	}
	issues = append(issues, listIssues...)

	sort.Slice(issues, func(i, j int) bool {
		if issues[i].category == issues[j].category {
			return issues[i].details < issues[j].details
		}
		return issues[i].category < issues[j].category
	})
	return issues, nil
}

// schemaLedger is the per-schema view checkUnknownAttributeIDs classifies
// against: the active attributeIDs, the retired ledger #342 records, and the
// schema name for the report line.
type schemaLedger struct {
	name    string
	active  map[int16]struct{}
	retired map[int16]string
}

func buildSchemaLedgers(cache *schemameta.MetadataCache) map[int16]schemaLedger {
	ledgers := make(map[int16]schemaLedger)
	for _, schemaName := range cache.ListSchemas() {
		schemaID, ok := cache.GetSchemaID(schemaName)
		if !ok {
			continue
		}
		schemaCache, ok := cache.GetSchemaCache(schemaName)
		if !ok {
			continue
		}
		active := make(map[int16]struct{}, len(schemaCache))
		for _, meta := range schemaCache {
			active[meta.AttributeID] = struct{}{}
		}
		ledgers[schemaID] = schemaLedger{
			name:    schemaName,
			active:  active,
			retired: cache.RetiredAttributeIDs(schemaID),
		}
	}
	return ledgers
}

func (v schemaConsistencyValidator) checkUnknownAttributeIDs(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	ledgers := buildSchemaLedgers(cache)

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
		// A schema_id absent from the registry yields the zero ledger: both maps
		// are nil, so the row falls through to the failure branch as before.
		ledger := ledgers[schemaID]
		if _, exists := ledger.active[attrID]; exists {
			continue
		}
		issues = append(issues, classifyUndecodableRows(v.eavTable, ledger, schemaID, attrID, count))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unknown attribute ids: %w", err)
	}
	return issues, nil
}

// classifyUndecodableRows splits EAV rows current metadata cannot decode into
// the two cases the migration guide names. Rows under a retired ledger entry
// were preserved by an attribute removal (#294) — an expected, supported steady
// state, reported informationally. Everything else is genuinely orphaned and
// still fails the pre-flight (#341).
func classifyUndecodableRows(eavTable string, ledger schemaLedger, schemaID, attrID int16, count int64) validationIssue {
	if attrName, ok := ledger.retired[attrID]; ok {
		return validationIssue{
			category: "preserved EAV rows for retired attributes in " + eavTable,
			details: fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s rows=%d",
				ledger.name, schemaID, attrID, attrName, count),
			severity: severityInfo,
		}
	}
	return validationIssue{
		category: "unknown attribute IDs in " + eavTable,
		details:  fmt.Sprintf("schema_id=%d attr_id=%d rows=%d", schemaID, attrID, count),
	}
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
