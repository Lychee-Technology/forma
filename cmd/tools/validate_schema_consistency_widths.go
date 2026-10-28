package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// widthAuditOptions configures the integer-width census (#501).
type widthAuditOptions struct {
	changeLogTable  string
	entityMainTable string
	// cutoverMillis is the epoch millisecond from which cdc-flush ran the
	// #384 storage-width export. 0 means it is unknown.
	cutoverMillis int64
	// requeue is the retired -requeue-stale-width-exports. Since #729 no
	// finding is repaired by a re-flush, so it only prints why it requeued
	// nothing; the flag stays so scripts that pass it still run.
	requeue bool
}

type widthAuditFlags struct {
	changeLogTable  *string
	entityMainTable *string
	cutover         *string
	requeue         *bool
}

func registerWidthAuditFlags(flags *flag.FlagSet) widthAuditFlags {
	return widthAuditFlags{
		changeLogTable: flags.String("change-log-table", bootstrap.EnvAllowEmpty("CHANGE_LOG_TABLE", "change_log_dev"),
			"change_log table the integer-width census reads flush state from; empty (or CHANGE_LOG_TABLE= set empty) skips it (no CDC)"),
		entityMainTable: flags.String("entity-main-table", bootstrap.Env("ENTITY_MAIN_TABLE", "entity_main_dev"),
			"entity main table the integer-width census reads the columns of integers bound to a wider column from (#618, #708)"),
		cutover: flags.String("width-export-cutover", "",
			"RFC3339 time from which cdc-flush ran the #384 storage-width export; EAV rows last flushed before it are reported as also holding a stale parquet copy"),
		requeue: flags.Bool("requeue-stale-width-exports", false,
			"retired (#729): requeues nothing, since every width finding is a stored value only rewriting the attribute repairs"),
	}
}

func (f widthAuditFlags) options() (widthAuditOptions, error) {
	cutover, err := parseWidthExportCutover(*f.cutover)
	if err != nil {
		return widthAuditOptions{}, err
	}
	return widthAuditOptions{
		changeLogTable:  *f.changeLogTable,
		entityMainTable: *f.entityMainTable,
		cutoverMillis:   cutover,
		requeue:         *f.requeue,
	}, nil
}

// parseWidthExportCutover reads the -width-export-cutover flag. Empty means
// unknown.
func parseWidthExportCutover(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	cutover, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return 0, fmt.Errorf("invalid -width-export-cutover %q: want an RFC3339 time such as 2026-08-29T00:00:00Z: %w", value, err)
	}
	return cutover.UnixMilli(), nil
}

// checkIntegerWidthExports reports every stored integer outside its
// contract as a failure: an eav_data smallint/integer image its declared
// type cannot hold, an EAV-only attribute's or one a column-bound attribute
// kept from before its promotion (#729), with whether its parquet copy also predates the
// #384 export (#501); a bigint's float64 image past ±2^53 or non-integral,
// in eav_data or a double_* column (#618); and a smallint or integer image
// a wider bound column holds that its declared type cannot (#708). Each is
// repaired by rewriting the attribute, never by a re-flush.
func (v schemaConsistencyValidator) checkIntegerWidthExports(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	tables := widthaudit.Tables{EAV: v.eavTable, EntityMain: v.widths.entityMainTable, ChangeLog: v.widths.changeLogTable}
	findings, err := widthaudit.Census(ctx, v.pool, tables, widthaudit.TargetsWithRetainedEAV(cache))
	if err != nil {
		return nil, fmt.Errorf("integer-width census: %w", err)
	}
	if v.widths.requeue {
		fmt.Fprintln(v.out, "-requeue-stale-width-exports requeued nothing: since #729 the OLTP read refuses every out-of-width "+
			"EAV smallint/integer, so a re-flush repairs no finding; rewrite the attribute, then run cdc-flush")
	}

	var issues []validationIssue
	for _, f := range findings {
		switch f.Classify() {
		case widthaudit.ClassEAVIntegerOutOfContract:
			issues = append(issues, widthIssue(eavIntegerCategory(f, v.widths.cutoverMillis, v.eavTable), f, severityError))
		case widthaudit.ClassBigIntOutOfContract:
			if f.Column != "" {
				issues = append(issues, widthIssue("bigint values in double columns outside ±2^53 (the float64-exact range) or non-integral in "+v.widths.entityMainTable, f, severityError))
				continue
			}
			issues = append(issues, widthIssue("bigint EAV values outside ±2^53 (the float64-exact range) or non-integral in "+v.eavTable, f, severityError))
		case widthaudit.ClassBoundIntegerOutOfContract:
			issues = append(issues, widthIssue("smallint/integer values in wider bound columns outside the declared range or non-integral, which the OLTP read refuses, in "+v.widths.entityMainTable, f, severityError))
		}
	}
	return issues, nil
}

// eavIntegerCategory names an EAV smallint/integer finding, telling apart a
// row whose parquet copy also predates the #384 export: until the rewrite's
// flush replaces it, the federated route serves that copy's NULL or rounded
// value instead of refusing the row.
func eavIntegerCategory(f widthaudit.Finding, cutoverMillis int64, eavTable string) string {
	const refused = "smallint/integer EAV values outside the declared range or non-integral, which the OLTP read refuses"
	if f.ExportPredates(cutoverMillis) {
		return refused + ", whose parquet copy also predates the #384 storage-width export, in " + eavTable
	}
	return refused + ", in " + eavTable
}

func widthIssue(category string, f widthaudit.Finding, severity issueSeverity) validationIssue {
	details := fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s declared=%s row_id=%s",
		f.SchemaName, f.SchemaID, f.AttrID, f.AttrName, f.Declared, f.RowID)
	if f.Column != "" {
		details += " column=" + f.Column
	}
	if f.ArrayIndices != "" {
		details += " array_indices=" + f.ArrayIndices
	}
	details += " value=" + f.StoredValue
	if f.LastFlushedAt > 0 {
		details += fmt.Sprintf(" last_flushed_at=%d", f.LastFlushedAt)
	}
	return validationIssue{category: category, details: details, severity: severity}
}
