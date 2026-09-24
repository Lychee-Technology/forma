package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/model"
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
	// requeue re-marks every requeueable row dirty so the next flush
	// re-exports it (widthaudit.Class.Requeueable).
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
		changeLogTable: flags.String("change-log-table", bootstrap.Env("CHANGE_LOG_TABLE", "change_log_dev"),
			"change_log table the integer-width census reads flush state from; empty skips it (no CDC)"),
		entityMainTable: flags.String("entity-main-table", bootstrap.Env("ENTITY_MAIN_TABLE", "entity_main_dev"),
			"entity main table -requeue-stale-width-exports advances row versions in"),
		cutover: flags.String("width-export-cutover", "",
			"RFC3339 time from which cdc-flush ran the #384 storage-width export; rows last flushed before it fail as stale exports"),
		requeue: flags.Bool("requeue-stale-width-exports", false,
			"re-mark every exported out-of-width smallint/integer row for re-flush so the next cdc-flush re-exports it"),
	}
}

func (f widthAuditFlags) options() (widthAuditOptions, error) {
	cutover, err := parseWidthExportCutover(*f.cutover)
	if err != nil {
		return widthAuditOptions{}, err
	}
	if *f.requeue && *f.changeLogTable == "" {
		return widthAuditOptions{}, errors.New("-requeue-stale-width-exports needs -change-log-table: a requeue is a change_log entry")
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

// checkIntegerWidthExports reports EAV rows whose value does not fit the
// declared integer width, classified by what the tiers serve for them (#501).
// With requeue set, it repairs the stale-export classes first and reports
// those rows as requeued instead.
func (v schemaConsistencyValidator) checkIntegerWidthExports(ctx context.Context, cache *schemameta.MetadataCache) ([]validationIssue, error) {
	tables := widthaudit.Tables{EAV: v.eavTable, ChangeLog: v.widths.changeLogTable}
	findings, err := widthaudit.Census(ctx, v.pool, tables, widthaudit.Targets(cache))
	if err != nil {
		return nil, fmt.Errorf("integer-width census: %w", err)
	}

	requeued := map[widthRowKey]bool{}
	missing := map[widthRowKey]bool{}
	if v.widths.requeue {
		requeued, missing, err = v.requeueStaleWidthExports(ctx, cache, findings)
		if err != nil {
			return nil, err
		}
	}

	var issues []validationIssue
	for _, f := range findings {
		class := f.Classify(v.widths.cutoverMillis)
		switch {
		case requeued[rowKeyOf(f)] && class.Requeueable():
			issues = append(issues, widthIssue("EAV rows requeued for re-flush under the #384 storage-width export in "+v.eavTable, f, severityInfo))
		case missing[rowKeyOf(f)] && class.Requeueable():
			issues = append(issues, widthIssue("EAV rows with no entity_main row, which cannot be requeued, in "+v.eavTable, f, severityError))
		case class == widthaudit.ClassStaleExport:
			issues = append(issues, widthIssue("EAV integer values whose parquet copy predates the #384 storage-width export in "+v.eavTable, f, severityError))
		case class == widthaudit.ClassStaleCandidate:
			issues = append(issues, widthIssue("exported EAV integer values outside the declared width, which may predate the #384 storage-width export (pass -width-export-cutover to confirm), in "+v.eavTable, f, severityInfo))
		case class == widthaudit.ClassBigIntOutOfContract:
			issues = append(issues, widthIssue("bigint EAV values outside int64 or non-integral in "+v.eavTable, f, severityError))
		}
	}
	return issues, nil
}

// requeueStaleWidthExports requeues each row that holds a requeueable
// finding, once per row, through the repository's version-advancing requeue.
// A row with no entity_main row is collected in missing, not treated as
// fatal. Any other failure aborts, and rows already requeued stay requeued,
// which is harmless.
func (v schemaConsistencyValidator) requeueStaleWidthExports(ctx context.Context, cache *schemameta.MetadataCache, findings []widthaudit.Finding) (requeued, missing map[widthRowKey]bool, err error) {
	repo := internal.NewDBPersistentRecordRepository(v.pool, cache)
	tables := model.StorageTables{EntityMain: v.widths.entityMainTable, EAVData: v.eavTable, ChangeLog: v.widths.changeLogTable}
	requeued, missing = map[widthRowKey]bool{}, map[widthRowKey]bool{}
	for _, f := range findings {
		key := rowKeyOf(f)
		if !f.Classify(v.widths.cutoverMillis).Requeueable() || requeued[key] || missing[key] {
			continue
		}
		err := repo.RequeueForFlush(ctx, tables, f.SchemaID, f.RowID)
		if errors.Is(err, forma.ErrNotFound) {
			missing[key] = true
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("requeue schema=%s row_id=%s for re-flush: %w", f.SchemaName, f.RowID, err)
		}
		requeued[key] = true
	}
	if len(requeued) > 0 {
		fmt.Fprintf(v.out, "requeued %d row(s) for re-flush; run cdc-flush to re-export them\n", len(requeued))
	}
	return requeued, missing, nil
}

// widthRowKey is a row's identity in change_log: row_id is only unique
// within a schema.
type widthRowKey struct {
	schemaID int16
	rowID    uuid.UUID
}

func rowKeyOf(f widthaudit.Finding) widthRowKey {
	return widthRowKey{schemaID: f.SchemaID, rowID: f.RowID}
}

func widthIssue(category string, f widthaudit.Finding, severity issueSeverity) validationIssue {
	details := fmt.Sprintf("schema=%s schema_id=%d attr_id=%d attribute=%s declared=%s row_id=%s",
		f.SchemaName, f.SchemaID, f.AttrID, f.AttrName, f.Declared, f.RowID)
	if f.ArrayIndices != "" {
		details += " array_indices=" + f.ArrayIndices
	}
	details += " value=" + f.StoredValue
	if f.LastFlushedAt > 0 {
		details += fmt.Sprintf(" last_flushed_at=%d", f.LastFlushedAt)
	}
	return validationIssue{category: category, details: details, severity: severity}
}
