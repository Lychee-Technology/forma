package main

import "github.com/lychee-technology/forma/internal/cdc"

// cdcSharedFlags holds the parsed flag values that cdc-flush and cdc-init map
// onto CDCConfig identically (#445). Each tool fills it once from its own flag
// set and adds only its tool-specific fields, so the two config builders can
// no longer drift apart on the shared block.
//
// It is a struct with named fields rather than a positional parameter list on
// purpose: the shared block has runs of same-typed values (the two table
// names, batchSize and estimatedRowBytes), and a positional list let a
// transposition of two of them compile silently.
type cdcSharedFlags struct {
	pg                postgresFlags
	password          string
	duck              duckExportFlags
	s3                s3Flags
	changeLogTable    string
	entityMainTable   string
	eavDataTable      string
	batchSize         int
	estimatedRowBytes int
}

// cdcChangeLogTableUsage is the -change-log-table usage of both CDC writers:
// a flush selects and marks the table's unflushed entries, and an init hands
// the exported rows' entries off in it, so both must name the table init-db
// created (its -change-log-table, CHANGE_LOG_TABLE, change_log_dev by
// default).
const cdcChangeLogTableUsage = "Change log table name: the table init-db created (init-db's --change-log-table; " +
	"its default is change_log_dev). cdc-flush and cdc-init of one deployment must name the same table"

// defaultCDCDataPrefix is the -s3-prefix default of both cdc-flush and
// cdc-init, and the -data-prefix default of the compactor and
// manifest-reconcile.
const defaultCDCDataPrefix = "data"

// cdcDataPrefixUsage is the -s3-prefix usage of both CDC writers: they must
// name the same prefix, since a flush generation lists the init's base
// objects and a manifest that lists a key under another prefix is refused.
const cdcDataPrefixUsage = "S3 data prefix of the V2 object layout: parquet objects under {prefix}/{schema_id}/e{epoch}/{base|delta}/, " +
	"manifests under {prefix}/{schema_id}/_manifests/. cdc-flush and cdc-init of one deployment must name the same prefix"

// sharedConfig returns a CDCConfig holding only the shared block. It does not
// call WithDefaults: callers add their tool-specific fields first and apply
// WithDefaults last, so a default never masks a field the caller still sets.
// PGUseIAM is deliberately not part of the shared block: only cdc-flush
// registers -pg-use-iam.
func (s cdcSharedFlags) sharedConfig() cdc.CDCConfig {
	return cdc.CDCConfig{
		PGHost:                  s.pg.host,
		PGPort:                  s.pg.port,
		PGUser:                  s.pg.user,
		PGPassword:              s.password,
		PGDB:                    s.pg.database,
		PGSSLMode:               s.pg.sslMode,
		DuckDBPath:              s.duck.duckDBPath,
		DuckThreads:             s.duck.duckThreads,
		DuckMemLimit:            s.duck.duckMemLimit,
		QueryTimeout:            s.duck.queryTimeout,
		ParquetCompression:      s.duck.parquetCompression,
		ParquetCompressionLevel: s.duck.parquetCompressionLevel,
		S3Bucket:                s.s3.bucket,
		S3Prefix:                s.s3.prefix,
		S3Endpoint:              s.s3.endpoint,
		S3Region:                s.s3.region,
		S3UseSSL:                s.s3.useSSL,
		S3UsePath:               s.s3.usePath,
		ChangeLogTable:          s.changeLogTable,
		EntityMainTable:         s.entityMainTable,
		EAVDataTable:            s.eavDataTable,
		BatchSize:               s.batchSize,
		EstimatedRowBytes:       s.estimatedRowBytes,
	}
}
