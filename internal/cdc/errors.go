package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"go.uber.org/zap"
)

// ErrSchemaAttrCacheUnavailable marks a CDC export that cannot proceed because a
// schema's attribute metadata cache could not be resolved — the registry lookup
// failed or the cache is empty. CDC export needs it to produce parquet the
// federated reader can consume; without it the reader fails fast with
// ErrSchemaMetadataCacheRequired (#193). It is an operator-visible failure, not
// write-path validation, so it is NOT wrapped in forma.ErrInvalidInput. An
// empty cache or a nil registry is a configuration error; a failed lookup also
// wraps the registry's error, which may be transient (#768).
var ErrSchemaAttrCacheUnavailable = errors.New("schema attribute metadata cache unavailable")

// ErrSchemaNotExportable marks a cdc-init run refused because the one schema
// its --schema-id names is a schema the parquet tiers cannot hold (#704). A
// run over every schema skips and reports such a schema instead.
var ErrSchemaNotExportable = errors.New("schema cannot be exported to the parquet tiers")

// resolveRequiredAttrCache resolves schemaID's attribute cache, treating a nil
// registry, a lookup error, or an empty cache as a hard failure. A real schema's
// cache is never empty (it maps hot fields as well as EAV attributes), so an
// empty cache means the schema is not registered — the same assumption the
// federated reader makes (internal/federated/duckdb_query.go).
func resolveRequiredAttrCache(reg forma.SchemaRegistry, schemaID int16) (forma.SchemaAttributeCache, error) {
	if reg == nil {
		return nil, fmt.Errorf("resolve attribute cache for schema %d: schema registry is nil: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}
	_, cache, err := reg.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve attribute cache for schema %d: %w: %w", schemaID, ErrSchemaAttrCacheUnavailable, err)
	}
	if len(cache) == 0 {
		return nil, fmt.Errorf("schema %d has an empty attribute metadata cache: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}
	return cache, nil
}

// resolveRunAttrCache is the run-start pre-flight of cdc-flush and cdc-init:
// resolveRequiredAttrCache, and the schema's active generation still the one
// the registry started on. A long-lived process (the CDC runner, a warm
// Lambda) must not start a run on a declaration apply-schema has superseded
// (#733).
func resolveRunAttrCache(ctx context.Context, reg forma.SchemaRegistry, schemaID int16) (forma.SchemaAttributeCache, error) {
	cache, err := resolveRequiredAttrCache(reg, schemaID)
	if err != nil {
		return nil, err
	}
	if err := schemaledger.RequireUnchanged(ctx, reg, schemaID); err != nil {
		return nil, fmt.Errorf("schema %d: %w", schemaID, err)
	}
	return cache, nil
}

// SkippedSchema is a schema a CDC run left unexported because the parquet
// tiers cannot hold it (#704). Its change_log rows stay unflushed, and the
// federated engine answers its reads from PostgreSQL. Reason names the
// offending attribute.
type SkippedSchema struct {
	SchemaID int16
	Reason   string
}

// FlushReport is what a flush run reports besides its error. Skipped does
// not depend on the run's logger, so a caller with a no-op logger still
// learns which schemas were not exported.
type FlushReport struct {
	Skipped []SkippedSchema
}

// resolveRunAttrCaches runs resolveRunAttrCache for every schema of a run.
// One unresolvable schema fails the whole run before any side effect (#193).
func resolveRunAttrCaches(ctx context.Context, reg forma.SchemaRegistry, schemaIDs []int64) (map[int16]forma.SchemaAttributeCache, error) {
	caches := make(map[int16]forma.SchemaAttributeCache, len(schemaIDs))
	for _, sid := range schemaIDs {
		cache, err := resolveRunAttrCache(ctx, reg, int16(sid))
		if err != nil {
			return nil, err
		}
		caches[int16(sid)] = cache
	}
	return caches, nil
}

// exportableSchemaIDs drops from schemaIDs, after the run pre-flight
// resolved caches, every schema the parquet tiers cannot hold (#704), with a
// warning naming the offending attribute, and returns the dropped schemas
// too. Such a schema's rows stay unflushed: the federated engine answers its
// reads from PostgreSQL, so exporting would only write a collapsed row and
// mark it flushed. The run's other schemas proceed, and the run does not
// fail on the skip.
func exportableSchemaIDs(logger *zap.Logger, schemaIDs []int64, caches map[int16]forma.SchemaAttributeCache) ([]int64, []SkippedSchema) {
	out := make([]int64, 0, len(schemaIDs))
	var skipped []SkippedSchema
	for _, sid := range schemaIDs {
		if err := sqlgen.ValidateParquetRepresentable(caches[int16(sid)]); err != nil {
			logger.Warn("schema not exported: its rows stay in PostgreSQL, which answers its federated reads",
				zap.Int64("schema_id", sid), zap.Error(err))
			skipped = append(skipped, SkippedSchema{SchemaID: int16(sid), Reason: err.Error()})
			continue
		}
		out = append(out, sid)
	}
	return out, skipped
}

// refuseNamedSkippedSchema fails a cdc-init run whose schema-id filter named
// a schema the run skipped (#704). The filter selects that one schema, so
// nothing was exported or marked: the caller asked for an export that cannot
// happen, and a success would read as one that did.
func refuseNamedSkippedSchema(schemaIDFilter int, skipped []SkippedSchema) error {
	if schemaIDFilter <= 0 || len(skipped) == 0 {
		return nil
	}
	return fmt.Errorf("cdc init refused for schema %d: %s; its rows stay in PostgreSQL and nothing was exported: %w",
		skipped[0].SchemaID, skipped[0].Reason, ErrSchemaNotExportable)
}
