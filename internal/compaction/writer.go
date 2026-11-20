package compaction

import (
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
)

// manifestCacheEntries bounds a pass's cache of verified manifests: the
// input generation, and one head per rebase.
const manifestCacheEntries = 16

// WriterConfig is what the compactor's V2 publication writer is built from.
type WriterConfig struct {
	// DB is the PostgreSQL or Aurora DSQL pool the publication record lives
	// on, and Engine its engine.
	DB     *sql.DB
	Engine forma.DatabaseEngine
	// EntityMain names entity_main, after which the publication tables are
	// named.
	EntityMain string
	// Layout is the deployment's object layout (cdc-flush and cdc-init's
	// -s3-prefix), and Store holds its objects write-once.
	Layout manifestv2.Layout
	Store  manifestv2.Store
	// Compaction supplies the restart budget: MaxRetries bounds how often a
	// compaction that lost to a rival compaction or cdc-init reads the head
	// and merges again.
	Compaction cdc.CompactionConfig
}

// NewWriter builds the V2 writer a compactor publishes through. It marks no
// change_log entry, so it is given no change log.
func NewWriter(cfg WriterConfig) (*cdcv2.Writer, error) {
	if err := forma.ValidateParquetPrefix("data prefix", cfg.Layout.Prefix); err != nil {
		return nil, fmt.Errorf("build the compaction writer: %w", err)
	}
	cache, err := manifestv2.NewCache(cfg.Store, manifestCacheEntries)
	if err != nil {
		return nil, fmt.Errorf("build the compaction manifest cache: %w", err)
	}
	w, err := cdcv2.New(cdcv2.Config{
		DB: cfg.DB, Engine: cfg.Engine, EntityMain: cfg.EntityMain,
		Layout: cfg.Layout, Store: cfg.Store, Cache: cache,
		Restarts: cfg.Compaction.WithDefaults().MaxRetries,
	})
	if err != nil {
		return nil, fmt.Errorf("build the compaction writer: %w", err)
	}
	return w, nil
}
