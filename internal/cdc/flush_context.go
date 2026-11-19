package cdc

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"go.uber.org/zap"
)

// flushContextParams carries the per-run values both RunOnce frames resolve
// before a flush context can be built.
type flushContextParams struct {
	cfg            CDCConfig
	db             *sql.DB
	duck           *DuckExporter
	store          manifestv2.Store
	dryRun         bool
	logger         *zap.Logger
	schemaRegistry forma.SchemaRegistry
}

// newSchemaFlushContext assembles the flush context both entrypoints run:
// package-level RunOnce (cmd/tools cdc-flush) and Runner.RunOnce. It is the
// one place the run's V2 writer, object store and schema lock are wired, so
// neither frame can carry a wiring of its own.
func newSchemaFlushContext(p flushContextParams) (*schemaFlushContext, error) {
	cfg := p.cfg.WithDefaults()
	if err := cfg.ValidateFlush(); err != nil {
		return nil, fmt.Errorf("cdc flush configuration: %w", err)
	}
	if p.store == nil {
		return nil, errors.New("cdc flush needs an object store to stage its generations in")
	}
	logger := p.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	// The schema lock follows the configured engine (#633); an unknown one
	// fails the run here, before any schema is touched.
	locker, err := NewSchemaLocker(p.db, cfg.Engine, cfg.EntityMainTable)
	if err != nil {
		return nil, fmt.Errorf("cdc flush schema lock: %w", err)
	}
	writer, err := newPublicationWriter(cfg, p.db, p.store, cfg.BatchSize, 0)
	if err != nil {
		return nil, fmt.Errorf("cdc flush: %w", err)
	}
	return &schemaFlushContext{
		db:             p.db,
		duck:           p.duck,
		store:          p.store,
		writer:         writer,
		locker:         locker,
		cfg:            cfg,
		tableName:      cfg.ChangeLogTable,
		dryRun:         p.dryRun,
		logger:         logger,
		schemaRegistry: p.schemaRegistry,
	}, nil
}
