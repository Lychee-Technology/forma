package factory

import (
	"fmt"
	"slices"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/rowguard"
)

// requireRowGuards fails closed when the Aurora DSQL engine is selected and
// the row-guard table, its per-row write lock (#633), is absent: without it
// every write would fail at its first statement instead of at startup. The
// PostgreSQL engine locks with advisory locks and never reads the table.
func requireRowGuards(cfg *forma.Config, tables []string) error {
	if cfg.Database.Engine.Resolved() != forma.EngineAuroraDSQL {
		return nil
	}
	guards := rowguard.Table(cfg.Database.TableNames.EntityMain)
	if !slices.Contains(tables, normalizeTableName(guards)) {
		return fmt.Errorf("table %s is missing, which the %s engine locks rows with; docs/aurora-dsql.md gives its DDL",
			guards, forma.EngineAuroraDSQL)
	}
	return nil
}
