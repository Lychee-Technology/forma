package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// Schemas enumerates the schemas a run visits: every registered schema and
// every schema with a publication record. A registered schema without a
// record is reported (NoRecord); a dropped schema keeps its record and its
// generations, which expire and are collected like any other's.
type Schemas struct {
	DB       *sql.DB
	Registry string
	Tables   publication.Tables
	// Only, when positive, restricts the run to that schema, which must be
	// registered or have a record.
	Only int16
}

// SchemaIDs answers the ids in order.
func (s *Schemas) SchemaIDs(ctx context.Context) ([]int16, error) {
	registered, err := s.registered(ctx)
	if err != nil {
		return nil, err
	}
	recorded, err := publication.SchemaIDs(ctx, publication.DB(s.DB), s.Tables)
	if err != nil {
		return nil, fmt.Errorf("enumerate the schemas with a publication record: %w", err)
	}
	ids := slices.Compact(slices.Sorted(slices.Values(append(registered, recorded...))))
	if s.Only <= 0 {
		return ids, nil
	}
	if !slices.Contains(ids, s.Only) {
		// A silent empty run would report a schema it never inspected as
		// clean (a mistyped --schema-id).
		return nil, fmt.Errorf("schema %d is neither registered in %s nor has a record in %s; nothing was inspected", s.Only, s.Registry, s.Tables.Record)
	}
	return []int16{s.Only}, nil
}

func (s *Schemas) registered(ctx context.Context) ([]int16, error) {
	table := sqlutil.SanitizeIdentifier(s.Registry)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf("SELECT schema_id FROM %s ORDER BY schema_id", table))
	if err != nil {
		return nil, fmt.Errorf("query schema ids from %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int16
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan a schema id from %s: %w", table, err)
		}
		if id < -32768 || id > 32767 {
			return nil, fmt.Errorf("schema id %d from %s overflows int16", id, table)
		}
		ids = append(ids, int16(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema ids from %s: %w", table, err)
	}
	return ids, nil
}

var _ SchemaEnumerator = (*Schemas)(nil)
