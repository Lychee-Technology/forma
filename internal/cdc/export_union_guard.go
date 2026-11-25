package cdc

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// refuseUnionArrayForm answers agg, the export's aggregate of attr over one
// row's eav_data records, made to fail the export when the row holds attr
// in the array form of an object-or-array union (#696 review R4-F1).
//
// Such a schema is exportable: a row writing the union in its object form
// holds one value of attr, which agg exports as it did before #696 derived
// the union's array form. A row writing the array form holds one value per
// element, which agg would reduce to one and mark flushed. So the export
// raises a DuckDB error naming the row and the attribute instead: nothing
// is staged (the COPY writes a local scratch file first), nothing is marked
// flushed, and the schema's rows stay in PostgreSQL, which the federated
// engine's hot leg reads in full. The run reports the schema's failure, and
// the export resumes once no row of the batch holds the array form. An
// attribute without a union ancestor keeps agg as it is.
func refuseUnionArrayForm(attr string, meta forma.AttributeMetadata, agg string) string {
	cond := sqlgen.UnionArrayFormCondition(meta)
	if cond == "" {
		return agg
	}
	message := fmt.Sprintf("cdc export refused: a row holds attribute %q (attr_id %d) inside the array form of the object-or-array union %q, "+
		"which the parquet tiers hold one value of per row, not one per element (#704); the schema's rows stay unflushed in PostgreSQL. row_id=",
		attr, meta.AttributeID, meta.UnionArrayAncestors[0])
	return fmt.Sprintf("CASE WHEN count(*) FILTER (WHERE %s) > 0 THEN error('%s' || CAST(row_id AS VARCHAR)) ELSE %s END",
		cond, sqlutil.EscapeLiteral(message), agg)
}
