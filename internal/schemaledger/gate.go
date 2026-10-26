package schemaledger

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
)

// ErrNotActive reports a process whose declaration the ledger does not
// hold as active: it refuses to start.
var ErrNotActive = errors.New("the declaration is not the active generation")

// Contained answers why decl cannot run against the active generation, and
// nothing when it can. Every attribute decl holds, retired ones included,
// must be in the active generation under the same id with the same name,
// valueType, items_type and binding. Lacking an attribute the generation
// holds is allowed, since that is a process older than an addition; so is a
// differing retired flag, since a removal is not a type change.
func Contained(decl Declaration, active Generation) []string {
	recorded := active.entryByID()
	var problems []string
	for _, e := range decl.Entries {
		old, ok := recorded[e.AttrID]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("attribute %s (id %d) is declared but not in active generation %d",
				e.Name, e.AttrID, active.Number))
		case old.Name != e.Name:
			problems = append(problems, fmt.Sprintf("attribute id %d is declared as %s but active generation %d records it as %s",
				e.AttrID, e.Name, active.Number, old.Name))
		case !old.sameType(e) || !old.sameBinding(e):
			problems = append(problems, fmt.Sprintf("attribute %s (id %d) is declared as %s but active generation %d records %s",
				e.Name, e.AttrID, e.describe(), active.Number, old.describe()))
		}
	}
	return problems
}

// Gate checks each declaration a data-plane process loaded against its
// schema's active generation, read for every schema in one query, and
// answers the generation each schema runs on. It refuses, naming every
// failing schema, when a schema has no baseline or Contained finds a
// problem.
func Gate(ctx context.Context, q Querier, registry string, decls []Declaration) (map[int16]int64, error) {
	active, err := readAllActive(ctx, q, registry)
	if err != nil {
		return nil, err
	}
	loaded := make(map[int16]int64, len(decls))
	var refusals []string
	for _, decl := range sortedDeclarations(decls) {
		gen, ok := active[decl.SchemaID]
		label := fmt.Sprintf("schema %s (id %d)", decl.SchemaName, decl.SchemaID)
		if !ok {
			refusals = append(refusals, label+": has no active generation (no baseline); run baseline-schema")
			continue
		}
		problems := Contained(decl, gen)
		if len(problems) > 0 {
			refusals = append(refusals, label+": "+strings.Join(problems, "; ")+
				"; deploy the declaration apply-schema recorded, or apply this one first")
			continue
		}
		loaded[decl.SchemaID] = gen.Number
	}
	if len(refusals) > 0 {
		return nil, fmt.Errorf("schema registry table %s: %w:\n  %s", registry, ErrNotActive, strings.Join(refusals, "\n  "))
	}
	return loaded, nil
}

func sortedDeclarations(decls []Declaration) []Declaration {
	out := append([]Declaration(nil), decls...)
	sort.Slice(out, func(i, j int) bool { return out[i].SchemaID < out[j].SchemaID })
	return out
}

// readAllActive reads every baselined schema's active generation. A schema
// with no baseline is absent from the answer.
func readAllActive(ctx context.Context, q Querier, registry string) (map[int16]Generation, error) {
	t := TablesFor(registry)
	query := fmt.Sprintf(`SELECT r.schema_id, r.%[1]s, a.attr_id, a.attr_name, a.value_type, a.items_type, a.column_name, a.encoding, a.retired
		FROM %[2]s r LEFT JOIN %[3]s a ON a.schema_id = r.schema_id AND a.generation = r.%[1]s
		WHERE r.%[1]s IS NOT NULL ORDER BY r.schema_id, a.attr_id`, PointerColumn, t.Registry, t.Attribute)
	rows, err := q.Query(ctx, query)
	if err != nil {
		return nil, wrapReadError(err, registry)
	}
	defer rows.Close()
	active := make(map[int16]Generation)
	for rows.Next() {
		schemaID, number, entry, err := scanActiveRow(rows)
		if err != nil {
			return nil, wrapReadError(err, registry)
		}
		gen := active[schemaID]
		gen.SchemaID, gen.Number = schemaID, number
		if entry != nil {
			gen.Entries = append(gen.Entries, *entry)
		}
		active[schemaID] = gen
	}
	if err := rows.Err(); err != nil {
		return nil, wrapReadError(err, registry)
	}
	return active, nil
}

func scanActiveRow(rows pgx.Rows) (int16, int64, *Entry, error) {
	var schemaID int16
	var number int64
	var attrID *int16
	var name, valueType, itemsType, column, encoding *string
	var retired *bool
	if err := rows.Scan(&schemaID, &number, &attrID, &name, &valueType, &itemsType, &column, &encoding, &retired); err != nil {
		return 0, 0, nil, fmt.Errorf("failed to scan active generation row: %w", err)
	}
	if attrID == nil {
		return schemaID, number, nil, nil
	}
	return schemaID, number, &Entry{
		AttrID:    *attrID,
		Name:      *name,
		ValueType: forma.ValueType(*valueType),
		ItemsType: forma.ValueType(*itemsType),
		Column:    forma.MainColumn(*column),
		Encoding:  forma.MainColumnEncoding(*encoding),
		Retired:   *retired,
	}, nil
}
