package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

func (em *entityManager) enrichDataRecords(ctx context.Context, schemaName string, requested []string, records ...*forma.DataRecord) error {
	if em == nil || em.relations == nil || len(records) == 0 {
		return nil
	}

	rels := em.relations.Relations(schemaName)
	if len(rels) == 0 {
		return nil
	}

	requestedSet := make(map[string]struct{}, len(requested))
	for _, attr := range requested {
		requestedSet[attr] = struct{}{}
	}

	for _, rel := range rels {
		if len(requestedSet) > 0 {
			want := false
			for _, attr := range requested {
				if attr == rel.ChildPath || strings.HasPrefix(attr, rel.ChildPath+".") {
					want = true
					break
				}
			}
			if !want {
				continue
			}
		}

		fkBuckets := make(map[string][]*forma.DataRecord)
		for _, rec := range records {
			fkVal, ok := readStringAtPath(rec.Attributes, rel.ForeignKeyAttr)
			if !ok || fkVal == "" {
				if rel.ForeignKeyRequired {
					zap.S().Warnw("missing required parent foreign key", "schema", schemaName, "attr", rel.ForeignKeyAttr)
				}
				continue
			}
			fkBuckets[fkVal] = append(fkBuckets[fkVal], rec)
		}

		if len(fkBuckets) == 0 {
			continue
		}

		parents, err := em.fetchParents(ctx, rel, fkBuckets)
		if err != nil {
			return err
		}

		for fk, recs := range fkBuckets {
			parentAttrs, ok := parents[fk]
			if !ok {
				continue
			}

			fragment := getValueAtPath(parentAttrs, rel.ParentPath)
			if fragment == nil {
				continue
			}

			for _, rec := range recs {
				setNestedValue(rec.Attributes, rel.ChildPath, deepCopyValue(fragment))
			}
		}
	}

	return nil
}

func (em *entityManager) fetchParents(ctx context.Context, rel RelationDescriptor, fkBuckets map[string][]*forma.DataRecord) (map[string]map[string]any, error) {
	ids := make([]string, 0, len(fkBuckets))
	for id := range fkBuckets {
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return map[string]map[string]any{}, nil
	}

	parentSchemaID, _, err := em.registry.GetSchemaByName(rel.ParentSchema)
	if err != nil {
		return nil, fmt.Errorf("get parent schema %s: %w", rel.ParentSchema, err)
	}

	var cond forma.Condition
	if len(ids) == 1 {
		cond = &forma.KvCondition{Attr: rel.ParentIDAttr, Value: ids[0]}
	} else {
		conditions := make([]forma.Condition, 0, len(ids))
		for _, id := range ids {
			conditions = append(conditions, &forma.KvCondition{Attr: rel.ParentIDAttr, Value: id})
		}
		cond = &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: conditions}
	}

	page, err := em.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
		Tables:    em.storageTables(),
		SchemaID:  parentSchemaID,
		Condition: cond,
		Limit:     len(ids),
		Offset:    0,
	})
	if err != nil {
		return nil, fmt.Errorf("query parent records for schema %s: %w", rel.ParentSchema, err)
	}

	parents := make(map[string]map[string]any, len(page.Records))
	for _, rec := range page.Records {
		attrs, err := em.transformer.FromPersistentRecord(ctx, rec)
		if err != nil {
			return nil, fmt.Errorf("transform parent record for schema %s: %w", rel.ParentSchema, err)
		}
		parentID, _ := readStringAtPath(attrs, rel.ParentIDAttr)
		if parentID == "" {
			continue
		}
		parents[parentID] = attrs
	}

	return parents, nil
}
