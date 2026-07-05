package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type entityRelationService struct {
	relations     *RelationIndex
	repository    model.PersistentRecordRepository
	registry      forma.SchemaRegistry
	transformer   model.PersistentRecordTransformer
	storageTables storageTablesResolver
}

func newEntityRelationService(em *entityManager) *entityRelationService {
	if em == nil {
		return &entityRelationService{}
	}
	return &entityRelationService{
		relations:     em.relations,
		repository:    em.repository,
		registry:      em.registry,
		transformer:   em.transformer,
		storageTables: em.storageTables,
	}
}

func (s *entityRelationService) enrichDataRecords(ctx context.Context, schemaName string, requested []string, records ...*forma.DataRecord) error {
	if s.relations == nil || len(records) == 0 {
		return nil
	}
	if s.repository == nil || s.registry == nil || s.transformer == nil {
		return fmt.Errorf("entity relation service is not initialized")
	}

	rels := s.relations.Relations(schemaName)
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

		parents, err := s.fetchParents(ctx, rel, fkBuckets)
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

func (s *entityRelationService) fetchParents(ctx context.Context, rel RelationDescriptor, fkBuckets map[string][]*forma.DataRecord) (map[string]map[string]any, error) {
	ids := make([]string, 0, len(fkBuckets))
	for id := range fkBuckets {
		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return map[string]map[string]any{}, nil
	}

	parentSchemaID, _, err := s.registry.GetSchemaByName(rel.ParentSchema)
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

	page, err := s.repository.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
		Tables:    s.resolveTables(),
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
		attrs, err := s.transformer.FromPersistentRecord(ctx, rec)
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

func (s *entityRelationService) resolveTables() model.StorageTables {
	if s.storageTables == nil {
		return model.StorageTables{}
	}
	return s.storageTables()
}
