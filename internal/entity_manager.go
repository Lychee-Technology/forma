package internal

import (
	"context"
	"fmt"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type entityManager struct {
	transformer PersistentRecordTransformer
	repository  PersistentRecordRepository
	registry    forma.SchemaRegistry
	config      *forma.Config
	relations   *RelationIndex
}

// NewEntityManager creates a new EntityManager instance
func NewEntityManager(
	transformer PersistentRecordTransformer,
	repository PersistentRecordRepository,
	registry forma.SchemaRegistry,
	config *forma.Config,
) forma.EntityManager {
	var relationIdx *RelationIndex
	if config != nil {
		idx, err := LoadRelationIndex(config.Entity.SchemaDirectory)
		if err != nil {
			zap.S().Warnw("failed to load schema relations", "error", err)
		} else {
			relationIdx = idx
		}
	}
	return &entityManager{
		transformer: transformer,
		repository:  repository,
		registry:    registry,
		config:      config,
		relations:   relationIdx,
	}
}

func (em *entityManager) storageTables() StorageTables {
	if em == nil || em.config == nil {
		return StorageTables{}
	}
	tables := StorageTables{}
	if em.config.Database.TableNames.EntityMain != "" {
		tables.EntityMain = em.config.Database.TableNames.EntityMain
	}
	if em.config.Database.TableNames.EAVData != "" {
		tables.EAVData = em.config.Database.TableNames.EAVData
	}
	return tables
}

func (em *entityManager) toDataRecord(ctx context.Context, schemaName string, record *PersistentRecord) (*forma.DataRecord, error) {
	if record == nil {
		return nil, fmt.Errorf("persistent record cannot be nil")
	}
	resolvedName := schemaName
	if resolvedName == "" {
		name, _, err := em.registry.GetSchemaAttributeCacheByID(record.SchemaID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve schema name for id %d: %w", record.SchemaID, err)
		}
		resolvedName = name
	}

	attributes, err := em.transformer.FromPersistentRecord(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("failed to transform persistent record to JSON: %w", err)
	}

	return &forma.DataRecord{
		SchemaName: resolvedName,
		RowID:      record.RowID,
		Attributes: attributes,
	}, nil
}
