package federated

import (
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
)

type MetadataCache = schemameta.MetadataCache

type PersistentRecord = model.PersistentRecord
type PersistentRecordQuery = model.PersistentRecordQuery
type PersistentRecordPage = model.PersistentRecordPage
type StorageTables = model.StorageTables
type AttributeOrder = model.AttributeOrder
type AttributeQuery = model.AttributeQuery
type EAVRecord = model.EAVRecord
type FederatedAttributeQuery = model.FederatedAttributeQuery
type FederatedQueryOptions = model.FederatedQueryOptions
type DataTier = model.DataTier
type RoutingDecision = model.RoutingDecision
type ExecutionPlan = model.ExecutionPlan
type DataSourcePlan = model.DataSourcePlan
type MergePlan = model.MergePlan
type MergeStrategy = model.MergeStrategy
type KeysetCursor = model.KeysetCursor
type KeysetColumn = model.KeysetColumn
type KeysetCursorMode = model.KeysetCursorMode

const (
	DataTierHot                = model.DataTierHot
	DataTierWarm               = model.DataTierWarm
	DataTierCold               = model.DataTierCold
	MergeStrategyLastWriteWins = model.MergeStrategyLastWriteWins
	KeysetCursorModeAfter      = model.KeysetCursorModeAfter
	KeysetCursorModeBefore     = model.KeysetCursorModeBefore
	ConsistencyModeStrict      = model.ConsistencyModeStrict
	ConsistencyModeEventual    = model.ConsistencyModeEventual
)
