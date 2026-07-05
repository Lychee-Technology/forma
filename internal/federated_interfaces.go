package internal

import "github.com/lychee-technology/forma/internal/model"

type DataTier = model.DataTier

const (
	DataTierHot  = model.DataTierHot
	DataTierWarm = model.DataTierWarm
	DataTierCold = model.DataTierCold
)

type FederatedAttributeQuery = model.FederatedAttributeQuery
type DuckDBRenderHints = model.DuckDBRenderHints
type ConsistencyMode = model.ConsistencyMode

const (
	ConsistencyModeStrict   = model.ConsistencyModeStrict
	ConsistencyModeEventual = model.ConsistencyModeEventual
)

type FederatedQueryOptions = model.FederatedQueryOptions
type ExecutionPlan = model.ExecutionPlan
type DataSourcePlan = model.DataSourcePlan
type MergeStrategy = model.MergeStrategy

const MergeStrategyLastWriteWins = model.MergeStrategyLastWriteWins

type MergePlan = model.MergePlan
type KeysetCursorMode = model.KeysetCursorMode

const (
	KeysetCursorModeAfter  = model.KeysetCursorModeAfter
	KeysetCursorModeBefore = model.KeysetCursorModeBefore
)

type KeysetColumn = model.KeysetColumn
type KeysetCursor = model.KeysetCursor
type RoutingDecision = model.RoutingDecision
