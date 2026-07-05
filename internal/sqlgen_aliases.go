package internal

import (
	"text/template"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

type SQLGenerator = sqlgen.SQLGenerator
type DualClauses = sqlgen.DualClauses
type ListOperatorMapping = sqlgen.ListOperatorMapping

var ErrListInOrderBy = sqlgen.ErrListInOrderBy
var AdvancedQueryTemplateDuckDB = sqlgen.AdvancedQueryTemplateDuckDB

func NewSQLGenerator() *SQLGenerator { return sqlgen.NewSQLGenerator() }

func ToDualClauses(condition forma.Condition, eavTable string, schemaID int16, cache forma.SchemaAttributeCache, paramIndex *int) (DualClauses, error) {
	return sqlgen.ToDualClauses(condition, eavTable, schemaID, cache, paramIndex)
}

func BuildDuckDBQuery(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {
	return sqlgen.BuildDuckDBQuery(tpl, params, q, dirtyIDs, dual)
}

func RenderDuckDBQuery(tpl *template.Template, params any, whereArgs []any) (string, []any, error) {
	return sqlgen.RenderDuckDBQuery(tpl, params, whereArgs)
}

func AppendDirtyExclusion(baseClause string, dirtyIDs []uuid.UUID) (string, []any) {
	return sqlgen.AppendDirtyExclusion(baseClause, dirtyIDs)
}

func RenderS3ParquetPath(tmpl string, schemaID int16) (string, error) {
	return sqlgen.RenderS3ParquetPath(tmpl, schemaID)
}

func BuildListPredicate(column, operator, value string, elementType forma.ValueType) (string, any, error) {
	return sqlgen.BuildListPredicate(column, operator, value, elementType)
}

func ValidateOrderByForListTypes(orderBy []forma.OrderBy, getValueType func(attrName string) (forma.ValueType, bool)) error {
	return sqlgen.ValidateOrderByForListTypes(orderBy, getValueType)
}

func ValidateOrderByAttributesForListTypes(orderBy []AttributeOrder) error {
	return sqlgen.ValidateOrderByAttributesForListTypes(orderBy)
}

func MapValueTypeToDuckDBType(v forma.ValueType) string { return sqlgen.MapValueTypeToDuckDBType(v) }
func MapValueTypeToListDuckDBType(v forma.ValueType) string {
	return sqlgen.MapValueTypeToListDuckDBType(v)
}
func IsListType(v forma.ValueType) bool { return sqlgen.IsListType(v) }
func CastExpression(columnOrExpr string, v forma.ValueType) string {
	return sqlgen.CastExpression(columnOrExpr, v)
}
func ToDuckDBParam(value any, v forma.ValueType) (any, error) { return sqlgen.ToDuckDBParam(value, v) }
func RenderSQLTemplate(tpl *template.Template, data any) (string, []any, error) {
	return sqlgen.RenderSQLTemplate(tpl, data)
}

type SchemaProjection = sqlgen.SchemaProjection

func BuildSchemaProjection(schemaID int16, cache forma.SchemaAttributeCache) (*SchemaProjection, error) {
	return sqlgen.BuildSchemaProjection(schemaID, cache)
}

func isBenchmarkSchemaID(schemaID int16) bool { return sqlgen.IsBenchmarkSchemaID(schemaID) }
func BuildBenchmarkProjections(schemaID int16) *SchemaProjection {
	return sqlgen.BuildBenchmarkProjections(schemaID)
}
func BuildBenchmarkOuterSelect(schemaID int16) string {
	return sqlgen.BuildBenchmarkOuterSelect(schemaID)
}
func BuildBenchmarkS3Projection(schemaID int16) string {
	return sqlgen.BuildBenchmarkS3Projection(schemaID)
}

func buildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
	return sqlgen.BuildDuckClause(cond, cache)
}

func buildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
	return sqlgen.BuildPgMainClause(cond, cache, paramIndex)
}
