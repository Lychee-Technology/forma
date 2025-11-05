package forma

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// *** CompositeCondition 的 ToSqlClauses (重构版) ***
func (c *CompositeCondition) ToSqlClauses(
	schemaID int16,
	cache SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {

	if len(c.Conditions) == 0 {
		// 返回 "中性" 结果。
		// 对于 'AND'，空集合是 "true" (返回所有)
		// 对于 'OR'，空集合是 "false" (返回没有)
		// 为简单起见，我们返回一个 "无操作" 的空字符串，
		// 调用者（或父 CompositeCondition）应该忽略它。
		return "", nil, nil
	}

	var sqlJoiner string
	switch c.Logic {
	case LogicAnd:
		sqlJoiner = " INTERSECT "
	case LogicOr:
		sqlJoiner = " UNION " // 注意：UNION 会去重，UNION ALL 不会。UNION 通常是 EAV 所需的。
	default:
		return "", nil, fmt.Errorf("unknown logic: %s", c.Logic)
	}

	var childClauses []string
	var allArgs []any

	for _, cond := range c.Conditions {
		// 递归调用
		sql, args, err := cond.ToSqlClauses(schemaID, cache, paramIndex)
		if err != nil {
			return "", nil, err // 传播错误
		}

		if sql == "" {
			// 忽略空的子条件（例如一个空的 CompositeCondition）
			continue
		}

		childClauses = append(childClauses, sql)
		allArgs = append(allArgs, args...)
	}

	if len(childClauses) == 0 {
		// 所有子条件都是空的
		return "", nil, nil
	}

	// 用括号将子句组合起来，以确保正确的 SQL 执行顺序
	finalSql := "(" + strings.Join(childClauses, sqlJoiner) + ")"

	return finalSql, allArgs, nil
}

// (tryParseNumber 和 parseValueAndOp 辅助函数保持不变)
func tryParseNumber(s string) any {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func (kv *KvCondition) parseValueAndOp() (sqlOp string, value any, err error) {
	parts := strings.SplitN(kv.Value, ":", 2)
	var opStr, valStr string

	if len(parts) == 1 {
		opStr, valStr = "equals", kv.Value
	} else {
		opStr, valStr = parts[0], parts[1]
		if opStr == "" || valStr == "" {
			return "", nil, fmt.Errorf("invalid KvCondition value format: %s", kv.Value)
		}
	}

	value = valStr

	switch opStr {
	case "equals":
		sqlOp = "="
		value = tryParseNumber(valStr)
	case "gt":
		sqlOp = ">"
		value = tryParseNumber(valStr)
	case "gte":
		sqlOp = ">="
		value = tryParseNumber(valStr)
	case "lt":
		sqlOp = "<"
		value = tryParseNumber(valStr)
	case "lte":
		sqlOp = "<="
		value = tryParseNumber(valStr)
	case "starts_with":
		sqlOp = "LIKE"
		value = valStr + "%"
	case "contains":
		sqlOp = "LIKE"
		value = "%" + valStr + "%"
	case "not_equals":
		sqlOp = "!="
		value = tryParseNumber(valStr)
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", opStr)
	}

	return sqlOp, value, nil
}

// ToSqlClauses 为叶子节点实现 SQL 转换
// 这是最关键的部分。它：
// 1. 从缓存中查找 attr_id 和 value_type。
// 2. 根据 value_type 解析 kv.Value 字符串。
// 3. 构建一个 (SELECT row_id FROM eav_data WHERE ...) 子句。
func (kv *KvCondition) ToSqlClauses(
	schemaID int16,
	cache SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {

	// 1. 从缓存获取元数据
	meta, ok := cache[kv.Attr]
	if !ok {
		return "", nil, fmt.Errorf("attribute not found in cache: %s", kv.Attr)
	}

	// 2. 解析操作符和 *原始字符串值*
	parts := strings.SplitN(kv.Value, ":", 2)
	var opStr, valStr string
	if len(parts) == 1 {
		opStr, valStr = "equals", kv.Value
	} else {
		opStr, valStr = parts[0], parts[1]
		if opStr == "" || valStr == "" {
			return "", nil, fmt.Errorf("invalid KvCondition value format: %s", kv.Value)
		}
	}

	// 3. 根据元数据的 meta.ValueType 确定 value 列和解析值
	var valueColumn string
	var parsedValue any
	var err error

	switch meta.ValueType {
	case "text":
		valueColumn = "value_text"
		parsedValue = valStr // 保持为 string
	case "numeric":
		valueColumn = "value_numeric"
		parsedValue = tryParseNumber(valStr) // 使用辅助函数
		if _, ok := parsedValue.(string); ok {
			// tryParseNumber 失败, 返回了原始字符串
			return "", nil, fmt.Errorf("invalid numeric value for '%s': %s", kv.Attr, valStr)
		}
	case "date":
		valueColumn = "value_date"
		// 假设日期为 RFC3339 格式 (e.g., "2025-11-03T10:00:00Z")
		parsedValue, err = time.Parse(time.RFC3339, valStr)
		if err != nil {
			return "", nil, fmt.Errorf("invalid date value for '%s' (expected RFC3339): %s", kv.Attr, valStr)
		}
	case "bool":
		valueColumn = "value_bool"
		parsedValue, err = strconv.ParseBool(valStr)
		if err != nil {
			return "", nil, fmt.Errorf("invalid boolean value for '%s': %s", kv.Attr, valStr)
		}
	default:
		return "", nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, kv.Attr)
	}

	// 4. 确定 SQL 操作符并最终确定值 (例如为 LIKE)
	var sqlOp string
	switch opStr {
	case "equals":
		sqlOp = "="
	case "gt":
		sqlOp = ">"
	case "gte":
		sqlOp = ">="
	case "lt":
		sqlOp = "<"
	case "lte":
		sqlOp = "<="
	case "not_equals":
		sqlOp = "!="
	case "starts_with":
		sqlOp = "LIKE"
		// 确保值是字符串，并添加 '%'
		parsedValue = valStr + "%"
	case "contains":
		sqlOp = "LIKE"
		// 确保值是字符串，并添加 '%'
		parsedValue = "%" + valStr + "%"
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", opStr)
	}

	// 5. 验证操作符和类型的兼容性
	if meta.ValueType != "text" && (sqlOp == "LIKE") {
		return "", nil, fmt.Errorf("operator '%s' only supported for text attributes, not '%s'", opStr, meta.ValueType)
	}
	if meta.ValueType == "bool" && sqlOp != "=" && sqlOp != "!=" {
		return "", nil, fmt.Errorf("operator '%s' not supported for boolean attributes", opStr)
	}

	// 6. 构建 SQL 子句和参数
	var args []any

	// Param 1: schema_id
	*paramIndex++
	schemaIdPlaceholder := fmt.Sprintf("$%d", *paramIndex)
	args = append(args, schemaID)

	// Param 2: attr_id
	*paramIndex++
	attrIdPlaceholder := fmt.Sprintf("$%d", *paramIndex)
	args = append(args, meta.AttributeID)

	// Param 3: the value
	*paramIndex++
	valuePlaceholder := fmt.Sprintf("$%d", *paramIndex)
	args = append(args, parsedValue)

	// 子句被包裹在括号中，以便它可以安全地与 INTERSECT/UNION 组合
	sql := fmt.Sprintf(
		"(SELECT row_id FROM public.eav_data WHERE schema_id = %s AND attr_id = %s AND %s %s %s)",
		schemaIdPlaceholder, // e.g., $1
		attrIdPlaceholder,   // e.g., $2
		valueColumn,         // e.g., value_numeric
		sqlOp,               // e.g., >
		valuePlaceholder,    // e.g., $3
	)

	return sql, args, nil
}
