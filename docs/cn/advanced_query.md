# JSON Query DSL

查询结构支持复杂的逻辑组合，使用精简的 JSON 格式以减小载荷大小。查询由两种类型的节点组成：**组合条件**（Composite Condition）和**键值条件**（Key-Value Condition）。

### 1. 结构定义

#### 组合条件 (Composite Condition) - 逻辑节点
用于组合多个子条件。
- `l` (logic): 逻辑操作符，可选值为 `"and"` 或 `"or"`。
- `c` (conditions): 子条件数组，包含嵌套的组合条件或键值条件。

```json
{
  "l": "and",
  "c": [ ... ]
}
```

#### 键值条件 (Key-Value Condition) - 叶子节点
用于定义具体的属性过滤规则。
- `a` (attr): 属性名称。
- `v` (value): 过滤表达式，格式为 `operator:value`。如果省略操作符，默认为 `equals`。

```json
{
  "a": "price",
  "v": "gt:100"
}
```

### 2. 支持的操作符 (Operators)

在键值条件的 `v` 字段中，支持以下操作符：

| 操作符 | 简写 | 描述 | 示例 | SQL 映射 |
| :--- | :--- | :--- | :--- | :--- |
| **Equals** | `equals` | 等于 (默认) | `"active"` 或 `"equals:active"` | `=` |
| **Not Equals** | `not_equals` | 不等于 | `"not_equals:pending"` | `!=` |
| **Greater Than** | `gt` | 大于 | `"gt:100"` | `>` |
| **Greater Than or Equal** | `gte` | 大于等于 | `"gte:18"` | `>=` |
| **Less Than** | `lt` | 小于 | `"lt:50"` | `<` |
| **Less Than or Equal** | `lte` | 小于等于 | `"lte:100"` | `<=` |
| **Starts With** | `starts_with` | 前缀匹配 | `"starts_with:prod_"` | `LIKE 'val%'` |
| **Contains** | `contains` | 包含 | `"contains:search"` | `LIKE '%val%'` |

**注意**:
- `starts_with` 和 `contains` 操作符仅适用于文本类型 (`text`) 属性。
- 布尔类型属性仅支持 `equals` 和 `not_equals`，且值为 `1` (true) 或 `0` (false)。
- 日期类型支持 ISO 8601 格式或 Unix 毫秒时间戳。

### 3. 完整示例

```go
// --- 这是一个在您的服务层中如何使用它的示例 ---

// 1. 你的 JSON 输入 (使用短键：l=logic, c=conditions, a=attr, v=value)
var jsonFilter = `
{
    "l": "and",
    "c": [
        {
            "a": "price",
            "v": "gt:10"
        },
        {
            "l": "or",
            "c": [
                {
                    "a": "status",
                    "v": "active"
                },
                {
                    "a": "category",
                    "v": "starts_with:A"
                }
            ]
        }
    ]
}
`

// 2. 你的缓存 (你需要提前构建这个)
// 注意：需导入 "github.com/lychee-technology/forma" 包
var myCache = forma.SchemaAttributeCache{
	"price":    forma.AttributeMetadata{AttributeID: 10, ValueType: forma.ValueTypeNumeric},
	"status":   forma.AttributeMetadata{AttributeID: 11, ValueType: forma.ValueTypeText},
	"category": forma.AttributeMetadata{AttributeID: 12, ValueType: forma.ValueTypeText},
}
var mySchemaID int16 = 1 // 假设 'products' schema

// 3. 反序列化
var rootCondition forma.CompositeCondition
if err := json.Unmarshal([]byte(jsonFilter), &rootCondition); err != nil {
	panic(err)
}

// 4. *** 构建 SQL ***
// 关键：初始化参数索引，保留 $1 给 schema_id
paramCounter := 1 
sqlGenerator := internal.NewSQLGenerator()
// 注意：传入 &rootCondition，因为接口实现通常是指针接收者
filterClause, filterArgs, err := sqlGenerator.ToSqlClauses(&rootCondition, "public.eav_data", mySchemaID, myCache, &paramCounter)

if err != nil {
	panic(err)
}

// 5. 组装最终查询
// (filterClause 现在是 CTE 的基础)
finalSql := fmt.Sprintf(`
    WITH matched_entities AS (
        SELECT DISTINCT e.row_id
        FROM public.eav_data e
        WHERE e.schema_id = $1
          AND (%s)
    )
    SELECT row_id FROM matched_entities
    LIMIT 25 OFFSET 0;
`, filterClause)

// 6. 打印结果
fmt.Println("--- Generated SQL ---")
fmt.Println(finalSql)
fmt.Println("\n--- SQL Arguments ---")
fmt.Println(filterArgs)
```

```sql
WITH matched_entities AS 
    (
        SELECT DISTINCT e.row_id
        FROM public.eav_data e
        WHERE e.schema_id = $1
            AND 
                (
                    (
                        (
                            EXISTS 
                            (
                                SELECT 1 FROM public.eav_data x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = $2 AND x.value_numeric > $3
                            )
                        )
                        AND 
                        (
                            (
                                (
                                    EXISTS
                                    (
                                        SELECT 1 FROM public.eav_data x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = $4 AND x.value_text = $5
                                    )
                                )
                                OR 
                                (
                                    EXISTS
                                    (
                                        SELECT 1 FROM public.eav_data x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = $6 AND x.value_text LIKE $7
                                    )
                                )
                            )
                        )
                    )
                )
    )
        
    SELECT row_id FROM matched_entities
    LIMIT 25 OFFSET 0;
```

--- SQL Arguments ---
[10 10 11 active 12 A%]

