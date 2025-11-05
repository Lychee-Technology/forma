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
var myCache = SchemaAttributeCache{
	"price":    {AttrID: 10, ValueType: "numeric"},
	"status":   {AttrID: 11, ValueType: "text"},
	"category": {AttrID: 12, ValueType: "text"},
}
var mySchemaID int16 = 1 // 假设 'products' schema

// 3. 反序列化
var rootCondition CompositeCondition
if err := json.Unmarshal([]byte(jsonFilter), &rootCondition); err != nil {
	panic(err)
}

// 4. *** 构建 SQL ***
// 关键：初始化参数索引
paramCounter := 0 
filterClause, filterArgs, err := rootCondition.ToSqlClauses(mySchemaID, myCache, &paramCounter)

if err != nil {
	panic(err)
}

// 5. 组装最终查询
// (filterClause 现在是 CTE 的基础)
finalSql := fmt.Sprintf(`
    WITH matched_entities AS (
        %s
    )
    SELECT row_id FROM matched_entities
    -- 在这里添加你的 LEFT JOINs for sorting
    LIMIT 25 OFFSET 0;
`, filterClause)

// 6. 打印结果
fmt.Println("--- Generated SQL ---")
fmt.Println(finalSql)
fmt.Println("\n--- SQL Arguments ---")
fmt.Println(filterArgs)

/*
--- 打印输出 ---

--- Generated SQL ---
WITH matched_entities AS (
    (
        (SELECT row_id FROM public.eav_data WHERE schema_id = $1 AND attr_id = $2 AND value_numeric > $3)
        INTERSECT
        (
            (SELECT row_id FROM public.eav_data WHERE schema_id = $4 AND attr_id = $5 AND value_text = $6)
            UNION
            (SELECT row_id FROM public.eav_data WHERE schema_id = $7 AND attr_id = $8 AND value_text LIKE $9)
        )
    )
)
SELECT row_id FROM matched_entities
-- 在这里添加你的 LEFT JOINs for sorting
LIMIT 25 OFFSET 0;


--- SQL Arguments ---
[1 10 10 1 11 active 1 12 A%]
*/
