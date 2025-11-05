# 查询模式

## 查询指定RowID的所有属性

这个查询不需要分页，因为最终只会返回最多一个`DataRecord`。
```sql
SELECT eav.schema_id, row_id, attr_name, attr_value
		FROM <eav_table_name> eav inner join <schema_registry_table_name> sr
		ON eav.schema_id = sr.schema_id
		WHERE schema_name = $1 AND row_id = $2
```

## 删除指定RowID的所有属性

```sql
DELETE FROM <eav_table_name>
WHERE schema_id = (SELECT schema_id FROM <schema_registry_table_name> WHERE schema_name = $1)
AND row_id = $2
```

## 查询指定Schema的记录 （分页）


```sql

-- 查询指定schema的所有记录（分页）
WITH target_schema AS (
    SELECT schema_id
    FROM <schema_registry_table_name>
    WHERE schema_name = $1
),
distinct_rows AS (
    SELECT DISTINCT 
        t.schema_id,
        t.row_id
    FROM <eav_table_name> t
    INNER JOIN target_schema s ON t.schema_id = s.schema_id
    where <conditions>
    LIMIT $2 OFFSET $3
),
total_count AS (
    SELECT COUNT(DISTINCT t.row_id) as total
    FROM <eav_table_name> t
    INNER JOIN target_schema s ON t.schema_id = s.schema_id
    where <conditions>
)
SELECT 
    t.schema_id,
    t.row_id,
    t.attr_name,
    t.attr_value,
    tc.total as total_records,
    CEIL(tc.total::numeric / $2) as total_pages,
    ($3 / $2) + 1 as current_page
FROM <eav_table_name> t
INNER JOIN distinct_rows dr 
    ON t.schema_id = dr.schema_id 
    AND t.row_id = dr.row_id
CROSS JOIN total_count tc
ORDER BY <ordering_columns>;

-- 参数说明:
-- $1: schema_name (例如: 'invoices')
-- $2: items_per_page (例如: 20)
-- $3: offset (计算方式: (page - 1) * items_per_page)

```


## 跨Schema查询属性值 (分页)

```sql
WITH distinct_rows AS (
    SELECT DISTINCT 
        t.schema_id,
        t.row_id
    FROM <eav_table_name> t
    where <conditions>
    LIMIT $2 OFFSET $3
),
total_count AS (
    SELECT COUNT(DISTINCT t.row_id) as total
    FROM <eav_table_name> t
    where <conditions>
)
SELECT 

    t.schema_id,
    t.row_id,
    t.attr_name,
    t.attr_value,
    tc.total as total_records,
    CEIL(tc.total::numeric / $2) as total_pages,
    ($3 / $2) + 1 as current_page
FROM <eav_table_name> t
INNER JOIN distinct_rows dr 
    ON t.schema_id = dr.schema_id 
    AND t.row_id = dr.row_id
CROSS JOIN total_count tc
ORDER BY <ordering_columns>;
```

## 更新指定RowID的属性值

通过使用`INSERT ... ON CONFLICT`语句，可以高效地插入或更新EAV数据表中的属性值。例如，以下代码示例展示了如何为多个属性插入或更新值：

```sql
INSERT INTO eav (schema_id, row_id, attr_name, attr_value)
VALUES 
    (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'username', 'john_doe'),
    (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'email', 'john@example.com'),
    (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'age', '30'),
    (2, 'b1ffcd88-8d1c-5fg9-cc7e-7cc0ce491b22', 'product_name', 'Widget'),
    (2, 'b1ffcd88-8d1c-5fg9-cc7e-7cc0ce491b22', 'price', '19.99')
ON CONFLICT (row_id, attr_name, schema_id)
DO UPDATE SET
    attr_value = EXCLUDED.attr_value;
```