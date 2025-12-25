# QueryPersistentRecords 优化总结

## 问题分析

### 原有实现的性能问题

`QueryPersistentRecords` 方法存在典型的 **N+1 查询问题**：

```go
// 步骤 1: 查询 EAV 表，获取 rowIDs 和 attributeMap (1 次查询)
rowIDs, attributeMap, totalRecords, err := r.runAdvancedAttributeQuery(...)

// 步骤 2: 对每个 rowID 循环查询 entity_main 表 (N 次查询)
records, err := r.hydrateRecords(ctx, query.Tables, query.SchemaID, rowIDs, attributeMap)
```

`hydrateRecords` 方法内部实现：
```go
for _, rowID := range rowIDs {
    record, err := r.loadMainRecord(ctx, tables.EntityMain, schemaID, rowID)
    // 每次循环执行一次 SELECT 查询！
}
```

**性能影响**：
- 如果查询返回 100 条记录，总共需要执行 **1 + 100 = 101 次数据库查询**
- 每次查询都有网络往返开销
- 在高延迟网络环境下，性能严重下降

## 优化方案

### 采用单一 SQL 查询 + JSON 聚合

**核心思想**：在数据库端完成所有数据的关联和聚合，应用层只需执行一次查询。

### 1. 新增优化的 SQL 模板

创建 `optimizedQuerySQLTemplate`，包含以下 CTEs：

```sql
WITH anchor AS (
    -- 根据条件筛选符合条件的 row_id
    SELECT DISTINCT t.row_id
    FROM eav_table t
    WHERE t.schema_id = $1 AND [conditions]
),
keys AS (
    -- 提取排序键值
    SELECT a.row_id, ..., COUNT(*) OVER() AS total
    FROM anchor a
),
ordered AS (
    -- 排序和分页
    SELECT row_id, total
    FROM keys
    ORDER BY ... LIMIT ... OFFSET ...
),
main_data AS (
    -- JOIN entity_main 表获取主表数据
    SELECT m.*, o.total
    FROM ordered o
    INNER JOIN entity_main m 
        ON m.ltbase_schema_id = $1 AND m.ltbase_row_id = o.row_id
),
eav_aggregated AS (
    -- 使用 JSON_AGG 聚合 EAV 数据
    SELECT 
        e.row_id,
        JSON_AGG(
            JSON_BUILD_OBJECT(
                'schema_id', e.schema_id,
                'row_id', e.row_id,
                'attr_id', e.attr_id,
                'array_indices', e.array_indices,
                'value_text', e.value_text,
                'value_numeric', e.value_numeric,
            ) ORDER BY e.attr_id, e.array_indices
        )::TEXT AS attributes_json
    FROM ordered o
    INNER JOIN eav_table e 
        ON e.schema_id = $1 AND e.row_id = o.row_id
    GROUP BY e.row_id
)
-- 最终关联 main_data 和 eav_aggregated
SELECT 
    m.*,
    COALESCE(e.attributes_json, '[]') AS attributes_json,
    m.total AS total_records,
    ...
FROM main_data m
LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id;
```

**关键优化点**：
- `JSON_AGG` 将多行 EAV 数据聚合为单个 JSON 数组
- `LEFT JOIN` 确保即使没有 EAV 数据也能返回主表记录
- 所有数据在一次查询中返回

### 2. 新增 `runOptimizedQuery` 方法

```go
func (r *DbPersistentRecordRepository) runOptimizedQuery(
    ctx context.Context,
    tables StorageTables,
    schemaID int16,
    clause string,
    args []any,
    limit, offset int,
    attributeOrders []AttributeOrder,
) ([]*PersistentRecord, int64, error)
```

**职责**：
- 构建并执行优化的 SQL 查询
- 直接返回完整的 `PersistentRecord` 对象列表
- 消除了 `hydrateRecords` 的需要

### 3. 新增 `scanOptimizedRow` 方法

```go
func (r *DbPersistentRecordRepository) scanOptimizedRow(rows pgx.Rows) (*PersistentRecord, int64, error)
```

**职责**：
- 扫描包含 entity_main 列 + JSON 聚合属性的查询结果
- 解析 JSON 数组为 `[]Attribute` 结构
- 构建完整的 `PersistentRecord` 对象

**JSON 解析逻辑**：
```go
// 解析 JSON 属性
if len(attrsJSON) > 0 && string(attrsJSON) != "[]" {
    var attributes []map[string]interface{}
    if err := json.Unmarshal(attrsJSON, &attributes); err != nil {
        return nil, 0, fmt.Errorf("unmarshal attributes json: %w", err)
    }
    
    // 转换 JSON 对象为 Attribute 结构
    record.OtherAttributes = make([]Attribute, 0, len(attributes))
    for _, attrObj := range attributes {
        attr := Attribute{
            SchemaID: int16(attrObj["schema_id"].(float64)),
            AttrID:   int16(attrObj["attr_id"].(float64)),
        }
        // 解析其他字段...
        record.OtherAttributes = append(record.OtherAttributes, attr)
    }
}
```

### 4. 更新 `QueryPersistentRecords` 方法

```go
func (r *DbPersistentRecordRepository) QueryPersistentRecords(...) (*PersistentRecordPage, error) {
    // ... 前置检查和条件构建 ...
    
    // 使用优化的单一查询方法
    records, totalRecords, err := r.runOptimizedQuery(
        ctx,
        query.Tables,
        query.SchemaID,
        conditions,
        args,
        limit,
        offset,
        query.AttributeOrders,
    )
    if err != nil {
        return nil, err
    }
    
    // 直接返回结果，无需 hydrateRecords
    return &PersistentRecordPage{
        Records:      records,
        TotalRecords: totalRecords,
        TotalPages:   computeTotalPages(totalRecords, limit),
        CurrentPage:  currentPage,
    }, nil
}
```

## 性能提升

### 查询次数对比

| 场景 | 原实现 | 优化后 | 提升 |
|------|--------|--------|------|
| 10 条记录 | 11 次查询 | 1 次查询 | **91% ↓** |
| 50 条记录 | 51 次查询 | 1 次查询 | **98% ↓** |
| 100 条记录 | 101 次查询 | 1 次查询 | **99% ↓** |

### 延迟影响分析

假设每次数据库查询延迟为 10ms：

| 记录数 | 原实现总延迟 | 优化后总延迟 | 节省时间 |
|--------|--------------|--------------|----------|
| 10 条 | 110ms | 10ms | **100ms** |
| 50 条 | 510ms | 10ms | **500ms** |
| 100 条 | 1010ms | 10ms | **1000ms** |

### 其他优势

1. **减少网络往返**：从 N+1 次减少到 1 次
2. **降低数据库负载**：减少查询执行次数
3. **减少数据传输**：JSON 聚合比多行传输更高效
4. **利用数据库优化器**：PostgreSQL 可以更好地优化单一复杂查询

## 实现细节

### 关键文件修改

1. **internal/advanced_query_template.go**
   - 新增 `optimizedQuerySQLTemplate`

2. **internal/postgres_persistent_repository.go**
   - 新增 `runOptimizedQuery` 方法
   - 新增 `scanOptimizedRow` 方法
   - 更新 `QueryPersistentRecords` 方法
   - 添加 `encoding/json` 导入

### 向后兼容性

- 保留原有的 `runAdvancedAttributeQuery` 和 `hydrateRecords` 方法
- 仅修改 `QueryPersistentRecords` 的内部实现
- 外部 API 接口完全不变
- 确保现有代码无需修改

### PostgreSQL 功能依赖

- `JSON_AGG`: PostgreSQL 9.3+ 支持
- `JSON_BUILD_OBJECT`: PostgreSQL 9.4+ 支持
- `LEFT JOIN` 和 CTE: 标准 SQL 功能

## 测试建议

### 功能测试

1. 验证返回数据正确性
2. 测试边界情况（空结果、单条记录、大量记录）
3. 验证排序和分页功能
4. 测试复杂查询条件

### 性能测试

```go
// 基准测试示例
func BenchmarkQueryPersistentRecords(b *testing.B) {
    // 测试不同记录数量下的性能
    for _, n := range []int{10, 50, 100, 500} {
        b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                repo.QueryPersistentRecords(ctx, query)
            }
        })
    }
}
```

### 集成测试

需要 PostgreSQL 实例运行集成测试：
```bash
# 启动 PostgreSQL
docker-compose up -d

# 运行集成测试
go test -v ./internal -run Integration
```

## 后续优化建议

1. **添加查询日志**：记录生成的 SQL 和执行时间
2. **监控指标**：跟踪查询性能和数据库负载
3. **缓存策略**：对频繁查询的结果考虑缓存
4. **索引优化**：确保 EAV 表有适当的索引（schema_id, row_id, attr_id）
5. **批量优化**：考虑为超大结果集实现流式处理

## 总结

通过采用单一 SQL 查询 + JSON 聚合的方案，成功将 `QueryPersistentRecords` 的数据库查询次数从 **N+1** 减少到 **1**，在保持代码简洁性的同时大幅提升了查询性能。这种优化对于高并发场景和大数据量查询尤其有效。
