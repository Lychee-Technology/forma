# AttributeRepository 实现总结

## 概述

已成功实现 `AttributeRepository` 接口，基于 PostgreSQL 16 和高性能的 `github.com/jackc/pgx/v5` 驱动程序。该实现支持灵活的表名配置，并完全支持 `docs/query.md` 中定义的所有查询模式。

## 核心特性

### 1. 实现的方法

#### 属性操作
- **InsertAttributes**: 使用 pgx.Batch 批量插入属性，支持高效的批处理
- **UpdateAttributes**: 使用 pgx.Batch 批量更新属性值
- **BatchUpsertAttributes**: 使用 PostgreSQL 的 `INSERT ... ON CONFLICT` 实现高效的插入或更新操作
- **DeleteAttributes**: 根据 schema 名称和行 ID 列表删除属性
- **GetAttributes**: 查询特定行 ID 的所有属性（无分页）

#### 实体操作
- **ExistsEntity**: 检查实体是否存在
- **DeleteEntity**: 删除特定实体的所有属性
- **CountEntities**: 统计匹配过滤条件的实体数量

#### 高级查询
- **QueryAttributes**: 支持复杂的分页查询
  - 支持 schema 特定查询和跨 schema 查询
  - 支持多条件过滤
  - 支持多字段排序
  - 返回分页元数据（总记录数、总页数、当前页）

### 2. 查询支持

#### 过滤操作
- `FilterEquals` - 精确匹配
- `FilterNotEquals` - 不相等
- `FilterStartsWith` - 前缀匹配（不区分大小写）
- `FilterContains` - 包含匹配（不区分大小写）
- `FilterGreaterThan` - 大于
- `FilterLessThan` - 小于
- `FilterGreaterEq` - 大于等于
- `FilterLessEq` - 小于等于
- `FilterIn` - 在数组中
- `FilterNotIn` - 不在数组中

#### 排序支持
- 支持多字段排序
- 支持升序 (ASC) 和降序 (DESC)
- 默认按 row_id 升序排序

### 3. 高性能特性

- **批处理**: 使用 `pgx.Batch` 实现高效的批量操作，减少网络往返
- **连接池**: 通过 `pgxpool.Pool` 实现连接复用
- **预备语句**: 所有查询使用参数化，防止 SQL 注入
- **灵活的表名**: 支持动态配置 EAV 和 Schema Registry 表名

### 4. SQL 查询模式

#### 单行查询 (GetAttributes)
```sql
SELECT eav.schema_id, row_id, attr_name, attr_value
FROM <eav_table_name> eav INNER JOIN <schema_registry_table_name> sr
ON eav.schema_id = sr.schema_id
WHERE sr.schema_name = $1 AND eav.row_id = $2
```

#### 分页查询 (QueryAttributes - Schema 特定)
使用 CTE (Common Table Expression) 实现：
- `target_schema`: 获取目标 schema_id
- `distinct_rows`: 获取不同的行 ID（带分页）
- `total_count`: 计算总记录数
- 主查询: 返回属性值和分页元数据

#### 跨 Schema 查询 (QueryAttributes - 跨 Schema)
类似的 CTE 结构，但不限制于特定的 schema

#### 批量删除 (DeleteAttributes)
```sql
DELETE FROM <eav_table_name>
WHERE schema_id = (SELECT schema_id FROM <schema_registry_table_name> WHERE schema_name = $1)
AND row_id = ANY($2)
```

#### 批量插入或更新 (BatchUpsertAttributes)
```sql
INSERT INTO <eav_table_name> (schema_id, row_id, attr_name, attr_value)
VALUES (...)
ON CONFLICT (row_id, attr_name, schema_id)
DO UPDATE SET attr_value = EXCLUDED.attr_value
```

## 文件结构

```
internal/
├── postgres_repository.go          # 主实现（~450 行）
├── postgres_repository_test.go     # 单元和集成测试（~330 行）
└── 现有文件
    ├── interfaces.go               # AttributeRepository 接口定义
    ├── types.go                    # Attribute, AttributeQuery 等类型
    └── ...
```

## 测试覆盖

### 单元测试
- ✅ 构造函数初始化
- ✅ 过滤条件构建 (6 个测试用例)
- ✅ ORDER BY 子句构建 (4 个测试用例)
- ✅ 字段映射函数 (5 个测试用例)

### 集成测试模板
包含以下集成测试的模板（需要 PostgreSQL）：
- InsertAttributes
- GetAttributes
- QueryAttributes
- BatchUpsertAttributes
- CountEntities
- DeleteEntity
- ExistsEntity

测试使用 `testify` 框架，可以与 `testcontainers-go` 集成进行真实数据库测试。

### 测试结果
```
PASS
ok  lychee.technology/ltbase/forma/internal  0.226s

单元测试：全部通过 ✅
集成测试：已跳过（需要 PostgreSQL 环境）
编译：成功 ✅
```

## 使用示例

```go
import (
    "context"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/google/uuid"
    "lychee.technology/ltbase/forma/internal"
)

// 初始化
pool, _ := pgxpool.New(ctx, "postgres://...")
repo := internal.NewPostgresAttributeRepository(
    pool,
    "eav_abc123_proj1",
    "schema_registry_abc123_proj1",
)

// 插入属性
attributes := []internal.Attribute{
    {
        SchemaID: 100,
        RowID:    uuid.New(),
        Name:     "username",
        Value:    "john_doe",
    },
}
err := repo.InsertAttributes(ctx, attributes)

// 查询属性
attrs, err := repo.GetAttributes(ctx, "users", rowID)

// 复杂查询
query := &internal.AttributeQuery{
    SchemaName: "users",
    Filters: []forma.Filter{
        {
            Type:  forma.FilterContains,
            Field: forma.FilterFieldAttributeValue,
            Value: "example",
        },
    },
    OrderBy: []forma.OrderBy{
        {
            Field:     forma.FilterFieldAttributeName,
            SortOrder: forma.SortOrderAsc,
        },
    },
    Limit:  20,
    Offset: 0,
}
attrs, err := repo.QueryAttributes(ctx, query)

// 批量插入或更新
err := repo.BatchUpsertAttributes(ctx, attributes)

// 统计实体
count, err := repo.CountEntities(ctx, "users", filters)
```

## 配置要求

### PostgreSQL 版本
- PostgreSQL 16+ (支持 AWS DSQL)

### 表结构
需要两个表：

**Schema Registry Table**
```sql
CREATE TABLE schema_registry_<id> (
    schema_name TEXT PRIMARY KEY,
    schema_id int2
);
```

**EAV Data Table**
```sql
CREATE TABLE eav_<id> (
    schema_id int2,
    row_id UUID,
    attr_name TEXT,
    attr_value TEXT,
    PRIMARY KEY (row_id, attr_name, schema_id)
);

CREATE INDEX idx_schema_row ON eav_<id> (schema_id, row_id) 
    INCLUDE (attr_name, attr_value);
CREATE INDEX idx_attr_value ON eav_<id> (attr_value)
    INCLUDE (schema_id, row_id, attr_name);
```

## 依赖

- `github.com/jackc/pgx/v5` - PostgreSQL 驱动
- `github.com/google/uuid` - UUID 处理
- `github.com/stretchr/testify` - 测试框架

## 设计亮点

1. **参数化查询**: 所有 SQL 查询都使用参数化，防止 SQL 注入攻击
2. **错误处理**: 详细的错误消息，便于调试和日志记录
3. **批处理优化**: 使用 pgx.Batch 减少网络往返，提高吞吐量
4. **灵活性**: 支持动态表名配置，便于多租户部署
5. **CTE 优化**: 使用 Common Table Expressions 实现高效的分页和聚合
6. **类型安全**: 充分利用 Go 类型系统，避免运行时错误

## 后续扩展建议

1. **缓存层**: 添加 Redis 缓存层以加速频繁查询
2. **连接池监控**: 添加连接池健康检查和监控指标
3. **查询优化**: 基于实际使用模式添加更多索引
4. **事务支持**: 如果需要，可添加显式事务管理
5. **日志记录**: 集成结构化日志记录（如 slog）用于性能分析
