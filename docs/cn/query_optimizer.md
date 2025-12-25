# 查询优化器

本文设计一个基于 JSON Schema 与 Metadata 的查询优化器，生成可直接在 PostgreSQL/AWS DSQL 上执行的高性能 SQL。优化器的输出是「SQL + 参数 + 所需元数据」，可供 `DbPersistentRecordRepository` 或未来的查询服务直接使用。

## 目标与输入输出
- **输入**：用户提交的查询（JSON 形式的条件/排序/分页）、Schema Metadata（属性到存储位置/列的映射、attr_id、值类型及 fallback 编码）、运行时统计信息（可选）。
- **输出**：最优或近似最优的 SQL 语句及参数；同时返回计划说明，便于调试（如：驱动表、使用了哪些索引、哪些谓词被重写）。
- **约束**：多租户、表按项目拆分（`entity_main_<client>_<project>` / `eav_<client>_<project>`），字段数量有限且存在类型 fallback；查询多为「按属性过滤 + 排序 + 分页」。

## 现有表与性能特征
- **entity main（热属性表）**：包含 10 个 `text`、3 个 `smallint`、3 个 `integer`、3 个 `bigint`、5 个 `double precision`、2 个 `uuid` 以及 `ltbase_*` 元数据列。仅部分列有索引（text_01~03、smallint_01、integer_01、bigint_01、double_01~02、uuid_01），其他列只能顺序扫描或依赖位图索引合并。
- **EAV 表**：`schema_id, row_id, attr_id, array_indices, value_text, value_numeric`，主键为 `(schema_id, row_id, attr_id, array_indices)`。当前模型文档只定义了 `value_text/value_numeric` 的部分索引。
- **类型 fallback**：数字/日期/uuid/bool 在热表有 fallback 规则（如 int 存 double，uuid/bool 存 text），查询需做等值/范围的偏移重写；EAV 为强类型列，但依赖 attr_id 精确过滤。

## 核心思路
将查询优化拆成「计划生成」与「SQL 渲染」两阶段，采用规则驱动并辅以轻量成本估计（基于统计与启发式）。

### 1) 归一化与解析
1. 解析 JSON 查询为内部 IR：`Predicate{attr, op, value, logic}`, `SortKey{attr, order}`, `Pagination{limit, offset}`。
2. 基于 Schema Metadata 解析属性：确定 `存储位置(main/eav)`、`列名/attr_id`、`值类型`、`fallback 编码规则`。

### 2) 谓词分类与重写
- **main 可落地谓词**：属性存于热表，且列已知；可直接生成 `table.column op $n`。若列是 fallback（如 int 落 double），对等值/范围进行 ±0.1 的区间重写；bool/text fallback 需做字符串比较并限制 operator。
- **eav 谓词**：属性仅在 EAV；生成 `EXISTS/SEMI JOIN` 子句，形如 `EXISTS (SELECT 1 FROM eav t WHERE t.schema_id=$sid AND t.row_id=base.row_id AND t.attr_id=$aid AND t.value_xxx op $val)`。日期解析为 `timestamptz`，bool 仅支持 `=`/`!=`。
- **组合条件**：在 IR 层处理 AND/OR，分别对 main/eav 子句布尔组合；必要时拆分为「main 过滤 + eav 过滤」后求交集。

### 3) 驱动表选择与计划形态
按选择度和可用索引选择驱动：
1. **main-driven**：存在高选择度、已索引的热表谓词或排序列；先从 main 产出 row_id，再按需半连接 EAV（用于补充非热属性或排序）。
2. **eav-driven**：仅有 EAV 谓词或 main 列无索引；使用 EAV CTE 产出 row_id（按 attr_id + value 走索引），再回表到 main。
3. **hybrid-intersect**：main 与 EAV 各有高选择度谓词时，分别生成 row_id 列表后求交集（`INTERSECT` 或 hash join），减少回表成本。

### 4) SQL 结构（建议模板）
```sql
WITH anchor AS (
    -- 依据驱动表选择 main/eav/hybrid
    SELECT /* DISTINCT */ m.ltbase_row_id AS row_id
    FROM hot_attributes_<...> m
    WHERE m.ltbase_schema_id = $1
      AND <main_predicates>          -- 可为空
      AND EXISTS ( ... eav filter )  -- 可为空
),
sorted AS (
    SELECT a.row_id
         , s1.sort_key AS k1 ...      -- LATERAL 提取排序值（优先热表，无则 EAV 子查询）
         , COUNT(*) OVER() AS total
    FROM anchor a
    LEFT JOIN LATERAL (...) s1 ON TRUE
    ORDER BY k1 ASC NULLS LAST, a.row_id
    LIMIT $limit OFFSET $offset
)
SELECT m.*, e.*
FROM sorted s
JOIN hot_attributes_<...> m ON m.ltbase_row_id = s.row_id AND m.ltbase_schema_id = $1
LEFT JOIN LATERAL (
    SELECT attr_id, array_indices, value_text, value_numeric
    FROM eav_<...> e
    WHERE e.schema_id = $1 AND e.row_id = s.row_id
) e ON TRUE
ORDER BY <same as sorted>;
```
要点：
- 过滤、排序尽量在 `anchor/sorted` 完成，`SELECT` 阶段只做投影。
- `ORDER BY` 复用 `sorted` 的排序键，避免重复排序。
- 数值 fallback 采用区间谓词；日期存 double/text 时同样走区间或 `::timestamptz` 转换。

### 5) 排序策略
- 首选热表中已索引列；缺少索引但在热表的列可接受 Seq Scan + Sort。
- 对 EAV 排序使用 `LEFT JOIN LATERAL (SELECT value_xxx FROM eav ... ORDER BY array_indices LIMIT 1)`，并在 anchor 中仅计算一次。
- 多列排序按稳定顺序拼接，最后追加 `row_id` 保证确定性。

### 6) 投影与回补
- 热表列直接投影；EAV 通过 LATERAL 一次性取全，减少二次查询。
- 若调用方只需要部分属性，可在 Metadata 中标记「所需属性集合」，对 EAV 做投影裁剪。

### 7) 计划缓存与失效
- 按 `(schema_id, query_shape_hash)` 缓存模板化 SQL，参数化值不参与 hash。
- 当 Schema Metadata 版本或列绑定变更时，失效缓存；可使用 `schema_version` 作为缓存键的一部分。

## 需要的数据库基础设施/统计建议
- **索引补全**
  - 热表：若排序/过滤常落在 `text_04~10`、`double_03~05` 等未建索引列，为热点项目按需追加异步索引；时间范围查询可为 `ltbase_created_at` 建 BRIN（低维护）。
- **统计信息**
  - 定期 `ANALYZE`；为 `value_numeric` 设置更高统计目标（`ALTER TABLE ... ALTER COLUMN ... SET STATISTICS 500;`）。
  - 为 `(schema_id, attr_id, value_*)` 建 `CREATE STATISTICS`（ndistinct）以改善选择度估计，尤其多 attr 组合查询时。
- **存储/维护**
  - 主库：保持主键 `(schema_id, row_id, attr_id, array_indices)`，必要时为大表开启 `autovacuum_vacuum_scale_factor` 调优，避免膨胀影响顺序/索引扫描。
  - 分片/分区策略（冷热分层友好）：优先按「项目/Schema」横向拆表（现有命名已如此），在单表内按时间（`ltbase_created_at` 或业务时间字段）做月/周级分区或基于分桶的后缀表，方便批量下沉。
  - 冷数据增量合并（每 15 分钟批次）：
    1) 读取主库中「最近 15 分钟变更」(insert/update/delete) 的行，最好有 `ltbase_updated_at` 过滤 + 变更日志表/CDC 以降低扫描。
    2) 将增量与既有冷数据（上一版 Parquet）在 DuckDB 中 merge，生成新的 Parquet。Delete 用 tombstone 或直接在 DuckDB merge 过滤。
    3) Parquet 物理布局：按分片键（项目/Schema）+ 时间分桶写目录；文件内按 `schema_id, ltbase_created_at, ltbase_row_id` 排序并用 ZSTD；可把热表 + EAV 展平成超宽表，数组属性保留为 list<struct> 列以减少 join。
    4) 控制文件大小 256–512MB，避免过碎；滚动合并时可采用「新文件 + 旧文件延迟回收」以保证查询可见性。
  - 查询加速（Parquet + Delta 查询）：
    - 先查 Parquet（DuckDB/Trino/Arrow DataFusion）获取 row_id 集合或聚合结果，再对 Postgres 仅查询最近 N 分钟/小时的增量表，最后 union/intersect。适合「谓词全部在 EAV」且选择度低的场景。
    - Parquet 侧 schema：将常用 EAV attr 展平成列（宽表），长尾 attr 以 map<string, variant> 或重复 (attr_id, value*) 的 list 存储，结合 bitmap/zone map 跳过无关数据。
  - 冷热边界：近期窗口（如 24–72 小时）保留在 Postgres 以支持写与低延迟读；更老数据转 Parquet。为避免查询穿越冷热边界时慢，可在查询层自动生成「Parquet 查询 + PG 增量查询」的计划。
- **运行时度量**
  - 采集查询命中率、索引使用率与实际行数，记录到 Metadata（如 per-attr 基数、常用算子），可在驱动选择时提供启发式权重。

## 交付与集成
- 在 `internal/sql_generator` 基础上扩展：支持 main/eav 双通路、fallback 重写、排序 LATERAL，并输出 `PlanExplain`。
- 在 `metadataCache` 中补充列绑定与存储位置，供优化器判定。
- 与 `EntityManager`/`PersistentRecordRepository` 集成：优先复用 anchor/sorted CTE 模式，保持向后兼容的参数接口。

该设计在不引入复杂成本模型的前提下，利用 Schema Metadata 做静态决策，结合必要的索引与统计信息即可显著提升常见过滤 + 排序场景的性能。***
