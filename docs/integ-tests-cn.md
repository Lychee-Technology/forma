# Forma 集成测试用例文档

更新时间：2026-02-16  
适用仓库：`forma`

## 1. 目的与范围

本文档按当前代码实现，整理 Forma 的集成测试用例，覆盖：

- API 层：CRUD、分页/排序、高级查询、跨 Schema 搜索
- 存储层：PostgreSQL（`entity_main`/`eav_data`/`change_log`）联动
- 联邦查询层：Postgres Hot + S3 Delta/Base + DuckDB Merge-on-Read
- 数据管道：`cdc-init`、`cdc-flush`、`compactor`
- 部署形态：本地 server 与 AWS Lambda 入口
- 性能与故障场景

不包含纯单元测试（unit test）细粒度断言。

## 2. 测试环境基线

### 2.1 最低依赖

- Go 1.26+
- Docker / Docker Compose
- PostgreSQL（本地默认：`localhost:5432/forma`）
- S3 兼容存储（本地常用 RustFS）
- DuckDB（由服务或工具链内嵌使用）
- Bun（用于 `tests/e2e`）
- k6（用于压测场景）

### 2.2 通用通过标准（Gate）

- 所有 P0 自动化用例通过
- 手工 P0 用例全部通过
- 数据一致性：无记录丢失、无重复、软删除行为正确
- 性能门槛满足（至少 smoke/full 场景达到阈值）
- 故障恢复后可继续读写，且不会产生重复或脏读

## 3. 自动化集成测试用例

## 3.1 Go 集成测试（内部层）

执行入口：

- `go test ./internal -run TestIntegration_`
- `go test ./internal -run 'TestInsertPersistentRecordIntegration|TestChangeLogWritesOnUpdateAndDeleteIntegration|TestRunOptimizedQueryIntegration'`
- `go test ./internal/federated -run 'TestEvaluateRoutingPolicy_VariousStrategies|TestNewDuckDBClient_HealthCheck|TestAppendDirtyExclusion|TestFinalizeDuckDBExecutionPlan_CaptureDisabled|TestBuildDuckDBQuery_AdvancedTemplate|TestExecuteDuckDBFederatedQuery_NilQuery'`
- `go test ./internal/sqlgen -run 'TestBuildDuckDBQuery_PropagatesRenderError|TestBuildDuckDBQuery_KeysetArgsBindLast'`

用例清单：

- `A-GI-001` `TestIntegration_EntityLifecycle`  
  验收条件：Create/Get/Update/Query/Delete 全链路成功；删除后按 row_id 读取失败。
- `A-GI-002` `TestIntegration_AdvancedQuery`  
  验收条件：高级查询条件生效，返回记录和总数正确。
- `A-GI-003` `TestIntegration_AdvancedQuery_MixedSorting`  
  验收条件：主表字段 + EAV 字段混合排序顺序正确。
- `A-GI-004` `TestInsertPersistentRecordIntegration`  
  验收条件：写入 `entity_main`/`eav_data` 成功，`change_log` 生成且 `flushed_at=0`。
- `A-GI-005` `TestChangeLogWritesOnUpdateAndDeleteIntegration`  
  验收条件：更新后 `changed_at` 变化；删除后 `deleted_at` 被正确标记。
- `A-GI-006` `TestRunOptimizedQueryIntegration`  
  验收条件：优化查询返回记录、总数及属性符合预期。
- `A-GI-007` `TestEvaluateRoutingPolicy_VariousStrategies`  
  验收条件：`hybrid`/`cost-first`/禁用配置下路由决策符合策略。
- `A-GI-009` `TestNewDuckDBClient_HealthCheck`  
  验收条件：DuckDB client 初始化成功且健康检查通过。
- `A-GI-011` `TestAppendDirtyExclusion`  
  验收条件：脏 ID 排除子句构造正确（包含 `NOT IN`，参数数量与脏 ID 一一对应）。
- `A-GI-012` `TestFinalizeDuckDBExecutionPlan_CaptureDisabled`  
  验收条件：执行计划捕获关闭时，finalize 路径可安全执行且不崩溃。
- `A-GI-013` `TestBuildDuckDBQuery_AdvancedTemplate`  
  验收条件：高级查询模板携带 Limit/Offset 参数渲染成功，产出非空 SQL 与非 nil 参数。
- `A-GI-014` `TestExecuteDuckDBFederatedQuery_NilQuery`  
  验收条件：空查询入参返回明确错误。
- `A-GI-015` `TestBuildDuckDBQuery_PropagatesRenderError` (`internal/sqlgen`)  
  验收条件：模板渲染错误由 `BuildDuckDBQuery` 向上抛出（surfaced），不被吞掉。
- `A-GI-016` `TestBuildDuckDBQuery_KeysetArgsBindLast` (`internal/sqlgen`)  
  验收条件：keyset 查询参数合并顺序正确——keyset 值最后绑定，`?` 占位符数量与参数数一致，且不出现 `$n` 占位符。

## 3.2 Go E2E Harness 基础用例

执行入口：

- `go test -v ./internal/e2e_harness/... -timeout=5m`
- `go test -v ./internal/e2e_harness/federated/... -tags=e2e -run "TestSuite_Smoke|TestSuite_Integration"`

用例清单：

- `A-EH-001` `TestE2EHarnessMinimal`  
  验收条件：Postgres/S3/DuckDB 启动成功；可写 Parquet、上传 S3、DuckDB 可读 Parquet。
- `A-EH-002` `TestSuite_Smoke`  
  验收条件：测试基础设施可用，基础数据操作/查询/S3 操作可通。
- `A-EH-003` `TestSuite_Integration`  
  验收条件：全链路（Seed -> Query -> Flush -> Compaction -> Verify）完成，无重复记录。

## 3.3 Federated E2E（按 TC 分类）

执行入口：

- 全量：`go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m`
- 按分类：`go test -v ./internal/e2e_harness/federated/... -tags=e2e -run "<分类前缀>"`

### TC-01 三层数据架构（Data Tier）

- `A-F01-001` `TestDataTier_S3BaseFilesOnly`  
  验收条件：仅 Base 层存在时联邦查询可返回有效结果。
- `A-F01-002` `TestDataTier_S3DeltaFilesOnly`  
  验收条件：仅 Delta 层存在时联邦查询可返回有效结果。
- `A-F01-003` `TestDataTier_PostgresHotBufferOnly`  
  验收条件：仅 Hot Buffer 存在时查询可返回实时数据。
- `A-F01-004` `TestDataTier_AllThreeTiers`  
  验收条件：三层联合查询成功，结果完整。
- `A-F01-005` `TestDataTier_TierPriorityOrder`  
  验收条件：层优先级与合并顺序符合设计（Base/Delta/Hot 合并逻辑）。
- `A-F01-006` `TestDataTier_EmptyTiers`  
  验收条件：空层场景无异常，返回空结果而非错误。
- `A-F01-007` `TestDataTier_LargeLimitPagination`  
  验收条件：大分页参数下结果与总数一致，无重复/漏数。

### TC-02 Merge-on-Read

- `A-F02-001` `TestMergeOnRead_UnionAllCorrectness`  
  验收条件：非重叠数据 `UNION ALL` 后结果正确。
- `A-F02-002` `TestMergeOnRead_OverlappingRecords`  
  验收条件：跨层重叠 row_id 去重正确。
- `A-F02-003` `TestMergeOnRead_LastWriteWins`  
  验收条件：同 row_id 多版本按最新 `changed_at` 生效。
- `A-F02-004` `TestMergeOnRead_DirtyIDExclusion`  
  验收条件：Dirty Set 中 row_id 从 S3 结果中被排除。
- `A-F02-005` `TestMergeOnRead_MultipleOverlappingRecords`  
  验收条件：多条重叠记录合并后不出现重复版本。
- `A-F02-006` `TestMergeOnRead_MixedCleanAndDirty`  
  验收条件：脏/净数据混合时结果仍保持一致性。
- `A-F02-007` `TestMergeOnRead_TimeSlotOrdering`  
  验收条件：按时间槽/版本序列排序稳定。

### TC-03 全局去重（Deduplication）

- `A-F03-001` `TestDeduplication_SameTier`  
  验收条件：单层重复记录可去重。
- `A-F03-002` `TestDeduplication_CrossTier`  
  验收条件：跨层重复记录可去重。
- `A-F03-003` `TestDeduplication_BulkPerformance`  
  验收条件：大批量重复数据下仍可完成去重。
- `A-F03-004` `TestDeduplication_UUIDv7TimeOrdering`  
  验收条件：UUIDv7 时序特性与去重结果兼容。
- `A-F03-005` `TestDeduplication_NoFalsePositives`  
  验收条件：不同 row_id 不会被误判为重复。
- `A-F03-006` `TestDeduplication_MultipleRowsWithVersions`  
  验收条件：多 row、多版本场景下每 row 保留单一最新版本。

### TC-04 软删除过滤（Soft Delete）

- `A-F04-001` `TestSoftDelete_ExcludeDeleted`  
  验收条件：已软删除记录不会出现在查询结果。
- `A-F04-002` `TestSoftDelete_NullVsZeroDeletedAt`  
  验收条件：`deleted_at` 为 `NULL`/`0` 均视为未删除。
- `A-F04-003` `TestSoftDelete_RestoreAfterDelete`  
  验收条件：恢复后的记录可再次被查询。
- `A-F04-004` `TestSoftDelete_DeleteThenReuse`  
  验收条件：删除后复用 row_id 场景行为正确。
- `A-F04-005` `TestSoftDelete_AllTiersDeleted`  
  验收条件：所有层已删除时结果不泄漏旧数据。
- `A-F04-006` `TestSoftDelete_BulkDeletedExclusion`  
  验收条件：高删除比例场景下过滤正确。
- `A-F04-007` `TestSoftDelete_DeletedAtTimestampPrecision`  
  验收条件：毫秒精度时间戳处理正确。

### TC-05 CDC 智能 Flush

- `A-F05-001` `TestCDCFlush_MinRecordsThreshold`  
  验收条件：记录数阈值机制可触发 flush；大于阈值时可 flush。  
  备注：当前 harness 对 “低于阈值不触发” 断言不严格，需手工补充。
- `A-F05-002` `TestCDCFlush_MaxAgeThreshold`  
  验收条件：最老未刷记录超过年龄阈值可触发 flush。
- `A-F05-003` `TestCDCFlush_AdvisoryLockPreventsConurrent`  
  验收条件：并发 flush 时 advisory lock 防止重复刷写。
- `A-F05-004` `TestCDCFlush_RecordsMarkedFlushed`  
  验收条件：flush 后 `flushed_at` 被正确更新。
- `A-F05-005` `TestCDCFlush_DeltaFileNaming`  
  验收条件：Delta 文件命名规则与后缀正确。
- `A-F05-006` `TestCDCFlush_BatchSizeRespected`  
  验收条件：单次 flush 不超过 batch size，剩余记录可继续刷。
- `A-F05-007` `TestCDCFlush_MultipleFlushesComplete`  
  验收条件：多轮 flush 最终可清空未刷记录。

### TC-06 Compaction

- `A-F06-001` `TestCompaction_NewDataAppendsToDeltas`  
  验收条件：新增数据先进入 Delta，不直接重写 Base。
- `A-F06-002` `TestCompaction_LowDirtyRatioSkipsCompaction`  
  验收条件：低脏比场景可识别为低紧急度。  
  备注：当前 harness 未严格断言 “必须 skip rewrite”，需手工补充。
- `A-F06-003` `TestCompaction_HighDirtyRatioTriggersRewrite`  
  验收条件：高脏比场景会执行合并，Delta 被吸收/清空。
- `A-F06-004` `TestCompaction_MergesMultipleDeltaFiles`  
  验收条件：多 Delta 文件可合并到 Base，行数一致。
- `A-F06-005` `TestCompaction_PreservesDeduplication`  
  验收条件：Compaction 后去重语义保持（无重复版本）。
- `A-F06-006` `TestCompaction_FileSizeRotation`  
  验收条件：可读取大文件元数据，验证大文件场景可处理。  
  备注：当前自动化未严格验证 256MB 旋转策略，需手工补充。
- `A-F06-007` `TestCompaction_PreservesSoftDeletes`  
  验收条件：合并后软删除记录仍被正确过滤。
- `A-F06-008` `TestCompaction_DurationWithinThreshold`  
  验收条件：中等数据量下 compaction 在阈值内完成（30s 内）。
- `A-F06-009` `TestCompaction_EmptyDeltaNoOp`  
  验收条件：无 Delta 文件时为 no-op，不误改 Base。

### TC-07 一致性（Consistency）

- `A-F07-001` `TestConsistency_CountMatch`  
  验收条件：计数对齐（或三层场景满足 Federated >= Postgres）。
- `A-F07-002` `TestConsistency_AttributeValueMatch`  
  验收条件：属性值无不一致。
- `A-F07-003` `TestConsistency_ChecksumValidation`  
  验收条件：同数据 checksum 一致，数据变化 checksum 改变。
- `A-F07-004` `TestConsistency_AfterCDCFlush`  
  验收条件：Flush 前后计数/checksum 保持，且无重复。
- `A-F07-005` `TestConsistency_AfterCompaction`  
  验收条件：Compaction 前后计数/checksum 保持，且无重复。
- `A-F07-006` `TestConsistency_RowIDExistence`  
  验收条件：抽样 row_id 在两侧结果中都可定位。
- `A-F07-007` `TestConsistency_MissingRecordDetection`  
  验收条件：可识别仅在 S3 或仅在 Postgres 的缺失差异。
- `A-F07-008` `TestConsistency_DeduplicationAcrossComparison`  
  验收条件：比较过程中 dedup 结果一致。
- `A-F07-009` `TestConsistency_TimestampOrdering`  
  验收条件：时间戳有效且排序语义可验证。

### TC-08 性能（Performance）

- `A-F08-001` `TestPerformance_SimplePagination`  
  验收条件：分页查询 P95 延迟满足放宽阈值（代码中按基准 *3）。
- `A-F08-002` `TestPerformance_ComplexFilter`  
  验收条件：复杂过滤查询 P95 满足放宽阈值。
- `A-F08-003` `TestPerformance_FullTableScan`  
  验收条件：全表扫描 P95 满足放宽阈值。
- `A-F08-004` `TestPerformance_ConcurrentQueries`  
  验收条件：成功率 > 90%，吞吐与延迟达到放宽门槛。
- `A-F08-005` `TestPerformance_CDCFlush`  
  验收条件：flush 延迟 P95 满足阈值（默认 10s）。
- `A-F08-006` `TestPerformance_Compaction`  
  验收条件：compaction 行数/文件数正确，时延不超过阈值。
- `A-F08-007` `TestPerformance_QueryLatencyDistribution`  
  验收条件：输出可用延迟分布报告（P50/P95/P99 和桶分布）。
- `A-F08-008` `TestPerformance_MemoryUsage`  
  验收条件：连续查询与流式查询期间无 OOM/崩溃。

### TC-09 失败模式（Failure Modes）

- `A-F09-001` `TestFailureMode_S3Unavailable`  
  验收条件：S3 不可用时系统可降级或返回可诊断错误；恢复后可写。
- `A-F09-002` `TestFailureMode_PostgresUnavailable`  
  验收条件：Postgres 异常场景处理平稳，flush 失败可感知。
- `A-F09-003` `TestFailureMode_CorruptedParquet`  
  验收条件：缺失/损坏 parquet 时错误可识别，恢复后查询可用。
- `A-F09-004` `TestFailureMode_QueryTimeout`  
  验收条件：超时 context 返回 `deadline exceeded`；正常超时可成功。
- `A-F09-005` `TestFailureMode_PartialFailureRecovery`  
  验收条件：部分 flush 失败后恢复执行可继续推进，不丢数据。
- `A-F09-006` `TestFailureMode_GracefulDegradation`  
  验收条件：S3 故障时 Hot Buffer 读取可用，计数保持正确。
- `A-F09-007` `TestFailureMode_ConcurrentFailures`  
  验收条件：并发故障下注入失败不会导致全量不可用（仍有成功请求）。
- `A-F09-008` `TestFailureMode_DataIntegrityAfterFailure`  
  验收条件：故障与重试后无数据丢失、无重复。

## 3.4 Bun/TS E2E 与 k6 自动化用例

执行入口（`tests/e2e/package.json`）：

- `bun run register-schemas`
- `bun run gen-data`
- `bun run cdc-init`
- `bun run cdc-flush`
- `bun run compactor`
- `bun run federated-check`
- `bun run test`（流水线）
- `bun run k6-smoke`
- `bun run k6-full`
- `bun run k6-perf`

用例清单：

- `A-TS-001` `register-schemas`  
  验收条件：目标 schema 状态为 `registered`/`already_exists`，无 `error`。
- `A-TS-002` `gen-data`  
  验收条件：批量写入成功数与请求数一致，失败数可接受为 0。
- `A-TS-003` `cdc-init`  
  验收条件：命令退出码为 0；报告含导出行数/文件数。
- `A-TS-004` `cdc-flush`  
  验收条件：命令退出码为 0；报告显示 flush 成功并产生 Delta 文件。
- `A-TS-005` `compactor`  
  验收条件：目标 schema compaction 成功，失败数为 0。
- `A-TS-006` `federated-check`  
  验收条件：各 schema 比对通过，计数匹配且属性 mismatch 为 0。
- `A-TS-007` `test`（`register-schemas -> gen-data -> cdc-flush -> federated-check`）  
  验收条件：串行流水线全通过。
- `A-TS-008` `k6-smoke`  
  验收条件：smoke 负载下通过阈值检查并产出报告。
- `A-TS-009` `k6-full`  
  验收条件：full 负载下满足阈值（p95、成功率、错误率）。
- `A-TS-010` `k6-perf`  
  验收条件：perf 负载下满足阈值或触发可追踪告警。

## 3.5 非门禁基准用例（Benchmark）

- `BenchmarkFederatedQuery`  
  用途：联邦查询吞吐/延迟基线观测（趋势性指标）。
- `BenchmarkCDCFlush`  
  用途：CDC flush 吞吐与稳定性基线观测。

## 4. 手工集成测试用例（补充）

说明：以下手工用例用于补齐自动化未完全覆盖或生产上线前必须人工验收的场景。

| ID | 场景 | 关键步骤 | 验收条件 | 优先级 |
|---|---|---|---|---|
| `M-001` | Server 冷启动与 schema 自动加载 | 启动本地 server，读取 `SCHEMA_DIR` 下 schema | 服务启动成功；`/api/v1/<schema>` 可访问且非 404 | P0 |
| `M-002` | Lambda 路由与健康检查 | 部署 Lambda/API Gateway，调用 `/health` 和 API | `/health=200`；CRUD/查询路由均可用 | P0 |
| `M-003` | Lambda DSQL IAM 鉴权 | 配置 `DSQL_ENDPOINT`，不使用 DB 密码 | IAM token 连接成功，可执行查询/写入 | P1 |
| `M-004` | API 错误码契约 | 构造非法 method/path/body/sort/uuid | 返回 400/405，错误信息可诊断 | P0 |
| `M-005` | 批量创建原子性 | 批量创建中注入 1 条非法记录 | 事务原子：整体失败或明确部分失败策略与文档一致 | P0 |
| `M-006` | 单删/批删一致性 | 执行 DELETE 单条与批量后复查 | 删除记录不可见，未删记录可见，计数正确 | P0 |
| `M-007` | `attrs` 投影正确性 | 使用 `attrs=a,b` 查询单条和列表 | 仅返回指定属性（含必要元数据） | P1 |
| `M-008` | Advanced Query DSL 复杂嵌套 | 构造多层 `and/or` + 多操作符 | 返回结果与离线预期集合一致 | P0 |
| `M-009` | Cross-schema 搜索过滤 | `schemas=lead,visit` 与全局搜索对比 | 指定 schema 过滤生效；分页/计数正确 | P1 |
| `M-010` | 向后兼容 schema 演进 | 增加可选字段/放宽约束后写入读取 | 历史数据可读，新数据可写，无停机 | P0 |
| `M-011` | 非兼容 schema 变更拦截 | 尝试改字段类型/新增必填/删除字段 | 变更被拒绝，旧数据与服务不受影响 | P0 |
| `M-012` | 类型 fallback 精度 | 设计逼近列上限数据，验证数值/日期/UUID/bool fallback | 查询比较结果正确，无明显精度回归 | P1 |
| `M-013` | CDC Init Dry Run 安全性 | 执行 `cdc-init --dry-run` | 不落地文件，仅输出计划/统计 | P0 |
| `M-014` | CDC Init 全量导出 | 执行 `cdc-init` 并检查 Base 与 manifest | Base 文件/manifest 生成；导出行数与源数据一致 | P0 |
| `M-015` | CDC Flush 阈值与幂等 | 分别构造低于/高于阈值与重复执行 | 低于阈值不触发；高于阈值触发；重复执行不重复刷 | P0 |
| `M-016` | Compaction 阈值策略 | 构造低脏比和高脏比两组数据 | 低脏比不重写；高脏比合并并清理 Delta | P0 |
| `M-017` | S3 故障恢复链路 | 故障期间执行查询/flush，恢复后重试 | 恢复后可继续处理，无数据丢失/重复 | P0 |
| `M-018` | 可观测性与日志审计 | 检查 server/tools 日志字段 | 含关键字段（schema/row/rows_flushed/耗时）；不泄露敏感信息 | P1 |
| `M-019` | 注入与输入安全 | 在 query DSL、`q`、`sort_by` 注入恶意内容 | 查询被安全处理，库表无破坏，服务可继续 | P0 |
| `M-020` | 生产级性能门禁 | 使用真实规模数据跑 `k6-full`/`k6-perf` | 满足 SLO（p95、成功率、失败率） | P0 |
| `M-021` | 灾备恢复演练 | 备份后恢复 Postgres + S3 元数据，再回放查询 | 恢复后结果一致，可继续写入/flush/compaction | P1 |

## 5. 自动化覆盖缺口（需重点手工补齐）

- `G-001` CDC 最小记录阈值 “低于阈值不触发” 在当前 harness 断言不严格  
  对应用例：`M-015`
- `G-002` Compaction 低脏比 skip rewrite 策略未在自动化中严格断言  
  对应用例：`M-016`
- `G-003` 256MB 文件旋转策略未在自动化中完整模拟  
  对应用例：`M-016`
- `G-004` Postgres 真正不可用（容器级）演练不足  
  对应用例：`M-017`
- `G-005` Lambda + API Gateway + DSQL 组合验收目前主要依赖手工  
  对应用例：`M-002`、`M-003`

## 6. 推荐执行顺序

1. 运行 Go 内部集成测试（`A-GI-*`）  
2. 运行 Go E2E Harness（`A-EH-*` + `A-F**-*`）  
3. 运行 Bun E2E 流水线（`A-TS-007`）  
4. 运行 k6（`A-TS-008/009/010`）  
5. 执行手工 P0（`M-001`、`M-002`、`M-004`、`M-005`、`M-006`、`M-008`、`M-010`、`M-011`、`M-013`、`M-014`、`M-015`、`M-016`、`M-017`、`M-019`、`M-020`）

## 7. 参考实现位置

- Go 集成：`internal/integration_suite_test.go`  
- Repo 集成：`internal/postgres_persistent_repository_integration_test.go`  
- DuckDB/联邦集成：`internal/federated/duckdb_federated_integration_test.go`  
- E2E Harness：`internal/e2e_harness/e2e_test.go`  
- Federated E2E：`internal/e2e_harness/federated/*_test.go`  
- Bun E2E：`tests/e2e/scripts/*.ts`  
- 负载测试：`tests/e2e/k6/scenarios.ts`  
- 服务 API：`internal/httpapi/server.go`、`cmd/lambda/main.go`
