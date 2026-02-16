# Forma Refactor Plan (Findings Only)

## 评审基准
- 参考 [Refactoring.Guru - Code Smells](https://refactoring.guru/refactoring/smells) 及其子类（尤其是 `Duplicate Code`, `Long Method`, `Long Parameter List`, `Data Clumps`, `Large Class`, `Switch Statements`, `Shotgun Surgery`, `Divergent Change`）。
- 本文仅记录重构发现，不包含代码修改方案的落地实现。

## 优先级约定
- `P0`：高优先级，建议优先重构（会明显拖慢迭代/放大回归风险）。
- `P1`：中优先级，建议在下一轮代码整理中处理。
- `P2`：低优先级，可在相关模块变更时顺手收敛。

## Findings

### 1) `P0` Duplicate Code + Divergent Change: HTTP 处理器在 `cmd/server` 和 `cmd/lambda` 基本镜像复制
- Smell: `Duplicate Code`, `Divergent Change`
- 证据:
  - `cmd/server/handlers.go:13`
  - `cmd/lambda/handlers.go:25`
  - `cmd/server/utils.go:15`
  - `cmd/lambda/utils.go:15`
- 说明:
  - 两套 handler/参数解析/响应写出逻辑几乎一致，仅 Lambda 多了 health route。
  - 一个接口行为修改需要在两处同步，极易出现行为漂移。
- 重构方向:
  - 提取共享 HTTP API 层（如 `internal/httpapi`），`server` 与 `lambda` 仅保留 transport/adapter 装配。

### 2) `P0` Duplicate Code: 启动与数据库连接池构建逻辑多点重复
- Smell: `Duplicate Code`, `Shotgun Surgery`
- 证据:
  - `cmd/server/main.go:117`
  - `cmd/lambda/main.go:154`
  - `cmd/server/factory.go:27`
  - `cmd/server/main.go:155`
  - `cmd/lambda/main.go:250`
- 说明:
  - 连接串拼装、pool 配置、ping 健康检查、环境变量解析在多个入口重复。
  - 连接策略变更（超时、连接数、ssl）会引发多点修改。
- 重构方向:
  - 提取统一 bootstrap/config loader + db pool builder，入口仅传入运行模式参数。

### 3) `P0` Long Method + Long Parameter List + Data Clumps: CDC flush/init 主流程过长且上下文参数臃肿
- Smell: `Long Method`, `Long Parameter List`, `Data Clumps`
- 证据:
  - `internal/cdc/flusher.go:247` (`executeFlush`)
  - `cmd/tools/cdc_init.go:291` (`initSchema`)
  - `cmd/tools/cdc_init.go:190` (`runInit`)
- 说明:
  - 单方法同时处理批次切分、导出、拷贝、mark flushed、manifest 更新、日志。
  - 大量固定组合参数反复传递（`ctx/db/duck/s3/cfg/logger/manifest/...`）。
- 重构方向:
  - 引入 `FlushJob` / `InitJob` 上下文对象，拆分为 `select/export/copy/mark/manifest` 的步骤函数。
  - 将 chunk 与非 chunk 分支合并为统一批次执行管线。

### 4) `P0` Duplicate Code + Long Method: 两套 DuckDB 导出 SQL 生成器高度重复
- Smell: `Duplicate Code`, `Long Method`
- 证据:
  - `internal/cdc/duckdb_exporter.go:122` (`buildExportSQL`)
  - `internal/cdc/init_exporter.go:40` (`buildBaseExportSQL`)
  - 共享辅助逻辑被交叉依赖在同文件/跨文件（如 `castMainValue`, `castEAVValue`, `safeColumnAlias`）
- 说明:
  - base/delta 两套 SQL 拼接路径大量相同，维护和测试成本高。
  - 属性投影、EAV 聚合、压缩参数拼接都存在重复分支。
- 重构方向:
  - 提取统一 SQL AST/Builder（区分 source: `change_log` vs `entity_main`），减少字符串模板复制。

### 5) `P0` Duplicate Code + Shotgun Surgery: 条件解析/操作符映射在多个查询路径重复实现
- Smell: `Duplicate Code`, `Shotgun Surgery`
- 证据:
  - `internal/sql_generator.go:147`
  - `internal/dualpath_sql_helpers.go:21`
  - `internal/postgres_condition_helpers.go:81`
  - `internal/queryoptimizer/normalizer.go:158`
- 说明:
  - `op:value` 解析、SQL 操作符映射、日期/数值转换在多处平行实现。
  - 新增操作符或修正语义时，必须跨多个模块同步，容易出现 PG/DuckDB 语义不一致。
- 重构方向:
  - 抽象统一 `ConditionParser + OperatorSemantics + ValueCoercion` 组件，查询后端只做方言适配。

### 6) `P1` Switch Statements + Primitive Obsession: 类型转换逻辑分散且大量 `switch any`
- Smell: `Switch Statements`, `Primitive Obsession`, `Duplicate Code`
- 证据:
  - `internal/attribute_converter.go:177`
  - `internal/attribute_converter.go:263`
  - `internal/transformer.go:285`
  - `internal/duckdb_type_mapper.go:65`
- 说明:
  - 数值/布尔/时间/UUID 转换策略在多个模块重复，且依赖 `any + switch`。
  - 类型扩展或编码策略变化会触发多点同步。
- 重构方向:
  - 引入集中式 `ValueCodec` 注册表（按 `ValueType` + encoding），统一 PG/EAV/DuckDB 转换入口。

### 7) `P1` Duplicate Code + Inconsistent Behavior: `sanitizeIdentifier` 存在三份实现且语义不一致
- Smell: `Duplicate Code`, `Divergent Change`
- 证据:
  - `internal/utils.go:21`（使用 `pgx.Identifier(...).Sanitize()`）
  - `internal/cdc/helpers.go:158`（仅去除反引号）
  - `cmd/tools/cdc_init.go:497`（直接包双引号）
- 说明:
  - 同名函数行为不同，调用方难以预期安全边界与转义规则。
  - 标识符处理规则更新时会出现隐蔽不一致。
- 重构方向:
  - 收敛到单一包级实现，显式区分“标识符拼接”和“用户输入校验”职责。

### 8) `P1` Duplicate Code: 主表 SQL 构建在 insert/update 路径重复遍历同构字段
- Smell: `Duplicate Code`
- 证据:
  - `internal/postgres_persistent_repository_main_table.go:30`
  - `internal/postgres_persistent_repository_main_table.go:108`
- 说明:
  - `Text/Int16/Int32/Int64/Float64/UUID` 六类 map 的排序、校验、拼接在 insert/update 基本重复。
  - 扩展新列类型或调整排序策略时要多处改动。
- 重构方向:
  - 抽象统一字段迭代器/column appender，insert/update 复用同一元逻辑。

### 9) `P1` Duplicate Code: batch CRUD 三个方法结构高度重复
- Smell: `Duplicate Code`
- 证据:
  - `internal/entity_manager_batch.go:13`
  - `internal/entity_manager_batch.go:58`
  - `internal/entity_manager_batch.go:101`
- 说明:
  - 三个方法都在做“遍历操作 -> 调用单条方法 -> 聚合成功失败 -> 统计耗时”。
  - 错误码、日志、原子语义调整需要三处改。
- 重构方向:
  - 引入泛化 batch executor（策略传入单条执行函数与错误码）。

### 10) `P1` Duplicate Code: schema registry 的 DB 模式与目录模式加载流程重复
- Smell: `Duplicate Code`
- 证据:
  - `internal/file_schema_registry.go:58`
  - `internal/file_schema_registry.go:278`
- 说明:
  - 两个流程都在做：读取 attributes 文件 -> 解析 metadata -> 读取 schema 文件 -> 填充 cache/map。
  - 文件格式或解析规则变更会引起双处维护。
- 重构方向:
  - 提取通用 `loadSchemaArtifacts(schemaName, schemaID)`，DB/目录模式只负责“schema 列表来源”。

### 11) `P2` Duplicate Code: `QueryRequest` 与 `CrossSchemaRequest` 的 JSON 编解码重复
- Smell: `Duplicate Code`
- 证据:
  - `types.go:150`
  - `types.go:175`
  - `types.go:203`
  - `types.go:228`
- 说明:
  - 两个结构的 `condition` 字段编解码模式一致，重复模板化代码。
- 重构方向:
  - 抽出通用 condition marshal/unmarshal helper，减少样板代码。

### 12) `P2` Large Class: `entityManager` 同时承担 CRUD、Batch、Query、Relations enrichment
- Smell: `Large Class`
- 证据:
  - `internal/entity_manager_crud.go:13`
  - `internal/entity_manager_batch.go:13`
  - `internal/entity_manager_query.go:13`
  - `internal/entity_manager_relations.go:12`
- 说明:
  - 同一对象整合过多变化原因（写路径、查询路径、跨 schema、关系补全）。
  - 新能力迭代时容易扩大回归面。
- 重构方向:
  - 按用例拆分为 `EntityCommandService` / `EntityQueryService` / `RelationEnricher`，`entityManager` 仅编排。

## 建议的实施顺序（仅排序，不代表本次改动）
1. 先做 `P0`（共享 API 层、统一条件解析、CDC 导出管线拆分）。
2. 再做 `P1`（统一转换器、主表 SQL builder 抽象、registry 加载抽象）。
3. 最后做 `P2`（样板代码与服务边界整理）。

## Effective Go 补充检查（Round 2）
- 参考: [Effective Go](https://go.dev/doc/effective_go)（重点对照 `if`、`errors`、`panic`、`init`、命名与接口使用习惯）。

### 13) `P0` Errors Should Be Values: CDC 主流程大量“记录日志后返回”，错误不会向上抛出
- Effective Go 对照点: `errors`（错误应作为值返回并由调用方决策处理）
- 证据:
  - `internal/cdc/flusher.go:185` (`processSchema` 返回 `void`)
  - `internal/cdc/flusher.go:247` (`executeFlush` 返回 `void`)
  - `internal/cdc/flusher.go:100` (`RunOnce` 循环调用时无法感知单 schema 失败)
- 说明:
  - 失败被局部日志吞掉，调用方只能看到“整体成功返回”，可观测性和重试策略都受限。
- 重构方向:
  - 把 `processSchema/executeFlush` 改为返回 `error`（可附带 schemaID 上下文），由 `RunOnce` 统一聚合/判定失败策略。

### 14) `P0` Panic Usage: 非 `main` 路径使用 `panic`，可恢复错误被提升为进程级失败
- Effective Go 对照点: `panic`
- 证据:
  - `internal/sql_template_renderer.go:36`
  - `cmd/server/factory.go:13`
  - `cmd/server/factory.go:16`
  - `cmd/server/factory.go:21`
- 说明:
  - `Ident` 校验失败目前直接 `panic`，无法由调用方按业务语义处理。
  - `NewEntityManager`（helper）在连接失败时 `panic`，不利于测试与错误注入。
- 重构方向:
  - 模板标识符校验改为 `error` 返回路径。
  - 工厂函数返回 `(forma.EntityManager, error)`，由 `main` 决定 `fatal` 或降级。

### 15) `P1` Init Function Overuse: Lambda `init()` 承担重型启动逻辑（网络/数据库/构造器）
- Effective Go 对照点: `The init function`
- 证据:
  - `cmd/lambda/main.go:61`
  - `cmd/lambda/main.go:202`
  - `cmd/lambda/main.go:234`
- 说明:
  - `init()` 无法返回错误，只能 `panic/fatal`；当前做了 AWS config、DB pool、schema registry、manager 全套初始化。
  - 启动路径不可组合，单测和局部复用困难。
- 重构方向:
  - 拆出显式 `bootstrap(ctx) (*httpadapter.HandlerAdapterV2, error)`，`main` 决策失败处理。

### 16) `P1` Interface/API Clarity: 形参与注释约定不一致，造成误导接口
- Effective Go 对照点: 接口应简洁且语义清晰（避免“看起来可注入，实际没用”）
- 证据:
  - `internal/cdc/flusher.go:50`（注释声明可传入 `S3ObjectClient`）
  - `internal/cdc/flusher.go:109`（`setupAWSClient(..., s3Client S3ObjectClient)` 未使用该参数）
  - `internal/postgres_persistent_repository.go:27`
  - `internal/postgres_persistent_repository.go:31`（构造函数传入 `duckDBClient` 但字段被写成 `nil`）
- 说明:
  - API 对外承诺与实现不一致，降低可维护性与可测试性。
- 重构方向:
  - 清理未使用参数，或真实接入注入分支；保证“签名即契约”。

### 17) `P1` If Style: 大量 `if err != nil { return ... } else { ... }` 可扁平化
- Effective Go 对照点: `if`（错误分支 `return` 后通常省略 `else`）
- 证据:
  - `internal/postgres_persistent_repository_main_table.go:39`
  - `internal/postgres_persistent_repository_main_table.go:48`
  - `internal/postgres_persistent_repository_main_table.go:57`
  - `internal/postgres_persistent_repository_main_table.go:122`
- 说明:
  - 当前模式重复出现，缩进加深，阅读成本高。
- 重构方向:
  - 采用 early-return + 顺序执行，结合通用迭代器可同时消除重复代码与多层嵌套。

### 18) `P1` Fat Interfaces: 接口职责过宽，不利于按用例依赖
- Effective Go 对照点: 接口应围绕使用方保持小而专注
- 证据:
  - `storage.go:8` (`EntityManager` 同时暴露 CRUD/Batch/Query/CrossSchema)
  - `internal/interfaces.go:69` (`PersistentRecordRepository` 混合 OLTP 与 federated 查询职责)
- 说明:
  - 调用方常只需子集能力，但被迫依赖大接口；mock 和替换成本上升。
- 重构方向:
  - 拆分为 `EntityReader/EntityWriter/BatchOperator/FederatedQuerier` 等窄接口。

### 19) `P2` Naming Consistency: 首字母缩写命名不一致（`Sql` vs `SQL`）
- Effective Go 对照点: 命名保持一致、可读
- 证据:
  - `internal/sql_generator.go:52` (`ToSqlClauses`)
  - `internal/sql_generator_test.go:11` (`TestSQLGenerator_ToSqlClauses`)
- 说明:
  - 与同仓库 `SQLGenerator/RenderSQLTemplate` 风格不统一，检索与心智负担增加。
- 重构方向:
  - 统一 initialism 风格（例如 `ToSQLClauses`）并一次性全仓替换。

### 20) `P2` Long Method + Duplicate Mapping: SQL 渲染器内联了大段类型映射逻辑
- Effective Go 对照点: 保持函数简洁、减少重复、提高可读性
- 证据:
  - `internal/sql_template_renderer.go:49`
  - `internal/sql_template_renderer.go:61`
  - `internal/sql_template_renderer.go:117`
  - `internal/duckdb_type_mapper.go:13`
- 说明:
  - `Render` 内部定义多段闭包映射（`cast/param_cast/duck_type`），与 `duckdb_type_mapper` 的类型映射重复。
- 重构方向:
  - 提取共享 mapper，`Render` 仅负责模板执行与参数收集。

### 21) `P2` Defensive Programming: 指针分支缺少空值保护，存在潜在 panic 点
- Effective Go 对照点: 错误和边界条件显式处理
- 证据:
  - `internal/attribute_converter.go:271`（`case *string` 直接解引用）
- 说明:
  - 与同文件其他指针分支处理风格不一致（多数已有 `nil` 校验）。
- 重构方向:
  - 统一 pointer-input guard 规范，集中到转换 helper 层。
