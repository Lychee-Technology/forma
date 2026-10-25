Forma Tools CLI
===============

命令通过 `go run ./cmd/tools <command> [options]` 调用。

退出码与输出流
--------------
- 任何子命令失败都以非零状态退出：一般为 1，带语义退出码的命令用自己的退出码
  （`manifest-reconcile` 有差异时为 2）。`-h`/`--help` 打印用法后以 0 退出。
  脚本和 CI 步骤可以直接依赖退出码判断成败（#643）。
- 失败时的 `<command>: <error>` 行，以及未知命令、缺少命令时的用法说明，写到 stderr。
  stdout 留给命令自身的输出，例如 `inline-schema` 未指定 `-out` 时输出的 JSON、
  `compactor` 的指标行。

可用命令
--------
- `generate-attributes`：从 JSON Schema 生成 `<schema>_attributes.json`。
- `init-db`：创建 schema registry、entity main、EAV 数据表及索引。
- `build-unique-keys`：从 schema 的存活行构建 `x-forma-unique-keys` 声明的唯一键（#651）。须先停止所有写入方。
- `manifest-reconcile`：比对 S3 parquet 对象与 manifest 条目，报告孤儿/dangling，
  `--repair` 补录 #197 flush 孤儿，`--gc` 清理 #188 compaction 遗留。
  详见 `docs/manifest-reconcile.md`。退出码：0 一致 / 2 有差异 / 1 失败。
- `validate-schema-consistency`：升级前校验 schema 元数据与 EAV 存储的一致性，
  使用与服务端相同的元数据加载路径。检查项包括：
  - 重复的 `attributeID` / `column_binding.col_name`；
  - `column_binding.col_name` 不是 `entity_main` 实际拥有的列（#557）；
  - `valueType`↔column-encoding 绑定无法 round-trip 的情况（#459）；
  - `eav_data` 中未知的 `attr_id`、存储列错位、list 属性下残留的标量行；
  - EAV-only `smallint`/`integer`/`bigint` 值超出声明宽度或非整数（#501）；
    绑定到 `double_*` 列的 `bigint` 值超出 ±2^53 或非整数（#618，读取 `-entity-main-table`，
    报告行、属性与列名；存在此类绑定而表名为空时报错）；
    绑定到更宽列的 `smallint`/`integer` 值超出声明类型范围、非整数、NaN 或无穷（#708，
    OLTP 读取会拒绝此类行；同样读取 `-entity-main-table`，需通过 API 重写，不会被重新排队）。
    `-change-log-table`（`CHANGE_LOG_TABLE`，默认 `change_log_dev`）用于区分已导出与待刷新的行；
    无 CDC 的部署传空值（`-change-log-table ''` 或显式 `CHANGE_LOG_TABLE=`，make 目标同样保留空值）；
    `-width-export-cutover`（RFC3339）为 #384 导出开始生效的时间，早于它导出的行报为失败；
    `-requeue-stale-width-exports` 通过推进版本并写入 `change_log` 将过期导出的
    `smallint`/`integer` 行重新排队（`bigint` 类需通过 API 重写，不会被重新排队），
    需在 `-entity-main-table`（`ENTITY_MAIN_TABLE`，默认 `entity_main_dev`）上执行，之后运行 `cdc-flush`。
  详见 `docs/schema-consistency-migration.md`。

generate-attributes
-------------------
选项（带默认值）：
- `-schema-dir`（`cmd/server/schemas`）：Schema 所在目录。
- `-schema`：Schema 名（不含 `.json`）；与 `-schema-file` 互斥。
- `-schema-file`：Schema 文件全路径；优先于 `-schema/-schema-dir`。
- `-out`：输出文件路径；默认写到 schema 同目录的 `<name>_attributes.json`。
- `-init`：允许创建全新的 attributes 文件；不指定此标志时，输出文件缺失会导致错误（因为生成会将所有 attributeID 从 1 重新编号）。

既有属性的类型（#732）：类型推断只产出每个类型族中的一个成员（数值 `numeric`、文本 `text`、日期 `date`、`bool`）。
既有条目的 `valueType`/`items_type` 与推断类型同族时保持不变，因此手工设置的 `bigint`、`integer`、`smallint`、`uuid`、`datetime`
在重新生成后依然保留。跨族变化（如 number 变 string、给 `text` 加上 `format: date`、标量变数组）会被拒绝：
错误列出每个属性及新旧类型，attributes 文件保持原样。这只保护本工具，手工编辑的 attributes 文件不受检查，
见 `docs/schema-evolution.md`。

示例：
- `go run ./cmd/tools generate-attributes -schema lead`（更新既有 attributes 文件）
- `go run ./cmd/tools generate-attributes -schema-file /path/to/schema.json`（再生成）
- `go run ./cmd/tools generate-attributes -schema-file /path/to/schema.json -out /tmp/schema_attributes.json -init`（创建新文件）

init-db
-------
选项（可用环境变量同名大写带下划线作默认值）：
- `-db-host`（`DB_HOST`，默认 `localhost`）
- `-db-port`（`DB_PORT`，默认 `5432`；`DB_PORT` 已设置但不是十进制整数（如 Kubernetes 注入的 `tcp://…`）时直接报错，即使同时传了 `-db-port`，但 `-h`/`--help` 仍会打印用法；`validate-schema-consistency` 同理，#600）
- `-db-name`（`DB_NAME`，默认 `forma`）
- `-db-user`（`DB_USER`，默认 `postgres`）
- `-db-password`（`DB_PASSWORD`，默认 `postgres`）
- `-db-ssl-mode`（`DB_SSL_MODE`，默认 `disable`）
- `-schema-table`（`SCHEMA_TABLE`，默认 `schema_registry`）
- `-eav-table`（`EAV_TABLE`，默认 `eav_dev`）
- `-entity-main-table`（`ENTITY_MAIN_TABLE`，默认 `entity_main_dev`）
- `-change-log-table`（`CHANGE_LOG_TABLE`，默认 `change_log_dev`）
- `-schema-dir`（`SCHEMA_DIR`，默认空即不注册）：把目录中的 JSON Schema（不含 `*_attributes.json`）
  注册到 `-schema-table`。已注册的 schema 保留原 `schema_id`。新 schema 按文件名排序，
  从现有最大 `schema_id` + 1 开始分配（空表从 100 开始），不回填空缺，因为空缺的 id
  可能仍是已删除注册的实体行或 parquet 文件的键。
- `-skip-declared-indexes`（默认 `false`）：跳过 `x-forma-indexes` 声明索引的创建，供在带外自行建索引的运维使用。

建表与建索引均为 `IF NOT EXISTS`，所以 `init-db` 可以对已初始化的数据库重复运行，
新增 schema 文件之后也可以（#643）。

指定 `-schema-dir` 时，`init-db` 在建表事务提交之后，为 schema 通过 `x-forma-indexes` 声明的组合索引
逐个执行 `CREATE INDEX CONCURRENTLY IF NOT EXISTS`（#647），不阻塞并发写入；索引已全部存在时不再发出任何索引 DDL。
声明索引无效（`indisvalid = false`，通常是被中断的并发建索引留下的）、键或形态与计划不符（如非 btree、带 INCLUDE 列）或名字被占用时，
`init-db` 以非零退出并给出索引名与处理方法。`init-db` 从不删除索引：声明被移除后，对应索引被报告为
unreferenced，由运维自行删除。详见 `docs/schema-indexes.md`。

`init-db` 还会创建唯一键表 `<entity-main-table>_unique_key` 与状态表 `<entity-main-table>_unique_key_state`（#651），无论是否指定 `-schema-dir`、是否有 schema 声明唯一键。
派生名超过 PostgreSQL 的 63 字节标识符上限时，改为表名前缀加 `_`、表名 SHA-256 的 16 位十六进制摘要和完整后缀，`init-db` 打印实际表名。
指定 `-schema-dir` 时，没有存活行的 schema 所声明的唯一键会被直接构建；已有存活行的 schema，`init-db` 打印需要运行的
`build-unique-keys` 命令。有声明的唯一键尚未构建时，服务端拒绝启动。即使 schema 没有存活行，也须在第一个带声明的写入方
启动前停止所有不带该声明的写入方：后者写入的行没有唯一键条目。详见 `docs/unique-keys.md`。

示例：
- `go run ./cmd/tools init-db`
- `go run ./cmd/tools init-db -db-host 127.0.0.1 -db-name forma_dev -entity-main-table entity_main_dev -eav-table eav_dev`

build-unique-keys
-----------------
从 schema 的存活行构建它声明的唯一键（#651）。构建是离线的：运行前须停止所有写入该 schema 的服务端、Lambda 与嵌入方，
否则构建期间漏掉的写入会让唯一键表缺少条目。存在共享同一键值的存活行时，构建失败并按组列出这些行的 RowID（不含键值），
本次运行的任何键都不写入条目。构建在扫描前先把本次运行要构建的每个键置为未构建（删除其状态与条目），因此失败的运行
（包括 `-rebuild` 发现重复值）会让这些键保持未构建，服务端在构建成功前拒绝启动。详见 `docs/unique-keys.md`。

选项（数据库与表名选项及其环境变量默认值与 `init-db` 相同）：
- `-schema`（必填）：要构建唯一键的 schema 名。
- `-key`：只构建这个键；省略时构建该 schema 声明且尚未构建的所有键，并删除 schema 不再声明的键。
  构建后其属性的存储位置（绑定列及其编码，或 EAV 属性 ID）发生变化的键视为未构建，按当前存储重新构建。
- `-rebuild`：即使键已按当前属性构建，也清空其条目重新构建。用于键构建后仍有不维护该键的写入方（如旧版本）写入过行的情况。
  按其他属性列表构建的键仍被拒绝。
- `-schema-dir`（`SCHEMA_DIR`，必填）：JSON Schema 目录，须与服务端使用的一致。
- `-schema-table`、`-eav-table`、`-entity-main-table`、`-db-*`：同 `init-db`。

示例：
- `go run ./cmd/tools build-unique-keys -schema event_record -key idempotency -schema-dir schemas`
- `go run ./cmd/tools build-unique-keys -schema event_record -schema-dir schemas`（构建全部键，删除已移除的键）
- `go run ./cmd/tools build-unique-keys -schema event_record -key idempotency -rebuild -schema-dir schemas`（重建已构建的键）
