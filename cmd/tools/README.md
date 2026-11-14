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
- `apply-schema`：按 D2 类型规则把修改后的 schema 声明记录为下一个 generation（#733）。
- `baseline-schema`：审计已存储的 PostgreSQL 与 parquet 数据，为尚无 generation 的 schema 记录第一个 generation（升级步骤，#733）。
- `migrate-rfc3339`：把已退役的 `iso8601` 编码存储的整秒图像改写为 `rfc3339` 图像（#842）。须先停止所有写入方。
- `manifest-reconcile`：比对 S3 parquet 对象与 manifest 条目，报告孤儿/dangling，
  `--repair` 补录 #197 flush 孤儿，`--gc` 清理 #188 compaction 遗留。
  详见 `docs/manifest-reconcile.md`。退出码：0 一致 / 2 有差异 / 1 失败。
- `validate-schema-consistency`：升级前校验 schema 元数据与 EAV 存储的一致性，
  使用与服务端相同的元数据加载路径。检查项包括：
  - 重复的 `attributeID` / `column_binding.col_name`；
  - `column_binding.col_name` 不是 `entity_main` 实际拥有的列（#557）；
  - `valueType`↔column-encoding 绑定无法 round-trip 的情况（#459）；
  - `eav_data` 中未知的 `attr_id`、存储列错位、list 属性下残留的标量行；
  - EAV-only `smallint`/`integer`/`bigint` 值超出声明宽度或非整数（#501）；自 #729 起 OLTP 读取会拒绝
    超出声明类型的 EAV `smallint`/`integer`（含 list 元素与提升为绑定列前遗留的 `eav_data` 行），
    每一条都报为失败，需通过 API 重写；
    绑定到 `double_*` 列的 `bigint` 值超出 ±2^53 或非整数（#618，读取 `-entity-main-table`，
    报告行、属性与列名；存在此类绑定而表名为空时报错）；
    绑定到更宽列的 `smallint`/`integer` 值超出声明类型范围、非整数、NaN 或无穷（#708，
    OLTP 读取会拒绝此类行；同样读取 `-entity-main-table`，需通过 API 重写，不会被重新排队）。
    `-change-log-table`（`CHANGE_LOG_TABLE`，默认 `change_log_dev`）用于区分已导出与待刷新的行；
    无 CDC 的部署传空值（`-change-log-table ''` 或显式 `CHANGE_LOG_TABLE=`，make 目标同样保留空值）；
    `-width-export-cutover`（RFC3339）为 #384 导出开始生效的时间，早于它导出的行会额外标注其 parquet 副本已过期，
    不改变判定；`-requeue-stale-width-exports` 已于 #729 停用：重新刷新无法修复任何一条报告，
    该标志仍被接受但不再重新排队任何行，只打印原因。
  - 以 `rfc3339` 编码绑定的 `date`/`datetime` 列中不是其时刻规范图像（`rfc3339.Image`）的每个图像（#842，读取 `-entity-main-table`）：
    OLTP 读取拒绝的图像（含形状规范但日期不存在的 `2024-02-30T03:04:05.250Z`），以及能读取但不规范的图像（带偏移、
    非三位小数、整秒图像、亚毫秒小数、偏移后超出 0000–9999 年的时刻）。后者在 PostgreSQL 路由（比较文本）与 DuckDB 层（比较时刻）上的结果可能不同。
    每一条都报为失败，附行、存储的图像与修复类别（仅改拼写 / 亚毫秒被向下取整 / 无规范图像），修复方法见 `docs/rfc3339-storage-contract.md`。
  - 以 `bool_text` 编码绑定的 `bool` 列中除 `"1"`、`"0"` 与 NULL 之外的每个图像（#404、#857，读取 `-entity-main-table`）：
    OLTP 读取与该行的每次更新都会拒绝它，`pgx` 热腿（#857 起的默认）下含该未刷新行的联邦查询页也会失败。
    每一条都报为失败，附行与存储的图像；API 无法修复，需用 SQL 改写该列，见 `docs/schema-consistency-migration.md`。
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
错误列出每个属性及新旧类型，attributes 文件保持原样。
标量变为同族元素的数组同样被拒绝：#204 之前的 attributes 文件把基本类型数组记成元素类型，无法与标量区分；
若该属性一直是数组，请手工改为 `valueType: list` 并以原类型作 `items_type`。这只保护本工具，手工编辑的 attributes 文件不受检查，
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

`init-db` 创建的每个文本列都声明为 `TEXT COLLATE "C"`（#816），与数据库默认排序规则无关。
`IF NOT EXISTS` 不会修改已存在的表，所以 `init-db` 在同一建表事务中读取 `pg_attribute`，
发现不在 `C` 排序规则下的文本列（早于 #816 创建的表）时回滚并以非零退出，列出表、列、实际与期望的排序规则，
并打印迁移所需的 `ALTER TABLE` 语句，每条之后跟一条 `ANALYZE`；`init-db` 从不自行执行这些语句。迁移会持有 `ACCESS EXCLUSIVE` 锁并重建相关索引，
还会删除被修改列的规划器统计信息，因此恢复服务之前必须先运行 `ANALYZE`，详见 `docs/text-collation.md`。

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

`init-db` 还会为 `-schema-table` 增加 `active_generation` 列并创建 generation 账本表 `<schema-table>_generation` 与 `<schema-table>_generation_attr`（#733），
并为本次新注册且有 attributes 文件的 schema 记录 generation 1。没有 attributes 文件的 schema 注册后没有 generation，`init-db` 打印提示，
待文件就绪后运行 `baseline-schema`。已注册的 schema 不受影响。详见 `docs/schema-evolution.md`。

示例：
- `go run ./cmd/tools init-db`
- `go run ./cmd/tools init-db -db-host 127.0.0.1 -db-name forma_dev -entity-main-table entity_main_dev -eav-table eav_dev`

apply-schema
------------
把每个已注册 schema 在 `-schema-dir` 中的声明与其 active generation 比较，在每一处变更都被 D2 类型规则允许时记录为下一个 generation（#733）。
规则拒绝任何一处变更时整体不记录，错误列出每个 schema、属性、新旧类型与处理方法；允许但有警告的变更（绑定 `bigint_*` 列的 `bigint` 改为 `numeric`）
打印警告并记录在 generation 上。内容未变的 schema 不记录。先运行 `apply-schema`，再滚动发布服务端、Lambda 与 CDC 进程：
仍加载旧声明的进程在新 generation 记录后拒绝启动。

`iso8601` 改为 `rfc3339`（同一列，#842）按仅改绑定的变更允许；但只要该列仍有整秒形状的图像（`YYYY-MM-DDThh:mm:ssZ`，包括工具留下待修复的无效图像），`apply-schema`
就拒绝，并列出 schema、属性、列与剩余行数，提示先运行 `migrate-rfc3339`。计数使用该工具自己的目标查询，读取 `-entity-main-table`。

选项（数据库选项及其环境变量默认值与 `init-db` 相同）：
- `-schema-dir`（`SCHEMA_DIR`，必填）：JSON Schema 目录。
- `-schema`：只处理这个已注册的 schema；省略时处理全部。
- `-schema-table`（`SCHEMA_TABLE`，默认 `schema_registry`）。
- `-entity-main-table`（`ENTITY_MAIN_TABLE`，默认 `entity_main_dev`）：仅在声明把 `iso8601` 改为 `rfc3339` 时读取。
- `-dry-run`：只报告将要记录的变更，不写入。

示例：
- `go run ./cmd/tools apply-schema -schema-dir schemas -dry-run`
- `go run ./cmd/tools apply-schema -schema-dir schemas`

baseline-schema
---------------
已有部署升级到带 generation 账本的版本前运行一次（#733）：按 OLTP 读取自身的规则审计 PostgreSQL（含 list 元素）：宽度普查（width census）；`numeric` 与 EAV 中的 `bool` 须为有限 float64（拒绝 NaN、无穷大或超出 float64 的值）；EAV 中的 `date`/`datetime` 须为 ±2^53 内的整数毫秒；EAV 中的 `uuid` 须能被 `uuid.Parse` 解析；值不得存放在另一个 `eav_data` 值列中；`bool_text` 列只能是 `"1"`/`"0"`，`rfc3339` 列须能按 RFC 3339 解析；能解析但不是其时刻规范图像的 `rfc3339` 图像逐行报告（附修复类别），但不拒绝基线（#842）。绑定主表列的属性在 EAV 提升为列绑定之前写入的 `eav_data` 行仍会被读取，同样按上述 EAV 规则审计。完整解码每个存活的 parquet 对象并按声明的值域扫描（含 list 元素），
数据与当前声明不符或无法读取时拒绝且不记录任何内容；通过后为尚无 generation 的 schema 记录 generation 1。已有 generation 的 schema 被跳过。
运行前须已有账本表（PostgreSQL 上运行 `init-db`；DSQL 上按 `docs/schema-evolution.md` 手工建表）。
基线是离线的：运行前须停止旧版本中所有写入的服务端、Lambda 与嵌入方，以及所有 `cdc-flush`、`cdc-init` 运行，并暂停 compactor、不运行
`manifest-reconcile --repair`，直到新版本取代它们为止。旧版本没有启动门禁，它在基线期间或之后写入的值不会被审计。记录基线的事务会再次读取每个
schema 的 `entity_main` 行数与版本号之和以及存活的 parquet 对象集合，与该 schema 审计开始时不同则拒绝且不记录任何内容；这只能发现审计期间仍在运行的
写入方或导出方，发现不了基线提交后才写入的进程。`-dry-run` 可在旧版本运行时执行，用于停机前找出须修正的数据。

选项（数据库与表名选项及其环境变量默认值与 `validate-schema-consistency` 相同）：
- `-schema-dir`（`SCHEMA_DIR`，必填）：部署当前使用的 JSON Schema 目录。
- `-schema`：只处理这个已注册的 schema。
- `--no-parquet-tier`：部署没有 lakehouse 层，只审计 PostgreSQL。否则必须给出 `--s3-bucket` 与 `--manifest-template`
  （与服务端的 DuckDB manifest 配置一致），可选 `--manifest-prefix`（`MANIFEST_PREFIX`）、`--data-prefix`（`S3_PREFIX`）及 S3 与 DuckDB 选项。
- `-dry-run`：只报告审计结果，不写入。

示例：
- `go run ./cmd/tools baseline-schema -schema-dir schemas --no-parquet-tier`
- `go run ./cmd/tools baseline-schema -schema-dir schemas --s3-bucket my-bucket --manifest-template 'manifest/{{.SchemaID}}.json'`

migrate-rfc3339
---------------
`rfc3339` 编码取代了只保存整秒的 `iso8601` 编码（#842）：图像固定为 24 个字符 `YYYY-MM-DDThh:mm:ss.sssZ`，文本顺序即时间顺序。
两种图像不能共存于一列，所以升级前须改写旧图像；注册时拒绝声明 `iso8601`，并指向本工具。

目标取自账本：每个 schema 的 active generation 中编码为 `iso8601` 的属性（`<schema-table>_generation_attr`，含已退役属性），而非 attributes 文件。
工具把每个能被读取解析的整秒图像改写为前 19 个字符加 `.000Z`，即同一时刻的规范图像：按行号分页读取整秒形状的图像，每页至多 `-batch-size`
（默认 1000，上限 2999，低于 Aurora DSQL 每事务 3000 行的限制）行，用 `rfc3339.Judge` 逐个判断，把该页中有效的图像用一条语句改写（各自一个事务，
按读到的图像逐行匹配）；逐个判断这些列中的每个其他图像（`rfc3339.Judge`，与普查规则相同），列出改写后仍不规范的图像（带偏移、其他长度的小数、
亚毫秒小数、无法解析的字符串，包括形状规范但日期或时间不存在的 `2024-02-30T03:04:05.250Z`）及其 schema、属性、行与修复类别，并保持不变，
修复方法见 `docs/rfc3339-storage-contract.md`。整秒形状但不表示任何时刻的图像（`2023-02-29T00:00:00Z`）不被改写，保持原值并列出；
它仍按整秒图像计数，因此在修复该行之前，工具始终以非零状态退出，`apply-schema` 也始终拒绝改名。非 dry-run 运行在改写之后列出，所列即工具退出时列中的值；`-dry-run` 列出同样的行。改写不标记 `change_log`：时刻不变，parquet 保存的是时刻（epoch 毫秒）而不是图像。
重复运行不会再改写任何内容。只要目标列仍有整秒形状的图像就以非零状态退出（`-dry-run` 也是），错误信息会说明其中有多少须先修复，脚本化的切换会在 `apply-schema` 之前停下。

工具无法检测仍在运行的写入方：从改写开始到新版本启动，须停止该 schema 的所有写入方。完整步骤：
1. 停止写入方，运行 `migrate-rfc3339`；
2. 把 attributes 文件中的 `iso8601` 改为 `rfc3339`，运行 `apply-schema`；
3. 对每个唯一键包含此类属性的 schema 运行 `build-unique-keys --schema <name>`（`uniquekey.AttributesHash` 含编码，存储位置变化的键会被重新构建）；
4. 启动新版本。

选项（数据库选项及其环境变量默认值与 `apply-schema` 相同）：
- `-schema`：只处理这个已注册的 schema；省略时处理全部。
- `-schema-table`（`SCHEMA_TABLE`，默认 `schema_registry`）。
- `-entity-main-table`（`ENTITY_MAIN_TABLE`，默认 `entity_main_dev`）。
- `-batch-size`：每条改写语句的行数。
- `-dry-run`：只报告每列的计数并列出待修复的图像，不改写。

示例：
- `go run ./cmd/tools migrate-rfc3339 -dry-run`
- `go run ./cmd/tools migrate-rfc3339 -entity-main-table entity_main`

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
