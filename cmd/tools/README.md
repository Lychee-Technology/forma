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
- `drop-schema`：退役一个 schema：在同一事务里删除其注册行与 generation 账本、把发布记录置为下一个 epoch 的 `DROPPED`，再分批清除其行（#896）。
- `build-unique-keys`：从 schema 的存活行构建 `x-forma-unique-keys` 声明的唯一键（#651）。须先停止所有写入方。
- `apply-schema`：按 D2 类型规则把修改后的 schema 声明记录为下一个 generation（#733）。
- `baseline-schema`：审计已存储的 PostgreSQL 与 parquet 数据，为尚无 generation 的 schema 记录第一个 generation（升级步骤，#733）。
- `migrate-rfc3339`：把已退役的 `iso8601` 编码存储的整秒图像改写为 `rfc3339` 图像（#842）。须先停止所有写入方。
- `cdc-flush`：把未刷新的 `change_log` 条目导出为 delta parquet，按 Protocol V2 发布不可变的 manifest 代际，
  并在推进该 schema 发布记录的同一事务里标记这些条目（`docs/publication-protocol-v2.md`）。见下文 `cdc-flush 与 cdc-init`。
- `cdc-init`：把 schema 的存活行全量导出为 base parquet，发布 `init` 代际，再按导出版本移交其未刷新条目；从不清空 `change_log`。
- `compactor`：按 Protocol V2 把已发布代际的 base/delta parquet 合并为新的 base 对象并发布 `compact` 代际（保留每行最新版本含墓碑；在并发 `cdc-flush` 之上自动 rebase；从不在线删除被替代的旧对象，旧对象由 `manifest-reconcile --gc` 在宽限期后回收，#890）。见下文 `compactor`。
- `manifest-reconcile`：按 Protocol V2 只读诊断每个 schema 的发布记录、代际历史与 manifest，
  报告丢失的代际与对象（含 schema、epoch、generation 与安全的处置方式）；`--gc` 按回收顺序
  回收过期代际与水位线之下的孤儿对象（#891）。`--gc` 只在部署绑定的存储目标（endpoint、bucket、
  data prefix）下删除。`cdc-flush`、`cdc-init` 与 `compactor` 在暂存前核对该绑定、已绑定到其他目标时拒绝暂存与发布，
  并在首次发布的发布事务内绑定未绑定的部署，绑定与该代一同提交，发布失败则不留下绑定（#911）；写入方与收集器的 S3 客户端都不跟随 HTTP 重定向。
  `--bind-storage` 可选，用于提前绑定或核对收集器的配置；在 Aurora DSQL 上它也会创建绑定表（`init-db` 尚不能在 DSQL 上运行）。
  修改绑定不会移动任何对象：先停止所有写入方与 `--gc`，迁移需另行验证，见 `docs/manifest-reconcile.md`。
  丢失的代际或对象无法从 S3 重建，恢复方式是 `cdc-init` 重新初始化。
  详见 `docs/manifest-reconcile.md`。退出码：0 一致 / 2 有发现 / 1 失败。
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

`init-db` 在注册事务里为每个已注册 schema 写 Protocol V2 发布记录（`docs/publication-protocol-v2.md`）：已有记录保留；
`DROPPED` 记录在 id 重新注册时激活到下一个 epoch（旧 schema 的行须已清除）；既没有记录也没有发布历史的 schema 在 schema 锁下
校验 `UNPUBLISHED` 不变式（`change_log` 中没有已刷新的条目，`entity_main` 中每行都有条目），成立则写 `UNPUBLISHED`。
不变式不成立说明这些行不是 Protocol V2 写入方留下的：`UNPUBLISHED` 记录会让读取方只读热层而漏掉 parquet 层可能持有的行，
所以 `init-db` 失败并回滚本次运行，不迁移这种状态；把发布表与其所属的数据表一同恢复，或清除该 schema 的行后重新注册。
以下两种情况同样区别对待（#896）：
- 本次新注册的 id 已有非 `DROPPED` 的记录：说明之前的注册没有经 `drop-schema` 退役就被删除了。新 schema 会继承旧 epoch、
  旧的已发布 generation 和旧行，所以 `init-db` 失败并回滚这次注册。先运行 `drop-schema --schema-id <id>`，再运行 `init-db`。
- 没有记录、但发布表里仍有该 id 的生命周期 epoch 或 generation 历史：说明记录丢失了，而不是新注册。`init-db` 在 schema 锁下把记录
  恢复到历史中最大 epoch 的下一个 epoch，以最大 generation 号作为计数器，因此不会重用任何 epoch 或 generation 号：仍注册的 schema
  在仍有 generation 历史时恢复为 `AWAITING_INIT`（随后运行 `cdc-init`，再重启读取方），没有 generation 历史时按上述不变式
  恢复为 `UNPUBLISHED`；本次新注册的 id 先恢复为 `DROPPED` 再按上条激活。

示例：
- `go run ./cmd/tools init-db`
- `go run ./cmd/tools init-db -db-host 127.0.0.1 -db-name forma_dev -entity-main-table entity_main_dev -eav-table eav_dev`

drop-schema
-----------
退役一个 schema（`docs/publication-protocol-v2.md` 的 Drop 转换）。先停止所有加载过该 schema 的服务端、`cdc-flush` 与 `cdc-init`。
选项：数据库与表名选项同 `init-db`；`-schema-name` 或 `-schema-id` 二选一（注册行已被删除时用 `-schema-id`）；
`-purge-batch`（默认 5000）为每条清除语句删除的行数。

1. 一个事务内：取 schema 锁（`cdc-flush`/`cdc-init` 持有时拒绝），删除注册行与 generation 账本，`Reset` 发布记录到下一个 epoch 的 `DROPPED`。
   此后读取方对该 schema 失败关闭，写入方的比较交换全部落空。
2. 分批清除该 schema 在 entity main、EAV 与 `change_log` 中的行。每批先在事务里锁住仍为 `DROPPED` 的记录，所以 id 一旦重新注册，
   清除即停止，不会删除新 schema 的行。中断后重新运行同一命令即可继续清除。

完成后，`init-db` 以同一 id 重新注册时把记录激活到再下一个 epoch。旧 epoch 的 parquet 对象留待垃圾回收（#891）。
不要只删除 `schema_registry` 行：发布记录会留下，下一次 `init-db` 拒绝以该 id 重新注册。

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
基线是离线的：运行前须停止旧版本中所有写入的服务端、Lambda 与嵌入方，以及所有 `cdc-flush`、`cdc-init` 运行，并暂停 compactor 与 `manifest-reconcile --gc`，直到新版本取代它们为止。旧版本没有启动门禁，它在基线期间或之后写入的值不会被审计。记录基线的事务会再次读取每个
schema 的 `entity_main` 行数与版本号之和以及存活的 parquet 对象集合，与该 schema 审计开始时不同则拒绝且不记录任何内容；这只能发现审计期间仍在运行的
写入方或导出方，发现不了基线提交后才写入的进程。`-dry-run` 可在旧版本运行时执行，用于停机前找出须修正的数据。

选项（数据库与表名选项及其环境变量默认值与 `validate-schema-consistency` 相同）：
- `-schema-dir`（`SCHEMA_DIR`，必填）：部署当前使用的 JSON Schema 目录。
- `-schema`：只处理这个已注册的 schema。
- `--no-parquet-tier`：部署没有 lakehouse 层，只审计 PostgreSQL。否则必须给出 `--s3-bucket`（与服务端的 `DUCKDB_S3_BUCKET` 一致），
  可选 S3 与 DuckDB 选项。审计的对象集合即联邦读扫描的集合：每个 schema 的发布记录为 `PUBLISHED` 时，其当前 generation 的 manifest
  列出的对象（`docs/publication-protocol-v2.md`）；没有记录或记录为 `UNPUBLISHED`、`AWAITING_INIT`、`DROPPED` 的 schema 没有 parquet 对象可审计
  （`AWAITING_INIT` 的 `cdc-init` 从 PostgreSQL 导出，而 PostgreSQL 已被审计）。记录或 manifest 无法读取或无效时拒绝基线。
- `-dry-run`：只报告审计结果，不写入。

示例：
- `go run ./cmd/tools baseline-schema -schema-dir schemas --no-parquet-tier`
- `go run ./cmd/tools baseline-schema -schema-dir schemas --s3-bucket my-bucket`

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

cdc-flush 与 cdc-init
---------------------
两者都只经发布记录发布（#896）：对象先写到本地临时文件，再以条件写（`If-None-Match: *`）写入从未用过的键，
manifest 代际同样只写一次，然后在一个 PostgreSQL 事务里比较并推进记录（flush 的标记也在该事务内）。
schema 没有发布记录时两者都拒绝（记录由 `init-db` 在注册或恢复丢失的记录时写入）；记录为 `AWAITING_INIT` 时
`cdc-flush` 拒绝并提示先运行 `cdc-init`；`DROPPED` 时两者都拒绝。

共同选项：
- `-s3-bucket`（必填）、`-s3-endpoint`、`-s3-region`、`-s3-use-ssl`、`-s3-use-path`。
- `-s3-prefix`（默认 `data`）：V2 对象布局的数据前缀。parquet 写在 `{prefix}/{schema_id}/e{epoch}/{base|delta}/`，
  manifest 写在 `{prefix}/{schema_id}/_manifests/`。同一部署的 `cdc-flush` 与 `cdc-init` 必须使用同一前缀：
  flush 代际列出其前一个 init 的 base 文件，列出其他前缀下键的 manifest 在写入前就被拒绝。
- `-change-log-table`（默认 `change_log`）：flush 在其中选取并标记未刷新条目，init 在其中移交导出行的条目。
  必须是 `init-db` 建的表（`init-db` 的 `-change-log-table`，默认 `change_log_dev`），同一部署的两者必须一致。
- `-pg-*`、`-pg-engine`（`postgresql` 或 `aurora-dsql`）、`-entity-main-table`、`-eav-table`、`-dry-run`
  （不导出、不写对象、不改记录、不标记）。

`cdc-flush` 选项：`-min-records`、`-max-age-ms`、`-max-batch-bytes`、`-estimated-row-bytes`、
`-batch-size`（默认 10000；PostgreSQL 上整批在一条语句里标记，最多 32766，超出时在解析参数时拒绝；Aurora DSQL
上一批按最多 1000 条一代分批发布）、`-pg-use-iam`、`-schema-registry-table`/`-schema-dir`。

`cdc-init` 选项：`-schema-registry-table`/`-schema-dir`（必填）、`-schema-id`、`-batch-size`、`-target-file-size-mb`、
`-max-batch-size`、`-estimated-row-bytes`。每个 base 文件是一个导出快照，行数由这些选项决定。

发布 `COMMIT` 无应答且写入方无法判定结果时，工具会用新的上下文再结算一次（`Settle` / `ResumeInit`）。仍无法判定时，
错误给出所暂存的代际、token 与 manifest 键，并说明遗留状态：flush 的标记随其发布一起提交，条目按 `change_log`
所示保持已刷新或未刷新；init 的移交未运行，其导出条目保持未刷新（安全，下一次 flush 会重新导出）。

compactor
---------
按 Protocol V2 合并单个 schema 的 parquet 层并发布 `compact` 代际（#890）：读取该 schema 当前 `PUBLISHED` 代际的 manifest，
默认先按 manifest 中的 SHA-256 校验全部输入源对象（#347），通过 DuckDB 合并每行最新版本（含墓碑），将新 base 对象与新
manifest 仅写一次到 S3，并在 PostgreSQL 发布记录上以 CAS 提交。并发 `cdc-flush` 推进了头代际时自动 rebase 保留新 delta，
与并发 `cdc-init` 或其他 `compactor` 竞争时在 `--max-retries` 预算内重新合并。`compactor` 从不在线删除被替代的旧对象，
旧代际与对象由 `manifest-reconcile --gc` 过宽限期后回收（`docs/manifest-reconcile.md`）。

主要选项：
- `-schema-id`（必填）、`-s3-bucket`（必填）、`-s3-endpoint`、`-s3-region`、`-s3-use-ssl`、`-s3-use-path`。
- `-data-prefix`（默认 `data`）：V2 对象布局前缀，须与 `cdc-flush`/`cdc-init` 的 `-s3-prefix` 一致。
- `-pg-host`、`-pg-port`、`-pg-user`、`-pg-password`、`-pg-db`、`-pg-ssl-mode`、`-pg-use-iam`、`-pg-engine`（`postgresql` 或 `aurora-dsql`）、`-entity-main-table`（默认 `entity_main`）。
- `-target-base-size-mb`（默认 256）、`-max-delta-size-mb`（默认 50）、`-dirty-ratio-pct`（默认 5）、`-max-retries`（默认 5）。
- `-skip-input-checksum-verify`（默认 `false`）：跳过合并前的源对象 SHA-256 校验，仅在已知损坏对象已完成排查时使用。
