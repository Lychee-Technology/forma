# manifest-reconcile — S3 对象与 manifest 对账工具（#203）

`forma-tools manifest-reconcile` 逐 schema 比对 S3 上的 parquet 对象与该 schema 的
manifest 条目，双向报告差异，并可选修复。它是两条既有故障路径的官方恢复工具：

- **#197 flush 孤儿**：delta flush 导出成功、行已标记 `flushed_at != 0`，但 manifest
  append 失败。文件在最终 key 上、数据只存在于该文件，重跑 flush 不会补录。
- **#188 compaction 遗留**：rewrite 崩溃留下的 `_tmp/` staging 对象与未列入 manifest 的
  `base-{uuid}.parquet`，以及 manifest 提交后删除失败的 merged source。这些对象的数据
  已并入 merged base，属于纯垃圾。
- **#226 swallowed-delete 残留**：flush / cdc-init / compaction 经 `CopyTmpToFinal`
  提升成功后，对 `_tmp/` staging 对象的 DeleteObject 失败被有意吞掉（提升已成功，
  流程不应失败）。CopyObject 或导出失败时 #226 已在带内当场自愈（best-effort 删除
  自己的 tmp），因此只有这条 post-copy delete 失败路径会留下 `_tmp/` 残留；本工具的
  `--gc` 两阶段删除即其成文回收机制。回收不在单次运行内完成：首次 `--gc` 只记录目击，
  须后续运行在双时钟都超过 grace 后才删除——因此回收有上界的前提是周期性调度 `--gc`
  （以周期 P 调度时上界 ≈ grace + 2P，默认 grace 15 分钟）；不调度则残留无界存留
  （对正确性不可见：不进 manifest、生产 glob 不跨 `/`）。

## 差异分类

按文件名形态把「S3 存在、manifest 未列」的对象分为四类：

| 类别 | 形态 | 来源 | 处置 |
|------|------|------|------|
| delta 孤儿 | `{prefix}/{schemaID}/{uuid}.parquet` | flush 的 manifest-append 失败（#252 起 append 先于 mark-flushed：行留 dirty、重试自愈覆盖，孤儿通常判为可 GC 残留；#252 前为 #197 丢失数据形态）或 #188 删除失败的 merged source | `--repair` 经守卫判定后补录或转残留（见下） |
| merged base 孤儿 | `base-{uuid}.parquet` | #188 | `--gc` 两阶段删除（见下） |
| init base 孤儿 | `{min}_{max}.parquet` | cdc-init 导出 | `--repair` 下先经晋升守卫（三重证明，见下）整组判定：证明通过则整组补录进 manifest base 层；证明不通过（或未开 `--repair`）保持 GC 候选，走 `--gc` 两阶段删除（见下）。#290 起 cdc-init 与 reconcile 持同一把 per-schema advisory lock，故此锁下的 init 形态孤儿必非 in-flight init——只可能是发布失败的 manifest 或被后续 init 覆盖的旧文件 |
| `_tmp` 孤儿 | `{prefix}/{schemaID}/_tmp/*` | staging 残留（#188 崩溃 / #226 swallowed-delete） | `--gc` 两阶段删除（见下） |

形态匹配是严格的：`base-` 后缀与 `{min}_{max}` 两段都必须是合法 UUID，否则归
unknown（只报告，永不删除）。

另报告两类只读发现：

- **dangling**：manifest 条目指向的对象已不存在（经二次确认：重载 manifest 后条目仍在
  且逐 key 探测仍缺席才判定，规避与无锁 compactor 的竞态假阳性）。移除条目会让数据从
  读路径消失，本工具**不**自动删除，保持人工处置。
- **unverifiable**：指向其他 bucket、带 glob、或不在本次 list 前缀下的条目——本次列举
  无法证明其缺席，只做信息展示。

无法识别形态的 `.parquet`（unknown）只报告，永不 repair / GC。

## `--repair` 的安全守卫（防墓碑复活）

一个 delta 形态孤儿有两种截然相反的身份：数据仅存于该文件的丢失形态（#197；#252 把
flush 改为 append 先于 mark 后，新产生的 append-失败孤儿其行仍 dirty、会被重试自愈覆盖，
从而落入下面的残留分支——丢失形态只剩历史遗留与极端交错）或 #188 中删除失败的 merged
source（数据已并入 merged base，其中墓碑行在合并时被物理丢弃——**补录会让已删除的行
复活**）。工具用两级探测区分：

1. **覆盖探测**（DuckDB 反连接）：孤儿中有哪些 row_id 不出现在任何 manifest 已列文件里；
2. **活性探测**（Postgres `entity_main`，`--entity-main-table`）：这些未覆盖行是否仍存活。

覆盖判定是**版本感知**的：孤儿中某行只有当某个已列文件携带同 row_id 且
`changed_at >=` 该版本时才算被覆盖（相等平局按 LWW base-wins，#183）——因此「同
row_id 的更新丢失」（孤儿携带比所有已列文件更新的版本）会被正确判为未覆盖，而不是
被 row_id 级比较误判成可删残留。活性判定对齐 cdc-init 的导出口径：行存在于
entity_main 且 `ltbase_deleted_at IS NULL`。

逐未覆盖行裁决（取该行未覆盖版本中最新者）：

- **活版本 + PG 存活** → #197 丢失数据，须补录；
- **墓碑 + PG 已删除** → 丢失的删除标记：它缺席时更老的已列版本正在读路径上复活，
  补录恢复删除语义；
- **活版本 + PG 已删除** → 复活风险：该行墓碑赢了合并后被物理丢弃，补录会复活它；
- **墓碑 + PG 存活** → 与实体状态矛盾，永不自动处理。

文件级裁决：存在须补录行且无风险/矛盾行 → 补录（元数据从 parquet 内容重算，幂等，
绝不重复已有 Path）；无未覆盖行或仅有复活风险行 → 判定为合并残留（报告列在
`delta leftover`），在 `--repair --gc` 同时开启时按下述两阶段删除；混杂 → 拒绝自动
补录与自动删除，留人工处置（保持非零退出码）。

## `--repair` 的 init 形态晋升守卫（#292）

cdc-init 是一次全量重导出，因此**一组完整的 init 输出本身就是 base 层的规范清单**——正是
发布失败时本该写下的那份 manifest。`--repair` 因此可以把 init 形态孤儿整组补录回 manifest
base 层（`SpliceTierFiles`：整个 base 层被这组条目替换，其他 tier 条目原样保留），但只在
能证明这么做无损时才做。

晋升在 delta 孤儿补录**之前**执行，跑在同一把 per-schema advisory lock 下；manifest 写入
仍走 ETag 乐观并发（compactor 不持这把锁），冲突后重载 manifest 并**重新验证**依赖在册清单
的那两个证明（下面的 2、3），因为重载后的 base / delta 条目可能已经变了。

语义是 **all-or-nothing**：整组晋升，或整组拒绝——不存在只补录其中几个文件。

### 三重证明

**1. 覆盖验证**（证明这组文件确实是一次完整 init 导出，只看 parquet 内容与 Postgres 计数）

- schema 当前**没有存活行** → 拒绝（此时 init 形态孤儿必然已被取代）；
- 逐文件重算 parquet 统计；统计读不到 → 拒绝；
- 文件名 `{min}_{max}` 与重算出的 row_id 区间**不一致** → 拒绝（元数据永不取自文件名，
  该检查只是为了让"看起来像 init 形态"的外来/截断文件进不了 base 层）；
- 任意两文件的 `[min, max]` 区间**重叠** → 拒绝：一次导出的批次互不相交，重叠说明这组
  文件混了多代 init，晋升会把重复行版本塞进 base 层；
- 文件含 **tombstone 行** → 拒绝（cdc-init 只导出存活行，含墓碑说明这不是 init 输出）。
  判定口径是逐 row_id 取**最新版本**（`arg_max`）看它是否为墓碑；真实 cdc-init 输出里每行
  只有一个版本，故与"文件中出现过任何墓碑行"等价；
- **计数恒等式**：Σ 每文件（distinct row_id 数 − 在 PG 中已死的行数）== `entity_main`
  存活行数（`--entity-main-table`，口径 `ltbase_schema_id = ? AND ltbase_deleted_at IS NULL`，
  与 cdc-init 的导出口径、与分子分母同一定义）。区间互不相交保证各文件的 row_id 集合互不
  相交，因此逐文件计数可以直接相加；不相等 → 拒绝（部分导出绝不能替换整个 base 层）。

条目元数据（行数、row_id / created_at 区间、大小）全部从 parquet 内容与对象本身重算。
#256 的列集印章是 best-effort 探测：探不到只是条目不带印章（读侧回落到探测），不阻塞晋升。

**2. 复活守卫**（证明晋升不会让已删除的行复活）

覆盖计数只是"不计入"死行，说明不了它们的去向。可达的故障场景是：某行在 init 导出失败**之后**
被删除，其墓碑 delta 已 flush 并被 compaction 合并丢弃（合并后的 base 不保留墓碑行），
change_log 也已标记 flushed（热层不再遮蔽），而 init 文件里仍留着该行的**旧的活版本**——
覆盖验证会过，驱逐安全也会过，晋升却把删除撤销了。

因此对每个含死行的文件，用**仍在册的非 base 条目（幸存 delta）**做版本反连接：只要某个死行
的活版本没有任何幸存 delta 覆盖 → 拒绝。晋升集自身及其兄弟文件不参与遮蔽（它们正是被审查的
对象，算进去会让检查空洞地通过），被驱逐的 base 条目也不参与（它们即将离场，splice 之后遮蔽
不了任何东西）。没有死行时该守卫零额外查询。

**3. 驱逐安全**（证明整体替换 base 层不丢版本）

整组晋升会驱逐 base 层当前**全部**在册条目。对每个被驱逐的 base 条目做版本反连接，遮蔽集 =
晋升集 ∪ 幸存 delta 条目；只要存在未被覆盖的行版本 → 拒绝。这挡住的是 compaction 竞态：
init 发布失败后 compaction 又合并出的 `base-{uuid}` 可能携带比孤儿集**更新**的版本，整体
替换会让这些版本变成孤儿、读路径回退到旧值（它们的 change_log 已标记 flushed，热层不会遮蔽）。
**被驱逐的 base 条目**路径无法在本 bucket 解析（跨 bucket / 带 glob）→ 直接拒绝整体替换：
无法验证即不放行。无法解析的**非 base 条目**不构成拒绝，而是被保守地剔出遮蔽集（读不到的
文件绝不能算作提供覆盖）——这只会让证明更难通过，不会放宽它。

保守拒绝是可接受的代价——兜底恢复手段始终是重跑 cdc-init 从 `entity_main` 全量重建 base 层。

### 并发写与快照说明

覆盖恒等式的两侧（`LiveRowCount` 与逐文件 `MissingLiveRows`）**不在同一个 PG 快照上取值，
也不需要**：任何并发插入 / 删除 / 复活让两侧偏斜的结果都是**不等**，也就是拒绝（保守方向）。
唯一能"意外通过"的形态是快照之后新插入的行——它们既不在 parquet 里也不在计数里，由
`change_log` → 热层遮蔽，与 cdc-init 自身分批导出（`LIMIT`/`OFFSET`，同样不是单一快照）
所依赖的一致性契约**完全相同**。普通实体写入不持这把 advisory lock，本工具也不假设存在写屏障。

### 同毫秒平局边界

全系统以 `changed_at`（毫秒）为版本；同戳视为同版本，平局走确定性的 **base-wins**
（#183，经 `deleted_at` 0-vs-NULL 排序）。晋升的驱逐证明沿用同样的 `>=` 覆盖语义，因此存在
一个极窄的**已知边界**：同一行在同一毫秒内被写入两个不同值、且恰好跨越失败 init 的快照读、
且较新值所在的 delta 已被 compaction 消费时，晋升会把可见值确定性地回退到 init 快照那一侧。
这是毫秒级版本粒度的固有语义，并非本工具引入；如需平局值等价校验请另开 follow-up。

### 拒绝与晋升的表现

- **拒绝**：报表打印一行 `init promotion refused: <reason>`（all-or-nothing，一条原因覆盖
  整组）；这组文件保持普通 GC 候选，`--repair --gc` 同跑时按两阶段删除处理。退出码取决于
  这些文件**本轮是否被删除**：未删除（只读、只 `--repair`、或目击尚未满 grace）时它们仍是
  残余差异 → 退出 `2`；`--repair --gc` 同跑且本轮真的删除了它们时，它们已被计为"已处置"，
  在无其他差异的情况下退出 `0`——但报表**照常打印那行 `init promotion refused:`**，不要把
  退出 0 读作"没发生过拒绝"。
- **晋升**：报表逐 key 打印 `promoted base-init`，这些文件成为 base 层在册条目，本轮不再是
  GC 候选（其目击状态也会被剪除，日后若再次未列出则宽限时钟重新起算）。被替换掉的**旧 base
  条目对应的对象**随即变成未列出对象；晋升本身不删除任何对象。这些对象在后续运行中按形态处理：
  `base-{uuid}` 形态直接进 `--gc` 两阶段删除候选；init 形态的旧文件会在下一次 `--repair`
  运行中**重新经过晋升守卫**（本文不承诺重评估必然拒绝），只有该轮未晋升它们时才落回
  `--gc` 候选。

### 运维注意事项：探测失败也表现为"拒绝"

parquet 统计读不到、DuckDB / S3 访问失败、Postgres 查询出错，这些**探测类故障**不会让工具
退出 1，而是与"证明不成立"一样表现为一次拒绝（原因文本里带着底层错误）。后果是：`--repair --gc`
同跑时，这组 init 文件仍会照常进入 GC 的两阶段流程（本次只记录目击；若故障持续到目击满
grace，后续运行就会删除它们）。

因此看到 `init promotion refused:` 且原因属于探测失败（`unreadable parquet stats` /
`live-row count unavailable` / `enumerate rows of` / `check live rows of` /
`cannot prove ...` 之类）时，**先修复故障源、重跑确认拒绝原因
是真的"无法证明完整"而不是"暂时探不到"，再开 `--gc`**。删除本身仍然是安全的（重跑 cdc-init
即可重建），但会白白丢掉一次可以零成本晋升的机会。

## `--gc` 的两阶段删除（unlisted 时长语义）

#188 follow-up 要求「对象解除列出超过最大查询时长后才删除」。对象的 LastModified
无法表达该时长（一个很老的 source 可能刚刚被 compactor splice 出 manifest），因此
工具在 manifest 旁持久化目击状态（`<manifest path>.gc-state`，同 ETag 乐观并发）：

1. 第一次观察到某候选对象未被列出 → 只记录 first-unlisted 时间戳，不删除；
2. 后续运行中，当「已观察的未列出时长」与「对象年龄」**都**超过 `--gc-grace` 时才
   删除；重新回到 manifest 的 key 会从状态中剪除，宽限时钟重新起算。

状态丢失/写入失败只会让下次运行重新记录（推迟删除，绝不提前）；状态读取失败拒绝
删除并按工具故障退出。

运维口径：单次手工运行对新候选只记录目击、不删除；只有周期性调度 `--gc` 才有回收
上界——泄漏发生后最迟一个周期内被目击，目击满 grace 后的下一次运行删除，故以周期
P 调度时上界 ≈ grace + 2P。
混用"带 --gc 与不带 --gc"的运行方式时，早先运行留下的目击记录可能在文件短暂重新列入清单期间未被清理，建议保持一致的 --repair --gc 运行方式以让目击清理正常工作。

## 用法

```bash
# 只读巡检（可周期运行；退出码 0=一致，2=有差异，1=工具故障）
forma-tools manifest-reconcile \
  --s3-bucket my-bucket --data-prefix data \
  --schema-registry-table schema_registry \
  --pg-host ... --pg-db forma

# 修复 #197 孤儿（经守卫判定），并晋升可证明完整的 init 形态孤儿集（#292）
forma-tools manifest-reconcile ... --repair

# 清理 #188 遗留：merged base / _tmp / 已判定的 delta 残留
forma-tools manifest-reconcile ... --repair --gc --gc-grace 15m
```

要点：

- **`--data-prefix` 必须与 flusher/compactor 的数据前缀一致**（cdc-flush 的
  `--s3-prefix`、compactor 的 `--data-prefix`）。前缀不一致时列举落空，所有 manifest
  条目会被误报为 unverifiable/dangling、所有对象被误报为孤儿。
- 工具需要 Postgres：逐 schema 取与 flusher/cdc-init 同款 advisory lock
  （`pg_try_advisory_lock(schemaID, schemaID)`，钉在单一连接上），消除与 flusher 的
  list/load 竞态；拿不到锁的 schema 跳过并计入差异退出码。#290 起 cdc-init 也持这把锁，
  故此锁下的 init 形态孤儿必非 in-flight init：`--repair` 下先经晋升守卫判定（#292），
  未晋升者纳入 `--gc` 候选。compactor 仍不持这把锁，
  故 manifest 写入另有 ETag 条件写保护（不存在的 manifest 用 If-None-Match
  条件创建，绝不覆写并发新建的 manifest），dangling 判定做二次确认。
- `--schema-id` 指向未注册 schema 时直接报错退出 1（而不是空跑报「一致」）。
- `--gc-grace` 必须为正（默认 15m）。
- `--data-prefix` 允许为空（对象 key 形如 `/7/{uuid}.parquet`，与 cdc path builder
  一致，比较时不做任何前导斜杠归一化）。
- 退出码：`0` 一致；`2` 存在残余差异（含跳过的 schema、拒绝自动处理的混杂文件、
  记录目击但尚未过宽限期的残留）；`1` 工具自身失败（含任一 schema 的
  list/load/lock/dangling 复核/GC 状态读取失败——不会伪装成「有差异」）。

## init 形态晋升（#292）

cdc-init 的 manifest 发布失败（导出成功但 `ReplaceTierFiles` 失败）留下的 init 形态孤儿
现在有两条恢复路径：

- `--repair`：经上述三重证明（覆盖验证 / 复活守卫 / 驱逐安全）后整组晋升进 manifest
  base 层，等价于补上那次失败的发布，数据无需重新导出；
- 证明不通过或未开 `--repair`：这组文件保持 GC 候选，由 `--gc` 两阶段删除（#290），
  删除后重跑 cdc-init 从 `entity_main` 重新导出。

两条路径都依赖 #290 起 cdc-init 与 reconcile 共持的那把 per-schema advisory lock：此锁下
出现的 init 形态孤儿必非 in-flight init。
