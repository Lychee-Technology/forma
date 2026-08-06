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
`changed_at >=` 该版本时才算被覆盖（反连接取 `>=`，同戳已列版本计为覆盖；#274 之后
同戳活/活平局是值相同副本、胜者不指定）——因此「同
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
无法验证即不放行。无法解析的**非 base 条目**同样被保守地剔出遮蔽集（读不到的文件绝不能算作
提供覆盖）——这只会让证明更难通过，不会放宽它；而且自时间栅栏起，这类条目本身也会拒绝整组晋升
（见下文方向二：定不了年就不放行）。

版本反连接之前还有一道**双向对象时间栅栏**：被驱逐的 base 条目必须能在本轮列举中定年，且对象
时间**严格早于**晋升集；**幸存的非 base 条目**同样要过栅栏——要么对象时间严格早于晋升集，要么
版本区间严格高于晋升集。它补上的正是反连接看不见的那一面（同戳不同值），见下文
《同毫秒平局与时间栅栏》。

保守拒绝是可接受的代价——兜底恢复手段始终是重跑 cdc-init 从 `entity_main` 全量重建 base 层。

### 并发写与快照说明

覆盖恒等式的两侧（`LiveRowCount` 与逐文件 `MissingLiveRows`）**不在同一个 PG 快照上取值，
也不需要**：任何并发插入 / 删除 / 复活让两侧偏斜的结果都是**不等**，也就是拒绝（保守方向）。
唯一能"意外通过"的形态是快照之后新插入的行——它们既不在 parquet 里也不在计数里，由
`change_log` → 热层遮蔽，与 cdc-init 自身分批导出（`LIMIT`/`OFFSET`，同样不是单一快照）
所依赖的一致性契约**完全相同**。普通实体写入不持这把 advisory lock，本工具也不假设存在写屏障。

### 同毫秒平局与时间栅栏

全系统以 `changed_at`（毫秒）为版本；同戳视为同版本。#274 之后两个导出器都把活行编码为
`deleted_at = 0`，读路径的同戳活/活冷层平局**没有任何判别键**——胜者不指定，任意一侧副本
都可能被服务。这之所以安全，是因为写侧同时保证了**行内版本严格递增**（#274：更新以
`GREATEST($now, ltbase_updated_at + 1)` 计算有效版本、RETURNING 回填 change_log 同事务同值；
删除的墓碑戳严格晚于被删行的版本）——同一行的两次串行写入即使落在同一毫秒、即使跨越失败
init 的快照边界，也不可能同戳，同戳活/活副本必然值相同。因此残余危险收窄为**排序修复之前
写入的存量对象**仍可能携带值分歧的同戳副本：晋升守卫对两个方向各设一道栅栏，把这类
旧时代（或无法核验的）平局挡在发布之外。

以下两个方向描述的分歧同戳副本都只能由**排序修复之前**的写入产生（修复后的写入落在 T+1，
根本不会同戳），但这类存量对象在被 compaction 归并前持续在册，栅栏因此保留：

**方向一：被驱逐的 base 条目。** 驱逐证明的反连接谓词是 `changed_at >= changed_at`，即**同戳
算作被覆盖**，而反连接看不见值。某行在失败 init 的快照读**之后**、同一毫秒内被写入一个**不同的
值**（pre-#274 写入），该写入的 delta 又已被 compaction 折进某个在册 base 条目，此时同戳"覆盖"
成立，整体替换后读路径可能把可见值回退到 init 快照那一侧（胜者不指定，且可能随扫描翻覆）。

**方向二：幸存的非 base 条目。** 即使 manifest 里**根本没有 base 条目**（方向一无事可查、覆盖
恒等式也照常成立），危险依旧存在：某个在册 delta 携带 `R=new@T`（pre-#274 的快照后同毫秒写入，
在失败的 init 释放锁之后才 flush），而 init 集里是 `R=old@T`。晋升把 `old` 发布成 base，读路径的
同戳活/活平局胜者不指定，读结果可能回退到 `old`（且逐次扫描可能翻覆），任何探针都看不见。
（幸存**墓碑** `R@T` 不在此风险内：`deleted_ts DESC` 让墓碑 `T > 0` 稳赢被晋升的活版本 `0`，
删除保持生效——危险仅限值分歧的活副本。）

改用严格 `>` **不可行**：跨代际未变更的行本就同戳，严格不等式会让任何非空 base 层都无法晋升。
两个方向因此都改由**对象时间栅栏**关闭：

1. **被驱逐的 base 条目**必须**严格早于**晋升集（`postdates the init export`，取 `>=` 即拒绝）。
   cdc-init 全程持 per-schema advisory lock，而 flusher 也需要这把锁，因此任何"init 快照读之后
   的写入"所在的 delta 只可能在失败的 init **释放锁之后**才落盘——晚于该 init 写下的每一个对象。
   折进了这种 delta 的 base 条目，其对象 `LastModified` 必然更晚。对**严格更早**的条目，
   `>=` 覆盖语义是**可靠**的：这种对象只可能含锁前的写入，而对锁前写入而言，同一 row_id 上的
   同戳就意味着**同一次 PG 写**（`changed_at` 即该行的 `ltbase_updated_at` 印章，同印章即同一次
   盖章）⇒ **同值**，驱逐它不丢任何东西。
2. **幸存的非 base 条目**（注意：是**全部**在册非 base 条目，不只是参与遮蔽探测的那些——探测不到
   的条目 splice 之后照样在册、照样参与读路径平局）必须满足以下**任一**条件，否则拒绝：
   - **对象时间严格早于晋升集**：同上，锁前内容，同戳即同值，平局无害；
   - **版本区间严格高于晋升集**：条目的 `created_min` > 晋升集各文件 `created_max` 的最大值
     （且 > 0）。严格更高的区间与任何 init 行都平不了局，该条目在 LWW 里是正当胜出。这正是
     "丢失删除两轮收敛"的形态——被修复补录回来的墓碑 delta 必然晚于失败的 init，若采用"一律
     拒绝晚于 init 的幸存条目"的粗暴规则，该序列将永远无法收敛。

     该分支信任幸存条目自身的 `created_min` 元数据。这是针对**系统写入者时序**的安全论证，
     不是防篡改论证；元数据与字节不符的检测是 `--verify-stamps` 的职责。

栅栏跑在版本反连接**之前**（纯算术，更便宜）。**无法定年**的条目一律拒绝，而不是放行去做反连接：
本轮列举中查不到的在册 base 条目（例如并发 compaction 在本轮列举之后才提交，
`absent from this run's object listing`）；以及**无法定年的幸存非 base 条目**——路径带 glob、
跨 bucket，或 key 不在本轮列举里（`cannot be dated against the init export`）。注意这是一次
**收紧**：这类不可解析的非 base 条目此前只是被保守剔出遮蔽集、并不阻塞晋升，现在直接拒绝整组晋升。

**秒粒度残留已闭合**：S3 `LastModified` 为秒级粒度，因此两道栅栏都要求**严格早于**而非"不晚于"
——与最后一个 init 对象同一秒内完成的 flush+compaction 不再有可乘之机。代价是秒边界上的**保守
假拒绝**（对象恰好落在同一秒的合法条目会被拒），兜底手段一如既往：重跑 cdc-init。

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
  运行中重新经过晋升守卫，但**时间栅栏保证该重评估必然是拒绝**——旧代际的对象时间早于已在册的
  新 base 条目，而栅栏要求被驱逐条目严格早于晋升集，故更老的一代永远无法驱逐更新的 base。
  代际乒乓（新旧两代互相晋升、互相驱逐）因此不可能发生，这些旧文件的归宿是确定的：落回
  `--gc` 两阶段删除候选。

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

## `--verify-stamps`：#256 戳记的离线字节真值核查

#256 给每个 manifest 条目盖上 parquet footer 的列集印章（`FileEntry.columns`，列名 →
DuckDB 类型）。读路径信任通过系统列不变量的印章并**跳过 footer 探测**——这正是 #256 的
优化。代价是一条信任边界：若某个已列出对象的**字节**被带外改写，而条目上的印章依旧
"形状合法"，读路径看不见这次改写。`--verify-stamps` 就是这条边界的离线字节真值核查。

**比对什么。** 逐个已列出条目 `DESCRIBE` 其真实对象，把 footer 的**完整列名 → 类型映射**
与条目印章做全量相等比较。这比读路径的 scan guard 严格得多：scan guard 只覆盖
`row_id`/`changed_at` 的存在性与 `changed_at`/`deleted_at` 的类型，而这里任何一列的
增删改型都会被发现。

**能抓到什么。** 典型的三类带外改写：

- **缺列**：对象不再携带 `deleted_at`。读侧 `union_by_name` 会用兄弟对象的 schema 把它
  NULL 填充，而 #274 之前的存量 delta 对象本来就把活行的 `deleted_at` 写成 NULL、且在被
  compaction 归并前持续可读，两者在扫描层无法区分，所以这条通道**只有**本核查看得见
  （扫描层 presence guard 的扩展以存量对象退役为门，见 #365）；
- **改型**：`row_id` 从 UUID 变成 VARCHAR。`row_id` 的守卫刻意不带类型（#147），大小写不同
  的同一 UUID 拼写会被当作两行分区；
- **属性列漂移**：印章少报了对象实际携带的属性列，导致列并集偏短、NULL 别名与真实列相撞。

**跳过规则。** 三类条目不比对，也不因此报差异：

- 无印章的**历史条目**（`columns` 为空）：#256 不做回填，读侧对它们照常懒探测，没有可比对象；
- **未被证明存在的 dangling 候选**：dangling 复核本就逐个探测候选对象的存在性，因此候选分成三种
  归宿——**已确认 dangling**（对象确实没了，背后没有字节可探，差异本身已在报告里）、**并发被
  splice 出去**（复核根本没探，字节是否可达未知，探它可能撞上被并发删除的对象而误升级成 exit 1）、
  以及**探到了、对象活着**（陈旧列举导致的假 dangling）。前两种跳过，**第三种照常比对**：这轮
  已经证明它的字节可达，再跳过就是白白丢覆盖率；
- **无法解析的路径**（unverifiable，含 glob）：同样已在报告里。

**退出语义。** 发现分歧 → 计入残余差异 → 退出码 **2**（报告里逐条打印
`stamp divergence: <key>: column "x" stamp "A" vs footer "B"`，缺失的一侧渲染为
`(absent)`）；探测本身失败（S3 不可达、DuckDB 出错）→ 与 dangling 复核同一条规则，
算**工具故障**退出 **1**，绝不伪装成"字节不一致"的判决。

**与 `--repair` 的顺序耦合。** 验证跑在晋升/修复**之前**（先验证后变更），因此
`--verify-stamps` 的探测失败会在晋升/修复之前让该 schema 以 exit 1 终止——与 `--repair`
同跑时，一次与待修复文件无关的探测失败也会挡下本轮晋升，修复故障源后重跑即可。

**处置。** 分歧不是自动可修的：工具不会拿 footer 去覆写印章，因为无法判断是印章错了
还是字节被换了。运维应先查这个 key 的改写来源；确认对象本身正确后，用一次重写
（compaction / 重跑 cdc-init）把条目连同印章一并重新发布。

**运维建议。** 这是一次全量 footer 探测（每个已列出条目一次 `DESCRIBE`），比只读巡检
贵得多，因此默认关闭。建议低频周期运行（例如每日一次），以及在**怀疑发生过带外改写**
之后（手工改过桶、做过跨桶/跨环境恢复、外部作业写过数据前缀）立即跑一次。

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

# #256 印章的离线字节真值核查（全量 footer 探测，低频运行）
forma-tools manifest-reconcile ... --verify-stamps
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
  记录目击但尚未过宽限期的残留、`--verify-stamps` 发现的印章分歧）；`1` 工具自身失败
  （含任一 schema 的 list/load/lock/dangling 复核/GC 状态读取/印章 footer 探测
  失败——不会伪装成「有差异」）。

## init 形态晋升（#292）

cdc-init 的 manifest 发布失败（导出成功但 `ReplaceTierFiles` 失败）留下的 init 形态孤儿
现在有两条恢复路径：

- `--repair`：经上述三重证明（覆盖验证 / 复活守卫 / 驱逐安全）后整组晋升进 manifest
  base 层，等价于补上那次失败的发布，数据无需重新导出；
- 证明不通过或未开 `--repair`：这组文件保持 GC 候选，由 `--gc` 两阶段删除（#290），
  删除后重跑 cdc-init 从 `entity_main` 重新导出。

两条路径都依赖 #290 起 cdc-init 与 reconcile 共持的那把 per-schema advisory lock：此锁下
出现的 init 形态孤儿必非 in-flight init。
