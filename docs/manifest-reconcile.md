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
| delta 孤儿 | `{prefix}/{schemaID}/{uuid}.parquet` | #197 或 #188 删除失败的 merged source | `--repair` 经守卫判定后补录或转残留（见下） |
| merged base 孤儿 | `base-{uuid}.parquet` | #188 | `--gc` 两阶段删除（见下） |
| init base 孤儿 | `{min}_{max}.parquet` | cdc-init 导出 | `--gc` 两阶段删除（见下）。#290 起 cdc-init 与 reconcile 持同一把 per-schema advisory lock，故此锁下的 init 形态孤儿必非 in-flight init——只可能是发布失败的 manifest 或被后续 init 覆盖的旧文件（`--repair` 自动重建留 follow-up） |
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

一个 delta 形态孤儿有两种截然相反的身份：#197 孤儿（数据仅存于该文件，必须补录）或
#188 中删除失败的 merged source（数据已并入 merged base，其中墓碑行在合并时被物理丢弃
——**补录会让已删除的行复活**）。工具用两级探测区分：

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

## 用法

```bash
# 只读巡检（可周期运行；退出码 0=一致，2=有差异，1=工具故障）
forma-tools manifest-reconcile \
  --s3-bucket my-bucket --data-prefix data \
  --schema-registry-table schema_registry \
  --pg-host ... --pg-db forma

# 修复 #197 孤儿（经守卫判定）
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
  故此锁下的 init 形态孤儿必非 in-flight init，纳入 `--gc` 候选。compactor 仍不持这把锁，
  故 manifest 写入另有 ETag 条件写保护（不存在的 manifest 用 If-None-Match
  条件创建，绝不覆写并发新建的 manifest），dangling 判定做二次确认。
- `--schema-id` 指向未注册 schema 时直接报错退出 1（而不是空跑报「一致」）。
- `--gc-grace` 必须为正（默认 15m）。
- `--data-prefix` 允许为空（对象 key 形如 `/7/{uuid}.parquet`，与 cdc path builder
  一致，比较时不做任何前导斜杠归一化）。
- 退出码：`0` 一致；`2` 存在残余差异（含跳过的 schema、拒绝自动处理的混杂文件、
  记录目击但尚未过宽限期的残留）；`1` 工具自身失败（含任一 schema 的
  list/load/lock/dangling 复核/GC 状态读取失败——不会伪装成「有差异」）。

## 已知范围限制（follow-up）

cdc-init 的 manifest 发布失败（导出成功但 `ReplaceTierFiles` 失败）留下的 init 形态
孤儿自 #290 起可由 `--gc` 两阶段删除（cdc-init 与 reconcile 持同一把 per-schema
advisory lock，故此锁下的 init 形态孤儿必非 in-flight init）。删除后重跑 cdc-init 即
可从 entity_main 重新导出；把孤儿直接自动补录进 manifest（`--repair` 提升 init 形态）
仍留 follow-up issue——部分导出误提升风险尚未有守卫。
