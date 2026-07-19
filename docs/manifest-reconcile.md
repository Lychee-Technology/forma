# manifest-reconcile — S3 对象与 manifest 对账工具（#203）

`forma-tools manifest-reconcile` 逐 schema 比对 S3 上的 parquet 对象与该 schema 的
manifest 条目，双向报告差异，并可选修复。它是两条既有故障路径的官方恢复工具：

- **#197 flush 孤儿**：delta flush 导出成功、行已标记 `flushed_at != 0`，但 manifest
  append 失败。文件在最终 key 上、数据只存在于该文件，重跑 flush 不会补录。
- **#188 compaction 遗留**：rewrite 崩溃留下的 `_tmp/` staging 对象与未列入 manifest 的
  `base-{uuid}.parquet`，以及 manifest 提交后删除失败的 merged source。这些对象的数据
  已并入 merged base，属于纯垃圾。

## 差异分类

按文件名形态把「S3 存在、manifest 未列」的对象分为四类：

| 类别 | 形态 | 来源 | 处置 |
|------|------|------|------|
| delta 孤儿 | `{prefix}/{schemaID}/{uuid}.parquet` | #197 或 #188 删除失败的 merged source | `--repair` 经守卫判定后补录或转残留（见下） |
| merged base 孤儿 | `base-{uuid}.parquet` | #188 | `--gc` 删除（宽限期后） |
| init base 孤儿 | `{min}_{max}.parquet` | cdc-init 导出 | 只报告，**永不自动删除**：in-flight cdc-init 先落对象、跑完才发布 manifest，且不持 advisory lock |
| `_tmp` 孤儿 | `{prefix}/{schemaID}/_tmp/*` | staging 残留 | `--gc` 删除（宽限期后） |

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

裁决：

- 存在未覆盖行且**全部存活** → 真 #197 孤儿，补录（元数据从 parquet 内容重算，幂等，
  绝不重复已有 Path）；
- 无未覆盖行，或未覆盖行**全部已删除**（其墓碑赢了合并后被丢弃）→ 判定为合并残留
  （报告列在 `delta leftover`），在 `--repair --gc` 同时开启且过宽限期时删除；
- 未覆盖行**存活/已删混杂** → 拒绝自动补录与自动删除，留人工处置（保持非零退出码）。

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
- 工具需要 Postgres：逐 schema 取与 flusher 同款 advisory lock
  （`pg_try_advisory_lock(schemaID, schemaID)`，钉在单一连接上），消除与 flusher 的
  list/load 竞态；拿不到锁的 schema 跳过并计入差异退出码。compactor 与 cdc-init 不持
  这把锁，故 manifest 写入另有 ETag 条件写保护（不存在的 manifest 用 If-None-Match
  条件创建，绝不覆写并发新建的 manifest），dangling 判定做二次确认，init 形态对象
  排除在 GC 之外。
- `--schema-id` 指向未注册 schema 时直接报错退出 1（而不是空跑报「一致」）。
- `--gc-grace` 必须为正（默认 15m），兜住 in-flight reader 竞态：持有 splice 前对象
  清单的查询在 manifest 提交后约一个查询时长内仍可能引用已解除列出的 key。残余窗口
  （对象很老、恰在 GC 前一刻才被 splice）在实践中可忽略。
- 退出码：`0` 一致；`2` 存在残余差异（含跳过的 schema、拒绝自动处理的混杂文件）；
  `1` 工具自身失败（含任一 schema 的 list/load/lock 失败——不会伪装成「有差异」）。
