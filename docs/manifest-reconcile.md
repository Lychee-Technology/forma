# manifest-reconcile — S3 对象与 manifest 对账工具（#203）

`forma-tools manifest-reconcile` 逐 schema 比对 S3 上的 parquet 对象与该 schema 的
manifest 条目，双向报告差异，并可选修复。它是两条既有故障路径的官方恢复工具：

- **#197 flush 孤儿**：delta flush 导出成功、行已标记 `flushed_at != 0`，但 manifest
  append 失败。文件在最终 key 上、数据只存在于该文件，重跑 flush 不会补录。
- **#188 compaction 遗留**：rewrite 崩溃留下的 `_tmp/` staging 对象与未列入 manifest 的
  `base-{uuid}.parquet`，以及 manifest 提交后删除失败的 merged source。这些对象的数据
  已并入 merged base，属于纯垃圾。

## 差异分类

按文件名形态把「S3 存在、manifest 未列」的对象分为三类，处置方向相反：

| 类别 | 形态 | 来源 | 处置 |
|------|------|------|------|
| delta 孤儿 | `{prefix}/{schemaID}/{uuid}.parquet` | #197 | `--repair` 补录回 manifest |
| base 孤儿 | `{min}_{max}.parquet` / `base-{uuid}.parquet` | #188 | `--gc` 删除（宽限期后） |
| `_tmp` 孤儿 | `{prefix}/{schemaID}/_tmp/*` | #188 staging | `--gc` 删除（宽限期后） |

另报告两类只读发现：

- **dangling**：manifest 条目指向的对象已不存在。移除条目会让数据从读路径消失，
  本工具**不**自动删除，保持人工处置。
- **unverifiable**：指向其他 bucket 或带 glob 的条目、以及不在本次 list 前缀下的
  key——本次列举无法证明其缺席，只做信息展示。

无法识别形态的 `.parquet`（unknown）只报告，永不 repair / GC。

## 用法

```bash
# 只读巡检（可周期运行；退出码 0=一致，2=有差异，1=工具故障）
forma-tools manifest-reconcile \
  --s3-bucket my-bucket --data-prefix data \
  --schema-registry-table schema_registry \
  --pg-host ... --pg-db forma

# 修复 #197 孤儿：按 parquet 实际内容重算 RowCount/RowIDMin/Max/CreatedMin/Max
forma-tools manifest-reconcile ... --repair

# 清理 #188 遗留：只删 base/_tmp 形态、且对象最后修改时间早于宽限期的孤儿
forma-tools manifest-reconcile ... --gc --gc-grace 15m
```

要点：

- **`--data-prefix` 必须与 flusher/compactor 的数据前缀一致**（cdc-flush 的
  `--s3-prefix`、compactor 的 `--data-prefix`）。前缀不一致时列举落空，所有 manifest
  条目会被误报为 unverifiable/dangling、所有对象被误报为孤儿。
- 工具需要 Postgres：逐 schema 取与 flusher 同款 advisory lock
  （`pg_try_advisory_lock(schemaID, schemaID)`），消除「list 与 manifest 加载之间新
  上传文件被误判孤儿」的假阳性；拿不到锁的 schema 跳过并计入差异退出码。manifest
  写入另有 ETag 条件写保护（compactor 不持这把锁）。
- `--repair` 幂等：绝不重复已有 `Path`；统计读取失败的文件跳过并保留为孤儿。
  一个 delta 孤儿也可能是 #188 中删除失败的 merged source——补录它是安全的
  （联邦读按 row_id LWW 去重，下轮 compaction 会重新合并），而误删真正的 #197
  孤儿等于丢数据，因此对 delta 形态一律偏向补录。
- `--gc` 的宽限期兜住 in-flight reader 竞态：持有 splice 前对象清单的查询在
  manifest 提交后约一个查询时长内仍可能引用已解除列出的 key。默认 15m 远超查询
  超时；残余窗口（对象很老、恰在 GC 前一刻才被 splice）在实践中可忽略。
- 退出码：`0` 一致；`2` 存在残余差异（含跳过的 schema）；`1` 工具自身失败。
