# Forma E2E 测试矩阵

更新时间：2026-03-09  
适用仓库：`forma`

## 总览

当前仓库中的 E2E 测试分成两条主线：

- `internal/e2e_harness`：Go 容器化系统级 E2E，作为主测试体系
- `tests/e2e`：Bun/k6 脚本化黑盒流程验证，作为补充验证体系

代码是事实来源，README 用于导航。当前可确认的范围如下：

- Go harness：`68` 个 federated case，外加 `3` 个 suite/baseline 入口
- Bun/k6：`6` 个流程脚本，外加 `3` 个 k6 负载场景

默认执行口径：

- Go 侧默认通过 `go test` 入口运行，功能类与一致性类属于主路径
- Go 性能类存在于 federated suite 中，但建议单独执行并单独设置 timeout
- Bun 侧默认 `bun run test` 只执行 `register-schemas -> gen-data -> cdc-flush -> federated-check`
- `cdc-init`、`compactor`、k6 均属于存在但不在默认主流程中的扩展步骤

## 单一视图矩阵

| Case/脚本名 | 所属体系 | 层级 | 测试目标 | 覆盖链路 | 运行命令 | 是否默认执行 | 依赖条件 | 备注/限制 |
|---|---|---|---|---|---|---|---|---|
| `TestE2EHarnessMinimal` | Go E2E Harness | 基础设施/Smoke | 验证 Postgres、S3、DuckDB 基础设施可启动并连通 | 容器启动 -> Parquet 写入 -> S3 上传 -> DuckDB 读取 | `go test -v ./internal/e2e_harness/... -run TestE2EHarnessMinimal -timeout=5m` | 是 | Go、Docker | 基础设施烟测，不覆盖三层语义 |
| `TestSuite_Smoke` | Go E2E Harness | 基础设施/Smoke | 验证 harness 初始化、基础数据读写、S3 操作 | Harness 初始化 -> Seed -> Query -> S3 操作 | `go test -v ./internal/e2e_harness/federated/... -run TestSuite_Smoke -tags=e2e -timeout=10m` | 是 | Go、Docker | 关注环境可用性，不覆盖性能 |
| `TestSuite_Integration` | Go E2E Harness | 数据链路一致性 | 验证完整生命周期无重复和无明显断链 | Seed All Tiers -> Federated Query -> Postgres Query -> CDC Flush -> Compaction -> Verify | `go test -v ./internal/e2e_harness/federated/... -run TestSuite_Integration -tags=e2e -timeout=10m` | 是 | Go、Docker | 适合作为回归入口 |
| `TC-01 Three-Tier Data Architecture` | Go E2E Harness | 功能正确性 | 验证 Base/Delta/Hot 单层与三层联合查询 | Base/Delta/Hot -> Federated Query | `go test -v ./internal/e2e_harness/federated/... -run TestDataTier -tags=e2e` | 是 | Go、Docker | 共 7 个测试，覆盖空层与分页 |
| `TC-02 Merge-on-Read Logic` | Go E2E Harness | 功能正确性 | 验证跨层 `UNION ALL`、版本优先级和 dirty ID 排除 | Base + Delta + Hot -> Merge-on-Read | `go test -v ./internal/e2e_harness/federated/... -run TestMergeOnRead -tags=e2e` | 是 | Go、Docker | 共 7 个测试，覆盖 last-write-wins |
| `TC-03 Global Deduplication` | Go E2E Harness | 功能正确性 | 验证单层和跨层去重语义 | Same Tier/Cross Tier -> QUALIFY/ROW_NUMBER | `go test -v ./internal/e2e_harness/federated/... -run TestDeduplication -tags=e2e` | 是 | Go、Docker | 共 6 个测试，含 10K 级批量场景 |
| `TC-04 Soft Delete Filtering` | Go E2E Harness | 功能正确性 | 验证软删除过滤、恢复和 row_id 复用 | Deleted/Restored Records -> Federated Query | `go test -v ./internal/e2e_harness/federated/... -run TestSoftDelete -tags=e2e` | 是 | Go、Docker | 共 7 个测试，覆盖 `NULL`/`0` 语义 |
| `TC-05 CDC Smart Flushing` | Go E2E Harness | 数据链路一致性 | 验证 flush 触发条件、advisory lock、批处理和 delta 文件生成 | Hot Buffer -> CDC Flush -> Delta Parquet | `go test -v ./internal/e2e_harness/federated/... -run TestCDCFlush -tags=e2e` | 是 | Go、Docker | 共 7 个测试，关注 flush 结果而非真实生产调度 |
| `TC-06 Compaction Strategy` | Go E2E Harness | 数据链路一致性 | 验证 delta 合并进 base 后的正确性与 no-op 行为 | Delta Parquet -> Compaction -> Base Parquet | `go test -v ./internal/e2e_harness/federated/... -run TestCompaction -tags=e2e` | 是 | Go、Docker | 共 9 个测试，覆盖低脏比、高脏比、空 delta |
| `TC-07 Data Consistency` | Go E2E Harness | 数据链路一致性 | 验证 Postgres 与 Federated 结果一致 | Postgres Query <-> Federated Query | `go test -v ./internal/e2e_harness/federated/... -run TestConsistency -tags=e2e` | 是 | Go、Docker | 共 9 个测试，覆盖 count、checksum、属性值 |
| `TC-08 Performance Benchmarks` | Go E2E Harness | 性能与故障注入 | 验证分页、复杂过滤、全表扫描、并发、flush、compaction 的耗时门槛 | Federated Query / CDC / Compaction | `go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m` | 否 | Go、Docker | 共 8 个测试，建议单独执行 |
| `TC-09 Failure Modes` | Go E2E Harness | 性能与故障注入 | 验证 S3、Postgres、损坏 parquet、timeout 等故障场景下的降级和恢复 | Failure Injection -> Federated Query / Recovery | `go test -v ./internal/e2e_harness/federated/... -run TestFailureMode -tags=e2e` | 是 | Go、Docker | 共 8 个测试，重点是 graceful degradation |
| `register-schemas` | Bun E2E Scripts | 基础设施/Smoke | 校验 `lead`、`visit`、`log` schema 已可查询或可注册 | Schema Files -> API | `cd tests/e2e && bun run register-schemas` | 是 | Bun、运行中的 server | 默认主流程第一步，生成 `schema-registration.json` |
| `gen-data` | Bun E2E Scripts | 功能正确性 | 通过 API 为多个 schema 批量造数并保留交叉引用 | API Write Path -> Postgres Hot Tier | `cd tests/e2e && bun run gen-data -- --schema all --count 10000` | 是 | Bun、运行中的 server | 默认主流程第二步，生成 `data-gen.json` |
| `cdc-init` | Bun E2E Scripts | 数据链路一致性 | 为存量数据初始化 base parquet | Postgres Main/EAV -> Base Parquet | `cd tests/e2e && bun run cdc-init` | 否 | Bun、Go tools binary、S3、Postgres | 适用于开启 CDC 前的回填，不在 `bun run test` 中 |
| `cdc-flush` | Bun E2E Scripts | 数据链路一致性 | 调用 `tools` 二进制执行 flush 并导出 delta parquet | Change Log -> Delta Parquet -> Manifest | `cd tests/e2e && bun run cdc-flush` | 是 | Bun、Go tools binary、S3、Postgres | 默认主流程第三步，支持 `--dry-run` |
| `compactor` | Bun E2E Scripts | 数据链路一致性 | 调用 `tools` 二进制把 delta 合并进 base | Delta Parquet -> Base Parquet | `cd tests/e2e && bun run compactor -- --all` | 否 | Bun、Go tools binary、S3 | 扩展步骤，不在 `bun run test` 中 |
| `federated-check` | Bun E2E Scripts | 数据链路一致性 | 对比 Forma API 与 Postgres 直查结果 | Forma API <-> Postgres Direct Query | `cd tests/e2e && bun run federated-check -- --schema all --sample-size 100` | 是 | Bun、运行中的 server、Postgres | 默认主流程第四步，支持 `--full-scan` |
| `k6 smoke/full/perf` | Bun k6 | 性能与故障注入 | 验证分页、排序、`advanced_query` 的 SLA | HTTP Query Load -> API/Federated Query | `cd tests/e2e && bun run build-k6 && bun run k6-full` | 否 | Bun、运行中的 server、k6 或 Docker | `smoke=5 VUs/30s`，`full=30 VUs/2m`，`perf=100 VUs/5m` |

## 运行方式

### Go 主路径

```bash
# 基础设施验证
go test -v ./internal/e2e_harness/... -timeout=5m

# Federated 功能与一致性
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m

# 性能类单独跑
go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m
```

### Bun 主路径

```bash
cd tests/e2e
bun run test
```

`bun run test` 等价于：

```bash
bun run register-schemas
bun run gen-data
bun run cdc-flush
bun run federated-check
```

### Bun 扩展步骤

```bash
cd tests/e2e
bun run cdc-init
bun run compactor -- --all
bun run build-k6
bun run k6-smoke
bun run k6-full
bun run k6-perf
```

## 覆盖缺口与注意事项

- Go harness 已是主 E2E 体系，适合验证三层语义、CDC、Compaction、故障和性能门槛。
- Bun 侧更偏黑盒流程验证，重点是 API 造数、工具链执行、对账和压测，不替代 Go harness 的语义级断言。
- `tests/e2e/package.json` 的默认 `test` 不包含 `cdc-init`、`compactor`、k6；这些步骤必须手动执行。
- `tests/e2e/README.md` 过去未列出 `cdc-init.ts` 和 `compactor.ts`；当前文档与 README 已对齐。
- `internal/e2e_harness/README.md` 的 TC 列表与代码当前实现一致，可继续作为 Go harness 侧的分类索引。

## 事实来源

- `internal/e2e_harness/README.md`
- `internal/e2e_harness/e2e_test.go`
- `internal/e2e_harness/federated/suite_test.go`
- `internal/e2e_harness/federated/*_test.go`
- `tests/e2e/README.md`
- `tests/e2e/package.json`
- `tests/e2e/scripts/*.ts`
- `tests/e2e/k6/scenarios.ts`
