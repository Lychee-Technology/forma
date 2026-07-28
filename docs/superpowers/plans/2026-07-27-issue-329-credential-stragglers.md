# Issue #329 — 凭据解析收尾（session token / compactor / exporter region key）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> 执行时先按 AGENTS.md 惯例建 worktree（先清已合并分支、FF main），并将本计划落盘为 `docs/superpowers/plans/2026-07-27-issue-329-credential-stragglers.md`。

## Context

#326（PR #330）把 cdc 包的静态 S3 凭据统一到 `resolveStaticS3Credentials` 的 config-wins/both-halves 规则，但评审台账留下三个相邻分歧点（issue #329）：① session token 仍独立走 config→env 直落，config pair 可与环境的 `AWS_SESSION_TOKEN` 跨源混合；② `cmd/tools` 的 `resolveMergeCredentials` 是全仓最后一个逐变量 env 读取点，可返回半套三元组；③ `duckExporterKey.region` 用 chain-resolved region 而实际配置 exporter 的是 raw `cfg.S3Region`。另有 4 条 cosmetic 项（#330 评审 comment 追加 2 条）。

**用户裁决（已定）**：① token 随提供 pair 的来源走，DuckDB SET + SDK 静态 provider 一起修（顺带修掉 env-pair STS 临时凭据在 SDK 侧丢 token 的 latent bug）；② compactor **完全统一**进单一规则，chain-first 移除；③ key 改用 `cfg.S3Region`；④ cosmetic 全收（含 breaker 测试拆分）。

**⚠️ 裁决②的行为后果（须写入代码注释与 PR 描述）**：IMDS/instance-role、SSO、assumed-role、shared-profile 等仅 chain 可见的凭据源不再喂给 merge/reconcile 引擎的 DuckDB SET。`cmd/tools` 没有 `--s3-access-key` flag（`tool_flags.go:97-104` 已核实），故工具侧实际是 **env-pair-or-nothing**。Lambda 场景 env 三元组齐全且 token 现在随 env pair 走，不受影响；SDK 侧 `buildToolS3Client` 仍走完整 chain。若未来需恢复 chain 支持，正解是加凭据 flag 而非恢复第二套解析规则。

**Goal:** 全仓凭据解析收敛到唯一导出规则 `cdc.ResolveStaticS3Credentials(cfg) (key, secret, token)`，token 永不跨源；exporter 缓存 key 如实反映配置输入。

**Tech Stack:** Go, aws-sdk-go-v2, DuckDB httpfs SET, testify（cdc/factory）/ 纯 stdlib testing（cmd/tools，须匹配包内风格）。

## Global Constraints

- 文件 ≤500 行、函数 ≤100 行；错误一律 `fmt.Errorf("...: %w", err)`；early return。
- TDD 红先行（arity 变化的"红"= 编译错误，须确认报错点名正确符号）。
- 单测：`GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test <pkg> -run <Name> -v`；收尾 `make test` + `make lint`（钉 v1.64.8）。
- **e2e build tag 藏了一个调用点**：`internal/e2e_harness/production/manifest_reconcile_e2e_test.go:56` 在 `//go:build e2e` 后，`make test` 抓不到——每次签名变更后必须跑 `go vet -tags e2e ./internal/e2e_harness/production/`。
- 纯移动用 sed 机械搬运，禁手写重打；commit 尾加 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`。
- `internal/bootstrap/s3.go`（第 4 个 both-halves 站点，不同包）**不在范围**。
- PR 关联 #329（Closes #329），**不自动合并**。

## 探索核实的关键事实（@ 67a777d）

- 规则本体：`internal/cdc/aws_credentials.go`（23 行）；内部调用点 `runner.go:180`、`flusher.go:99`、`flusher.go:152`、`init.go:101`。
- `resolveExporterSessionToken`：`duckdb_exporter_init.go:73-78`，在 `buildExporterS3Steps` SET 表 :43 被调；删除后该文件 `os` import 孤立。
- `NewDuckExporter` 调用点 ×6：flusher.go:100、init.go:102、runner.go `newDuckExporterFn`(:28)+:252、compactor.go:123、manifest_reconcile.go:249、e2e_harness/production/cdc.go:199（+ e2e-tag 的 manifest_reconcile_e2e_test.go:56）。
- SDK 空 token provider：`flusher.go:153`、`runner.go:210`。
- `resolveMergeCredentials`：`compactor.go:88-104`（chain-first + 逐变量 env 回落）；调用点 compactor.go:108、manifest_reconcile.go:234；两处都把 token 写进 `duckCfg.S3SessionToken` 后调 `NewDuckExporter`——统一后该赋值成自引用死写，必须删除。删函数后 compactor.go 的 `awsconfig`、`os` import 孤立（manifest_reconcile.go 的 `os` 因 :199 `report.Render(os.Stdout)` 保留）。
- `internal/cdc/config.go:56` `S3SessionToken` 注释（"overrides environment variable AWS_SESSION_TOKEN"）将失真，须改。
- runner 缓存：`cachedS3Runtime`/`s3RuntimeKey`/`duckExporterKey` 均无 token 字段；`duckExporterKey.region` 用 `s3Runtime.region`(:236)。
- 测试现状：`aws_credentials_test.go` 7-case 二元组表测试；`duckdb_exporter_init_test.go` 的 `_SessionTokenEnvFallback`(:90) 钉旧跨源行为须重写、`_FullConfigStatementSet`(:65) 的 `S3SessionToken:"tok123"` 无 pair，须移到参数；`runner_test.go` 5 处手工 save/restore `newS3ClientFn`；`cmd/tools` 测试全 stdlib 无 testify；`resolveMergeCredentials` 零覆盖。
- 改名影响 22 处引用/4 文件；breaker 4 测试在 `factory_entity_manager_unit_test.go:293-341`，移出后老文件 `time`/`require`/`zap`/`observer` 4 个 import 孤立。
- `flusher_aws_test.go:65` `return aws.Config{}, err` 未包装（runner 孪生 ca99945 已修）；该文件无 `fmt` import。

## 依赖图

```
T1 (resolver 三元组+导出)
 ├─> T2 (SDK provider 接 token)
 └─> T3 (exporter token 显式参数) ──> T4 (cmd/tools 统一)
        └─> T5 (region key) ──> T6 (token 进缓存 key)
T7-T10 cosmetic 独立（T9 须在 T8 后：同文件）──> T11 全支验证
```

---

## Task 1 — 共享 resolver 返回三元组并导出

**Files:** modify `internal/cdc/aws_credentials.go`、`aws_credentials_test.go`、`config.go:56`（仅注释）、四个内部调用点（机械 arity，token 先弃元）。

**Interfaces:** produces `func ResolveStaticS3Credentials(cfg CDCConfig) (accessKeyID, secretAccessKey, sessionToken string)`；retires 未导出旧签名。直接改名导出而非包 wrapper：一条规则两个名字正是 #326 要消灭的重复；`CDCConfig` 已导出，无新类型面。

- [ ] **1.1 (red)** 重写 `aws_credentials_test.go` 表测试：保留原 7 case（补 token 列，期望 `""`），新增 6 个 token 纪律 case：

```go
// #329 token discipline: the token rides the source that supplied the pair.
{name: "config pair carries the config token", cfgKey: "ck", cfgSecret: "cs", cfgToken: "ct",
	wantKey: "ck", wantSecret: "cs", wantToken: "ct"},
{name: "config pair does not adopt an ambient env token", cfgKey: "ck", cfgSecret: "cs", envToken: "et",
	wantKey: "ck", wantSecret: "cs", wantToken: ""},
{name: "env pair carries the env token", envKey: "ek", envSecret: "es", envToken: "et",
	wantKey: "ek", wantSecret: "es", wantToken: "et"},
{name: "env pair does not adopt the config token", cfgToken: "ct", envKey: "ek", envSecret: "es", envToken: "et",
	wantKey: "ek", wantSecret: "es", wantToken: "et"},
{name: "half env pair drops the env token too", envKey: "ek", envToken: "et",
	wantKey: "", wantSecret: "", wantToken: ""},
{name: "config token alone is not a credential source", cfgToken: "ct",
	wantKey: "", wantSecret: "", wantToken: ""},
```

  循环体加 `t.Setenv("AWS_SESSION_TOKEN", tc.envToken)`，调用改三返回值 `ResolveStaticS3Credentials(CDCConfig{S3AccessKeyID: tc.cfgKey, S3SecretAccessKey: tc.cfgSecret, S3SessionToken: tc.cfgToken})`。

- [ ] **1.2 (run red)** `go test ./internal/cdc -run TestResolveStaticS3Credentials -v` → 编译错 `undefined: ResolveStaticS3Credentials`（确认点名该符号）。
- [ ] **1.3 (green)** 重写 `aws_credentials.go`：

```go
func ResolveStaticS3Credentials(cfg CDCConfig) (accessKeyID, secretAccessKey, sessionToken string) {
	if cfg.S3AccessKeyID != "" {
		return cfg.S3AccessKeyID, cfg.S3SecretAccessKey, cfg.S3SessionToken
	}
	envKey, envSecret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if envKey != "" && envSecret != "" {
		return envKey, envSecret, os.Getenv("AWS_SESSION_TOKEN")
	}
	return "", "", ""
}
```

  doc comment 保留 #326 both-halves 说明，追加 #329 段：token 随提供 pair 的来源走且永不跨源（长期 key 配外来临时 token 会产生存储端只报 opaque 签名失败的组合）；无 pair 的孤 token 不是凭据源，丢弃。
- [ ] **1.4 (green)** 四内部调用点机械改 `x, y, _ := ResolveStaticS3Credentials(cfg)`（flusher.go:152 处为 `if staticKey, staticSecret, _ := ...; staticKey != ""`）；flusher.go:150 注释同步改名。
- [ ] **1.5 (green)** `config.go:56` 注释改为：token 仅随 `S3AccessKeyID/S3SecretAccessKey` 同源生效，见 `ResolveStaticS3Credentials`（#329）。
- [ ] **1.6 (run green)** 13 subtests PASS；`go test ./internal/cdc` ok。
- [ ] **1.7 (commit)** `fix(cdc): #329 session token rides the pair source in the shared static-credential rule`

## Task 2 — SDK 静态 provider 接上 token

**Files:** modify `flusher.go`(~152)、`runner.go`（struct :37-43、:180、:209-219）、`flusher_aws_test.go`、`runner_test.go`。

**Interfaces:** `cachedS3Runtime` 增 `sessionToken string`。

- [ ] **2.1 (red)** `flusher_aws_test.go` 追加两个测试（stdlib 风格、用现有 `stubLoadAWSConfig`+`retrieveCreds`）：
  - `TestSetupAWSClient_EnvTripleCarriesSessionToken`：Setenv 三元组、`CDCConfig{}`，断言 `retrieveCreds(...).SessionToken == "env-token"`。
  - `TestSetupAWSClient_ConfigPairRejectsAmbientEnvToken`：只 Setenv token、config pair，断言 SessionToken 为空。

  `runner_test.go` 追加 `TestRunnerGetOrCreateS3Runtime_EnvTripleCarriesSessionToken`：stub 两个 seam、Setenv 三元组，断言 `runtime.sessionToken == "env-token"` 且 provider 取回同值。（seam 先用手工 save/restore，T7 统一清理。）
- [ ] **2.2 (run red)** 编译错 `runtime.sessionToken undefined`；flusher 两测试行为红：`got ""`。
- [ ] **2.3 (green)** flusher.go:149-154：`if staticKey, staticSecret, staticToken := ResolveStaticS3Credentials(cfg); staticKey != "" { awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(staticKey, staticSecret, staticToken) }`，注释补 #329。
- [ ] **2.4 (green)** runner.go：struct 加 `sessionToken string`；:180 收三元组；:210 provider 传 `sessionToken`；runtime 字面量存 `sessionToken`。
- [ ] **2.5 (run green)** 3 新测试 PASS + 包 ok。
- [ ] **2.6 (commit)** `fix(cdc): #329 SDK static credential providers carry the resolved session token`

## Task 3 — token 成为 exporter 显式入参，删 `resolveExporterSessionToken`

**Files:** modify `duckdb_exporter_init.go`（两 builder 加参、删 :73-78、删 `os` import）、`duckdb_exporter.go:48-49`、`flusher.go:99-100`、`init.go:101-102`、`runner.go:252`、`duckdb_exporter_init_test.go`、`runner_test.go:90`（stub 签名）、`cmd/tools/compactor.go:123`、`manifest_reconcile.go:249`（过渡期传现有 `token`）、`internal/e2e_harness/production/cdc.go:199`、`manifest_reconcile_e2e_test.go:56`。

**Interfaces:** produces `NewDuckExporter(ctx, cfg, s3AccessKey, s3Secret, s3SessionToken string, logger)`、`buildExporterInitSteps/buildExporterS3Steps(cfg, key, secret, token)`。**传播方式**：紧跟 `s3Secret` 的平参数——builder 内读 `cfg.S3SessionToken` 恰是 #329 要关的独立解析；struct 包装为一条三串返回值加转换成本、还动导出面。三个同型串的换位风险靠调用点固定写法 `key, secret, token := cdc.ResolveStaticS3Credentials(cfg)` 紧邻构造调用、顺序按构造对齐来压。

- [ ] **3.1 (red)** `duckdb_exporter_init_test.go`：`_FullConfigStatementSet` 的 `S3SessionToken: "tok123"` 移出 cfg 字面量、作为第 4 参传入；`_SessionTokenEnvFallback`（钉旧行为）替换为：

```go
func TestBuildExporterInitSteps_IgnoresAmbientEnvSessionToken(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue", "")
	require.NoError(t, err)
	for _, sql := range flattenStepSQL(steps) {
		require.NotContains(t, sql, "s3_session_token")
	}
}

func TestBuildExporterInitSteps_EmitsCallerResolvedSessionToken(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue", "callertok")
	require.NoError(t, err)
	require.Contains(t, flattenStepSQL(steps), "SET s3_session_token='callertok';")
}
```

  （若无 `newExporterInitTestConfig` helper 则内联最小 cfg。）其余调用点 :31/:51/:61/:99 补 `""` token 参。
- [ ] **3.2 (run red)** 编译错 `too many arguments in call to buildExporterInitSteps`。
- [ ] **3.3 (green)** 两 builder 加 `s3SessionToken string` 参；SET 表 `{"s3_session_token", s3SessionToken}`，注释改为「token 由调用方随 pair 一并解析传入，故只能来自提供 pair 的来源（#329），见 ResolveStaticS3Credentials」；删 :73-78 与 `os` import。
- [ ] **3.4 (green)** `NewDuckExporter` 加参并透传；doc comment 补一句三元组由调用方解析的理由。
- [ ] **3.5 (green)** cdc 包内调用点：flusher/init 改 `s3Key, s3Secret, s3Token := ResolveStaticS3Credentials(cfg)` + 6 参调用（注释补 #329）；runner.go:252 传 `s3Runtime.sessionToken`（用缓存值而非重解析：一次解析保证 SDK provider 与 DuckDB SET 恒同源）；runner_test.go:90 stub 签名同步。
- [ ] **3.6 (green)** 包外调用点：compactor.go:123 / manifest_reconcile.go:249 过渡传现有 `token`；e2e_harness/production/cdc.go:199 与 manifest_reconcile_e2e_test.go:56 改 `s3Key, s3Secret, s3Token := cdc.ResolveStaticS3Credentials(...)` + 6 参。
- [ ] **3.7 (run green)** `go test ./internal/cdc -run TestBuildExporterInitSteps -v`；`go test ./internal/cdc ./cmd/... ./internal/...`；**必跑** `go vet -tags e2e ./internal/e2e_harness/production/`（唯一能证 e2e-tag 调用点编译的闸门，不得推迟到 T11）。
- [ ] **3.8 (commit)** `fix(cdc): #329 session token becomes an explicit DuckExporter input; retire resolveExporterSessionToken`

## Task 4 — compactor / manifest-reconcile 完全统一

**Files:** modify `cmd/tools/compactor.go`（删 :88-104、重写 `openMergeEngine`、删 `awsconfig`+`os` import）、`manifest_reconcile.go:233-254`；create `cmd/tools/compactor_test.go`（stdlib 风格）。

- [ ] **4.1 (red)** 新建 `compactor_test.go`：`TestOpenMergeEngine_DoesNotConsultCredentialChain` + `TestOpenReconcileStatsEngine_DoesNotConsultCredentialChain`——stub `toolLoadAWSConfigFn` 置 `called=true` 并返回**非 nil provider**（`awscreds.NewStaticCredentialsProvider("chain-key",...)`；裸 `aws.Config{}` 会让旧代码在 `Credentials.Retrieve` 上 panic 而非红）；Setenv env 三元组；调 `openMergeEngine(ctx, &compactorOptions{...仅 s3.region...}, zap.NewNop())`，`defer exporter.DB.Close()`，断言 `!called`。注释写明 chain-only 源被有意断开。
- [ ] **4.2 (run red)** 两测试 FAIL：`merge engine must not load the AWS default credential chain (#329)`。
- [ ] **4.3 (green)** compactor.go：删 `resolveMergeCredentials`；`openMergeEngine` 改为先建 `duckCfg`（**删 `S3SessionToken: token` 行**——resolver 以 duckCfg 为输入，把自身输出写回是自引用死写），然后：

```go
	key, secret, token := cdc.ResolveStaticS3Credentials(duckCfg)
	exporter, err := cdc.NewDuckExporter(ctx, duckCfg, key, secret, token, logger)
```

  doc comment 写入完整 WARNING（deliberate narrowing：chain-only 源不再进 httpfs SET；工具无凭据 flag 故 env-pair-or-nothing；Lambda env 三元组不受影响；恢复 chain 的正解是加 flag）。删 `awsconfig`、`os` import。
- [ ] **4.4 (green)** manifest_reconcile.go 同构改造（`os` import 保留）；doc comment 指向 openMergeEngine 的 WARNING。
- [ ] **4.5 (run green)** 两测试 PASS；`go test ./cmd/...` ok。（离线沙箱 `INSTALL httpfs` 失败不碍事：`duckdbinit.MakeConnInit` log-and-skip，构造仍成功。）
- [ ] **4.6 (commit)** `fix(tools): #329 unify merge and reconcile engines onto cdc.ResolveStaticS3Credentials`

## Task 5 — `duckExporterKey.region` 改用 raw `cfg.S3Region`

**Files:** modify `runner.go:233-243`、`runner_test.go`。

- [ ] **5.1 (red)** 红锚点测试 `TestRunnerDuckExporterCacheKeyIgnoresChainResolvedRegion`：stub `newDuckExporterFn` 计数；同一 `cfg`（`S3Region` 空）、两个手工构造的 `cachedS3Runtime` 仅 `.region` 不同（us-east-1 / eu-west-1），两次 `getOrCreateDuckExporter` 须 `require.Same` 且 `createCalls == 1`。
- [ ] **5.2 (run red)** `Not same` + `expected: 1, actual: 2`。
- [ ] **5.3 (green)** key 字面量 `region: cfg.S3Region` + 注释：exporter 由 cfg 配置、空 `S3Region` 完全抑制 `SET s3_region`，用 chain region 做 key 是声称 exporter 从未做出的区分（#329）；进程内 ambient region 稳定，纯 key 诚实性无行为变化。
- [ ] **5.4 (run green)** `-run TestRunner` 全绿（既有 `TestRunnerCachesDuckExporter` cfg 设了 `S3Region: "us-east-1"` 与 runtime 一致，是天然对照）。
- [ ] **5.5 (commit)** `fix(cdc): #329 key the DuckExporter cache on raw cfg.S3Region, not the chain-resolved region`

## Task 6 — token 纳入两个缓存 key（与 T5 同类的 key 诚实性，**必做**）

T2/T3 之后缓存的 provider 与 exporter SET 都嵌入了 token，但两个 key 都不含它：env pair 不变而 `AWS_SESSION_TOKEN` 轮换后会命中 stale-token 工件——正是 T5 修的那类问题，差一个字段。

**Files:** modify `runner.go`（`s3RuntimeKey`、`duckExporterKey` 各加 `sessionToken string` + 两处 key 字面量）、`runner_test.go`。

- [ ] **6.1 (red)** 两测试：`getOrCreateS3Runtime` 跨 `t.Setenv("AWS_SESSION_TOKEN", ...)` 变更（pair 不变）两次调用须 `require.NotSame` 且 load 计数 =2；`getOrCreateDuckExporter` 对仅 `sessionToken` 不同的两个 runtime 须构造两次。
- [ ] **6.2 (run red)** `NotSame` 失败 / `expected: 2, actual: 1`。
- [ ] **6.3 (green)** 两 struct 加字段、key 字面量分别填 `sessionToken` / `s3Runtime.sessionToken`，注释：token 是缓存工件烘焙进去的签名身份的一部分，缺席会在轮换后返回 stale-token 工件（#329）。
- [ ] **6.4 (run green)** `go test ./internal/cdc` ok。
- [ ] **6.5 (commit)** `fix(cdc): #329 include the session token in the S3 runtime and exporter cache keys`

## Task 7 — `stubNewS3Client` 测试 helper（纯测试重构，无红步）

**Files:** modify `runner_test.go`。

- [ ] **7.1** 基线：`-run TestRunner -v | grep -c '^--- PASS'` 记数。
- [ ] **7.2** 加 helper（`stubLoadAWSConfig` 的 t.Cleanup 孪生，注释注明 seam 进程全局、勿 t.Parallel）：

```go
func stubNewS3Client(t *testing.T, fn func(aws.Config, string, bool) *s3.Client) {
	t.Helper()
	previous := newS3ClientFn
	newS3ClientFn = fn
	t.Cleanup(func() { newS3ClientFn = previous })
}
```

- [ ] **7.3** 清扫 6 处（原 5 处 + T2 新增那处）：块状/单行 save-restore 全部收敛为 `stubNewS3Client(t, func(aws.Config, string, bool) *s3.Client { return &s3.Client{} })`（计数场景闭包内自增）。`newDuckExporterFn` 的手工 stub 不动（不同 seam，未被要求）。
- [ ] **7.4** 复跑，PASS 计数与 7.1 一致。
- [ ] **7.5 (commit)** `test(cdc): #329 add stubNewS3Client and unify the runner seam-restore idiom`

## Task 8 — factory fixture verb-first 改名（22 处/4 文件）

- [ ] **8.1** 基线 `go test ./factory`。
- [ ] **8.2** 机械替换（`\b` 词边界防误伤）：

```bash
sed -i '' 's/\bunitEntityManagerDeps\b/buildUnitEntityManagerDeps/g; s/\bunitEntityManagerConfig\b/newUnitEntityManagerConfig/g' \
  factory/factory_entity_manager_unit_test.go factory/factory_close_test.go \
  factory/parquet_source_test.go factory/schema_validator_test.go
```

- [ ] **8.3** `grep -rn '\bunitEntityManagerDeps\b\|\bunitEntityManagerConfig\b' factory/` → 零输出。
- [ ] **8.4** `go test ./factory` ok。
- [ ] **8.5 (commit)** `test(factory): #329 rename entity-manager unit fixtures to build/new prefixes`

## Task 9 — breaker 4 测试纯移动（T8 之后；sed 搬运，全计划风险最高的编辑）

**Files:** create `factory/factory_circuit_breaker_unit_test.go`；modify `factory_entity_manager_unit_test.go`（删 :292-341 + 4 孤立 import）。

- [ ] **9.1** 切割前核边界：`sed -n '291,293p;341p'` 应见 `}`/空行/`func TestNewDuckDBCircuitBreakerUsesDefaultParametersForZeroConfig...`/`}`；**不符则停下重算区间**。
- [ ] **9.2** `printf` 头（package + import：testing/time/forma/assert/require/zap/observer + 移动注记）`+ sed -n '293,341p' >` 新文件。
- [ ] **9.3** `sed -i '' '292,341d'` 老文件。
- [ ] **9.4** 删孤立 import（`time`/`require`/`zap`/`observer`——移动后零使用已核实）：`sed -i '' '7d;15d;16d;17d'`（sed 按输入流编号，无级联）。
- [ ] **9.5** 验证：`gofmt -l factory/` 空；`go test ./factory -run TestNewDuckDBCircuitBreaker -v` 4 PASS；包 ok；字节保真检查：

```bash
git show HEAD:factory/factory_entity_manager_unit_test.go | sed -n '293,341p' | diff - <(tail -n 49 factory/factory_circuit_breaker_unit_test.go)
```

  零 diff（这是抓搬运损坏的那道检查）。失败则 `git checkout -- factory/` 重来，勿手工缝补。
- [ ] **9.6 (commit)** `test(factory): #329 split circuit-breaker unit tests into their own file (pure move)`

## Task 10 — `flusher_aws_test.go` stub 补 error wrap

- [ ] **10.1** :65 改 `return aws.Config{}, fmt.Errorf("apply AWS load option: %w", err)`（与 runner 孪生 ca99945 逐字一致）；import 块加 `"fmt"`。
- [ ] **10.2** `gofmt -l internal/cdc/` 空；`-run TestSetupAWSClient` 全 PASS。
- [ ] **10.3 (commit)** `test(cdc): #329 wrap the AWS load-option error in the flusher region stub`

## Task 11 — 全支验证

- [ ] `make test` 全 ok。
- [ ] `go vet -tags e2e ./internal/e2e_harness/production/` 干净（**不可省**：唯一覆盖 e2e-tag 调用点的闸门）。
- [ ] `make lint` 零 findings（重点盯孤立 import 与残留 `resolveExporterSessionToken`/`resolveMergeCredentials` 的 unused 告警）。
- [ ] 残留扫描：`grep -rn 'resolveMergeCredentials\|resolveExporterSessionToken\|resolveStaticS3Credentials' --exclude-dir=.git .` 仅 `docs/superpowers/plans/` 历史文件命中。
- [ ] `grep -n 'IMDS' cmd/tools/compactor.go` 命中 openMergeEngine doc comment——WARNING 落在代码而非仅计划里。
- [ ] 行数抽查全部 ≤500（compactor.go ~188、runner_test.go ~240）。
- [ ] Docker 可用时加跑 `go test -v ./internal/e2e_harness/... -timeout=5m`（infra smoke；本次动的是 CDC 凭据面，production harness 是历史上该类改动的真正闸门，PR CI 会跑）。
- [ ] 建 PR：标题 `fix(cdc): #329 ...`，body 含 Closes #329、裁决②行为后果说明、`🤖 Generated with [Claude Code](https://claude.com/claude-code)`；**不自动合并**。

## 已知风险与对策

- **arity 变化无行为红**：T1/T3 以编译错为红，须确认报错点名目标符号。
- **e2e tag 藏调用点**：3.7 就地 vet，不推迟。
- **三个同型 string 换位无声**：调用点一律 `key, secret, token := cdc.ResolveStaticS3Credentials(cfg)` 紧邻构造、顺序对齐，肉眼可核。
- **T9 sed 区间**：9.1 前核 + 9.5 字节 diff 双括号；失败整体回滚重算。
- **加参致既有调用点默默变默认值**（#314 教训）：本次全部是编译期强制的新参，无默认值路径；e2e vet 兜底。
