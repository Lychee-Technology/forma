# Issue #290: cdc-init 持有 per-schema advisory lock + reconcile GC 化 init 形状孤儿 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> 执行开始时：按仓库 workflow 建 worktree（先清已合并分支、FF main），并把本计划复制为 `docs/superpowers/plans/2026-07-20-issue-290-init-advisory-lock.md` 随 PR 入库。

**Goal:** `cdc-init` 在每个 schema 的 export + manifest publish 期间持有 `pg_try_advisory_lock(schemaID, schemaID)`（钉单连接），从而 `manifest-reconcile --gc` 可以安全删除 init 形状 `{min}_{max}.parquet` 孤儿（失败 publish 的输出 + 被新 init 取代的旧文件）；顺带完成 issue Notes 的两项清理：锁 helper 合并（修 flusher 池上 acquire/release 落不同连接的 latent bug）与共享 quoted DSN builder。

**Architecture:** 在 `internal/cdc` 新增钉单连接的 `TrySchemaLock`（以 `reconcile.PGAdvisoryLocker.TryLock` 为蓝本），四个调用方（init 新增、flusher 迁移、reconcile 委托、federated harness 委托）统一走它；reconcile 侧把 `ClassBaseInit` 加入现有两阶段 sighting GC 候选（分类与 report/退出码逻辑不动）。`--repair` 自动重建 base tier **不做**（部分导出误提升风险），留 follow-up issue。

**Tech Stack:** Go, database/sql + pgx (stdlib driver), PG advisory locks, testify。

## Global Constraints（用户已裁决）

- init 拿不到锁：该 schema 计入 `schemaErrors`（`errors.Is` 可匹配新 sentinel `ErrSchemaLockContended`），继续其他 schema，最终 `errors.Join` 非零返回。**不是** silent skip。
- reconcile 只做 `--gc`；`--repair` 提升 init 孤儿留 follow-up（收尾时开 issue）。
- 锁 helper 合并（含 flusher 迁移）与共享 DSN builder 均入本 PR。
- 仓库规范：文件 ≤500 行、函数 ≤100 行、错误 `fmt.Errorf("...: %w")` 包装、TDD 红先行、频繁提交。
- dry-run init 也拿锁（语义一致，dry-run 同样读 PG）。
- 请不要自动合并 PR；PR body 引用 `Closes #290`。

## 已核实事实（行号基于 main @ da56a4c）

- 无 import 环：`internal/cdc` 不 import `reconcile`/`compaction`；`reconcile` 已（经 `compaction`）传递依赖 `cdc`。委托方向安全。
- `report.go` **零改动**：`SchemaReport.Residual()` 已把 Deleted 键视为 resolved，init 孤儿被删后自动清出 residual；within-grace 仍 residual → exit 2，语义自洽。
- gc-state 无兼容问题：旧 state 文件从未含 init 键（此前被显式排除）。
- 旧 `AcquireSchemaLock`/`ReleaseSchemaLock`（helpers.go:29-46）生产调用方仅 flusher.go:301/305，**无测试引用**，可干净删除。
- flusher 锁测试 4 处注入闭包：flusher_test.go:307/336/368/401。
- init 单测直接构造 `initRunContext`（init_test.go、init_preflight_test.go:13），沿用此缝隙；flusher 已有 `processSchemaFn` 注入先例（flusher.go:256）。

---

### Task A: 共享 DSN builder（独立，可先做）

**Files:**
- Create: `internal/cdc/pgdsn.go`, `internal/cdc/pgdsn_test.go`
- Modify: `internal/cdc/init.go:84-89`、`internal/cdc/flusher.go:202-207`（`setupPostgresConnection`）与 `flusher.go:369-370` 附近（`executeFlush` 的 `pgConnForDuck`，同形状手拼一并换）、`cmd/tools/manifest_reconcile.go:263-281`（`buildToolSQLDB`，删本地 `quotePGConnValue`）
- Test migrate: `cmd/tools/manifest_reconcile_test.go:90-101` 的 quote 用例移入 `pgdsn_test.go`

**Interfaces:**
- Produces: `cdc.PGDSNParams{Host string; Port int; User, Password, DB, SSLMode string}`；`func BuildPGDSN(p PGDSNParams) string`

- [ ] **Step A1: 红** — 写 `internal/cdc/pgdsn_test.go`：

```go
func TestBuildPGDSN_QuotesValues(t *testing.T) {
	got := BuildPGDSN(PGDSNParams{Host: "h", Port: 5432, User: "u u", Password: `p'w\d`, DB: "d", SSLMode: "require"})
	want := `host='h' port=5432 user='u u' password='p\'w\\d' dbname='d' sslmode='require'`
	if got != want {
		t.Fatalf("BuildPGDSN = %s, want %s", got, want)
	}
}
```

注意转义顺序：先 `\`→`\\` 再 `'`→`\'`（与现 `quotePGConnValue` manifest_reconcile.go:277-281 一致），期望串按此推导核对。
Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestBuildPGDSN -v` → FAIL (undefined)

- [ ] **Step A2: 绿** — 写 `internal/cdc/pgdsn.go`：

```go
package cdc

import (
	"fmt"
	"strings"
)

// PGDSNParams are the inputs for a libpq keyword/value connection string.
type PGDSNParams struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
	SSLMode  string
}

// BuildPGDSN renders a libpq keyword/value DSN with every string value quoted
// (single-quote wrapped, backslash and single-quote escaped) so passwords with
// spaces or quotes survive parsing by pgx and DuckDB's postgres scanner.
func BuildPGDSN(p PGDSNParams) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		quotePGConnValue(p.Host), p.Port, quotePGConnValue(p.User),
		quotePGConnValue(p.Password), quotePGConnValue(p.DB), quotePGConnValue(p.SSLMode))
}

func quotePGConnValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}
```

- [ ] **Step A3: 行为保持重构** — 三处替换（既有测试当守卫）：
  - `init.go:88-89` → `pgConnStr := BuildPGDSN(PGDSNParams{Host: cfg.PGHost, Port: cfg.PGPort, User: cfg.PGUser, Password: cfg.PGPassword, DB: cfg.PGDB, SSLMode: sslMode})`
  - `flusher.go:206-207` → 同上但 `Password: pgPassword`（IAM 分支 :192-200 不动，token 已先解析进 `pgPassword`，builder 只负责格式化）；`flusher.go` 中 `pgConnForDuck` 的第二处手拼同样替换（redacted 日志串保持现状）
  - `cmd/tools/manifest_reconcile.go` `buildToolSQLDB` → `cdc.BuildPGDSN(cdc.PGDSNParams{...， Password: pg.resolvedPassword("PGPASSWORD"), ...})`，删除本地 `quotePGConnValue` 及其在 manifest_reconcile_test.go 的用例（已迁移）
- [ ] **Step A4:** `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc ./cmd/tools` → PASS；`make lint` → PASS
- [ ] **Step A5: Commit** — `git commit -m "refactor(cdc): #290 shared quoted PG DSN builder replaces three hand-built copies"`

---

### Task B: 钉单连接 TrySchemaLock + flusher/reconcile/harness 迁移

**Files:**
- Create: `internal/cdc/schema_lock.go`, `internal/cdc/schema_lock_test.go`
- Modify: `internal/cdc/helpers.go`（删 `AcquireSchemaLock`/`ReleaseSchemaLock` :29-46）、`internal/cdc/flusher.go`（字段 :252-253 合并为一、`processSchema` :296-317 重写）、`internal/cdc/flusher_test.go`（:307/336/368/401 四处注入改签名）、`internal/reconcile/db.go`（`PGAdvisoryLocker.TryLock` :72-95 委托 + 注释 :63-67 更新）、`internal/e2e_harness/federated/cdc.go`（`tryAcquireSchemaLock` :77-100 委托）

**Interfaces:**
- Produces: `var ErrSchemaLockContended = errors.New("schema advisory lock contended")`；`func TrySchemaLock(ctx context.Context, db *sql.DB, schemaID int16) (bool, func(), error)`（unlock 非 nil 当且仅当 locked；unlock 用 `context.Background()` 并 `conn.Close()`）
- Consumes: Task C 的 init 接线依赖此签名

- [ ] **Step B1: 红** — 改写 flusher_test.go 第一处锁测试（:307 附近）为单一注入字段：

```go
tryLock: func(context.Context, *sql.DB, int16) (bool, func(), error) {
	lockAttempts++
	return false, nil, nil
},
```

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestProcessSchema -v` → FAIL（编译错，unknown field tryLock）
（保留原断言语义：not-locked 时 skip 返回 nil、release 不被调；locked 用例断言返回的 unlock 闭包被调用。）

- [ ] **Step B2: 绿** — 写 `internal/cdc/schema_lock.go`（正文照搬 `reconcile/db.go:72-95` 的钉连接实现，泛化 schemaID 入参；执行者请对照原文逐行核对语义，尤其 Scan 失败关连接、!locked 关连接、unlock 用 background ctx）：

```go
package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrSchemaLockContended reports that another holder (flusher, cdc-init, or
// manifest-reconcile) owns the per-schema advisory lock.
var ErrSchemaLockContended = errors.New("schema advisory lock contended")

// TrySchemaLock pins one physical connection and takes
// pg_try_advisory_lock(schemaID, schemaID). The lock is session-scoped: on a
// pool, acquire and release could land on different connections, and a lock
// released on the wrong session silently fails — so unlock runs on the same
// pinned conn and closes it to end the session. unlock is non-nil iff locked;
// it uses a background context so the lock is released even after ctx cancel.
func TrySchemaLock(ctx context.Context, db *sql.DB, schemaID int16) (bool, func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("pin connection for schema %d advisory lock: %w", schemaID, err)
	}
	var locked bool
	row := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1, $2)", int32(schemaID), int32(schemaID))
	if err := row.Scan(&locked); err != nil {
		_ = conn.Close()
		return false, nil, fmt.Errorf("acquire schema %d advisory lock: %w", schemaID, err)
	}
	if !locked {
		_ = conn.Close()
		return false, nil, nil
	}
	unlock := func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", int32(schemaID), int32(schemaID))
		_ = conn.Close()
	}
	return true, unlock, nil
}
```

flusher 改造：`schemaFlushContext` 的 `acquireLock`/`releaseLock` 两字段（:252-253）合并为 `tryLock func(context.Context, *sql.DB, int16) (bool, func(), error)`；`processSchema` 锁段（:299-317）改为：

```go
tryLock := c.tryLock
if tryLock == nil {
	tryLock = TrySchemaLock
}
locked, unlock, err := tryLock(ctx, c.db, schemaID)
if err != nil {
	return fmt.Errorf("acquire schema lock: %w", err)
}
if !locked {
	c.logger.Sugar().Infow("lock not acquired, skipping", "schema_id", schemaID)
	return nil
}
defer unlock()
```

（flusher contended 语义**不变**：skip + return nil。）删除 helpers.go 的 `AcquireSchemaLock`/`ReleaseSchemaLock`。

- [ ] **Step B3:** 逐一改剩余三处 flusher 锁测试注入（:336/368/401）为 `tryLock` 形式；`schema_lock_test.go` 只做轻量单测（无 PG 环境）：验证 `ErrSchemaLockContended` 的 `errors.Is` 身份、以及 `db.Conn(ctx)` 失败路径返回包装错误（可用已关闭的 `sql.DB`）；真实锁行为由 reconcile/production e2e 覆盖（`PGAdvisoryLocker` 现状同此）。
- [ ] **Step B4:** 委托两处：
  - `internal/reconcile/db.go`：`func (l *PGAdvisoryLocker) TryLock(ctx context.Context, schemaID int16) (bool, func(), error) { return cdc.TrySchemaLock(ctx, l.DB, schemaID) }`，注释改为指向 `cdc.TrySchemaLock` 为单一实现来源
  - `internal/e2e_harness/federated/cdc.go:77-100`：`tryAcquireSchemaLock` 改为 `return cdc.TrySchemaLock(ctx, h.PGDB, h.SchemaID)`（原 2s unlock 超时被共享实现的 background ctx 取代，属预期变化）
- [ ] **Step B5:** `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc ./internal/reconcile` → PASS；`make lint` → PASS
- [ ] **Step B6: Commit** — `git commit -m "refactor(cdc): #290 single pinned-connection TrySchemaLock; flusher/reconcile/harness converge on it"`

---

### Task C: cdc-init 每 schema 持锁

**Files:**
- Modify: `internal/cdc/init.go`（`initRunContext` 加字段、`processInitSchemas` :194-204 循环改调包装函数、新增 `initSchemaUnderLock`）
- Create/Modify test: `internal/cdc/init_lock_test.go`（新文件，构造 `initRunContext` 的手法沿用 init_preflight_test.go:13）

**Interfaces:**
- Consumes: Task B 的 `TrySchemaLock`、`ErrSchemaLockContended`
- Produces: `initRunContext.tryLock func(context.Context, *sql.DB, int16) (bool, func(), error)`（nil→真实现）；`initRunContext.initSchemaFn func(context.Context, *initRunContext, int16) (int64, int, error)`（nil→`initSchema`，测试缝隙，镜像 flusher 的 `processSchemaFn` 先例）

- [ ] **Step C1: 红** — 写 `internal/cdc/init_lock_test.go`：

```go
func TestProcessInitSchemas_ContendedLockRecordsErrorAndContinues(t *testing.T) {
	var initCalled []int16
	var unlockCalled []int16
	runCtx := &initRunContext{
		logger:         zap.NewNop(),
		schemaRegistry: /* 复用 init_preflight_test.go 的 fake registry，为 schema 7、8 提供 attrCache */,
		tryLock: func(_ context.Context, _ *sql.DB, sid int16) (bool, func(), error) {
			if sid == 7 {
				return false, nil, nil
			}
			return true, func() { unlockCalled = append(unlockCalled, sid) }, nil
		},
		initSchemaFn: func(_ context.Context, _ *initRunContext, sid int16) (int64, int, error) {
			initCalled = append(initCalled, sid)
			return 5, 1, nil
		},
	}
	summary, err := processInitSchemas(context.Background(), runCtx, []int64{7, 8})
	if !errors.Is(err, ErrSchemaLockContended) {
		t.Fatalf("want ErrSchemaLockContended in joined error, got %v", err)
	}
	if !strings.Contains(err.Error(), "schema 7") {
		t.Fatalf("error must name the contended schema: %v", err)
	}
	if len(initCalled) != 1 || initCalled[0] != 8 {
		t.Fatalf("schema 8 must still be processed, got %v", initCalled)
	}
	if len(unlockCalled) != 1 || unlockCalled[0] != 8 {
		t.Fatalf("unlock must run for locked schema 8, got %v", unlockCalled)
	}
	if summary.TotalRowsExported != 5 || summary.TotalFilesCreated != 1 {
		t.Fatalf("summary must reflect the successful schema, got %+v", summary)
	}
}
```

（若 fake registry 不便复用则就地写最小 fake；错误消息须命名 schema 与锁状态，符合 error-semantics 规范。）
Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestProcessInitSchemas_Contended -v` → FAIL（unknown fields）

- [ ] **Step C2: 绿** — `init.go`：`initRunContext` 加 `tryLock`、`initSchemaFn` 两字段；`processInitSchemas` 循环把 `initSchema(...)` 调用换成 `initSchemaUnderLock(...)`（错误收集逻辑 :197-201 不动，外层已 wrap "schema %d: %w"）；新增：

```go
// initSchemaUnderLock holds the per-schema advisory lock for the whole
// export + manifest publish, so manifest-reconcile (which reconciles under
// the same lock) can never race an in-flight init (#290). Contended is an
// error, not a skip: init is operator-initiated and a silently skipped
// schema would stay uninitialized unnoticed.
func initSchemaUnderLock(ctx context.Context, runCtx *initRunContext, schemaID int16) (int64, int, error) {
	tryLock := runCtx.tryLock
	if tryLock == nil {
		tryLock = TrySchemaLock
	}
	locked, unlock, err := tryLock(ctx, runCtx.db, schemaID)
	if err != nil {
		return 0, 0, fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !locked {
		return 0, 0, ErrSchemaLockContended
	}
	defer unlock()

	initSchemaFn := runCtx.initSchemaFn
	if initSchemaFn == nil {
		initSchemaFn = initSchema
	}
	return initSchemaFn(ctx, runCtx, schemaID)
}
```

（锁覆盖整个 `initSchema` 含 0 行提前返回与 dry-run 路径；`defer unlock()` 保证释放。）

- [ ] **Step C3:** `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run TestProcessInitSchemas -v` → PASS；全包 `go test ./internal/cdc` → PASS
- [ ] **Step C4: Commit** — `git commit -m "feat(cdc): #290 cdc-init holds the per-schema advisory lock across export + manifest publish"`

---

### Task D: reconcile 把 ClassBaseInit 纳入 --gc 候选

**Files:**
- Modify: `internal/reconcile/reconcile.go:161-167`（候选构造 + 注释）、`internal/reconcile/classify.go:20-27`（类文档注释）、`internal/reconcile/gc.go:12-25` 附近（"init-shaped never reach here" 注释）
- Modify test: `internal/reconcile/race_guard_test.go:14-37`（语义反转）、`internal/e2e_harness/production/manifest_reconcile_e2e_test.go`（:323-328 注释、:353-370 within-grace 断言不变、:372-416 past_grace 断言反转——initShaped 应被删，deltaOrphan 仍幸存）

**Interfaces:**
- Consumes: 无代码依赖 Task A-C（可并行开发），但**语义前提**是 Task C（init 持锁）已入同一 PR
- Produces: 无新导出；`classifyObjectKey`、`diffSchema`、`report.go` 均零改动

- [ ] **Step D1: 红** — 反转 `race_guard_test.go`：`TestGC_SkipsInitShapedBaseOrphans` → `TestGC_CollectsInitShapedBaseOrphans`，fixture 不变（initShaped + merged 均 seed 过期 sighting），断言改为：

```go
require.ElementsMatch(t, []string{initShaped, merged}, report.Schemas[0].Deleted)
require.ElementsMatch(t, []string{initShaped, merged}, report.Schemas[0].BaseOrphans)
require.False(t, report.HasResidualDiscrepancies())
```

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/reconcile -run TestGC_Collects -v` → FAIL（initShaped 未被删）

- [ ] **Step D2: 绿** — `reconcile.go:161-167` 候选构造改为：

```go
// Init-shaped base orphans are GC candidates since #290: cdc-init holds
// the same per-schema advisory lock, so under this lock an init-shaped
// orphan is provably not from an in-flight init — it is either the output
// of a failed manifest publish or a file superseded by a later init run.
// Recovery for a failed publish is re-running cdc-init (the source of
// truth is entity_main); auto-promotion (--repair) is a follow-up.
// Delta leftovers require the repair analysis, so they are only deletable
// under --repair --gc.
candidates := append([]ObjectInfo(nil), d.baseInitOrphans...)
candidates = append(candidates, d.baseMergedOrphans...)
candidates = append(candidates, d.tmpOrphans...)
candidates = append(candidates, deltaLeftovers...)
```

同步改 `classify.go:20-27` 与 `gc.go` 头部注释中 "holds no advisory lock / never GC candidates" 的过时理由。

- [ ] **Step D3:** `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/reconcile` → PASS
- [ ] **Step D4:** 反转 production e2e：`manifest_reconcile_e2e_test.go` `past_grace_deletes_leftovers_only` 子测预期删除对象从 {staleBase, staleTmp} 扩为 {staleBase, staleTmp, initShaped}（deltaOrphan 仍幸存，未 --repair）；相应 count 与注释更新。within-grace 子测断言不变。
- [ ] **Step D5: 注释/文档扫尾** — `grep -rn "init-shaped\|ClassBaseInit" --include="*.go" .` 与 `grep -rn "init-shaped" docs/ cmd/` 全量过一遍，更新仍宣称 "report-only / holds no advisory lock" 的注释、log 文本（如 `internal/compaction/rewrite.go:168`、`cmd/tools/main.go` 帮助文本、`internal/cdc/init.go:399-407` 的 updateSchemaManifest 文档注释——现在可明确指向 `--gc`）与 manifest-reconcile 相关 docs。
- [ ] **Step D6: Commit** — `git commit -m "feat(reconcile): #290 init-shaped base orphans join two-phase --gc now cdc-init locks"`

---

### Task E: e2e 验证 + 并发 init 测试 revisit + 收尾

- [ ] **Step E1:** 单测全绿 + lint：`make test && make lint`
- [ ] **Step E2:** 定向 e2e（需 Docker）：
  - `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test -v -tags=e2e ./internal/e2e_harness/production/ -run 'TestManifestReconcile' -timeout 20m` → 反转后断言 PASS
  - `... -run 'TestInit' -timeout 30m` → 覆盖 `TestInitUnderConcurrentMutation`（mutator 直写 PG 不拿锁，预期不受影响，绿）与 init_rerun 等；若该测试内 harness flusher 与 init 抢锁导致行为变化，按实际观察调整（issue 第 3 点 "revisit" 的落点：预期是**注释说明新锁语义**而非改断言；contended 行为已由 Task C 单测覆盖，不加新 e2e）
  - federated 冒烟（harness cdc.go 被改）：`go test -v -tags=e2e -short ./internal/e2e_harness/federated/... -timeout 15m`
- [ ] **Step E3:** DSN 换用 quoted 形式后重点确认 DuckDB postgres_scanner 路径（init/flush e2e 已覆盖；若 e2e 失败先查引号解析）
- [ ] **Step E4:** 开 follow-up issue：`manifest-reconcile --repair` 自动重建 base tier（带覆盖完整性验证：DuckDB stats + PG live rows 证明孤儿集覆盖全部存活行后才 ReplaceTierFiles 提升；引用 #290 讨论）
- [ ] **Step E5:** 提 PR：body 引用 `Closes #290`，说明四块改动（init 锁、reconcile GC 化、锁 helper 合并、DSN builder）与 flusher latent bug 修复；**不自动合并**

## 风险与注意点

1. **init 长时间持锁饿 flusher**：init 导出期间 flusher 对该 schema 每轮 skip（返回 nil，安全）。不做 per-batch 放锁（会重新打开 #290 要关的竞态窗口）。PR body 注明运维影响。
2. **本 PR 只关 init↔reconcile 竞态**：init 的 `ReplaceTierFiles` 与 compactor 的 manifest 写仍靠 etag 乐观并发（compactor 不拿此锁），维持现状，issue 范围内。
3. **reconcile→cdc 依赖面变大**：无环（已核实），若 review 反对可退到独立小包 `internal/pglock`，默认按 issue 指向放 `internal/cdc`。
4. **federated harness 丢 2s unlock 超时**：共享实现用 background ctx，行为等价偏强，PR 里注明防 reviewer 误判回归。
5. **flusher_test 断言迁移**：not-locked 用例的 "release 不被调" 断言在新签名下变为 "unlock 为 nil 且无泄漏"，保持原语义覆盖。

## Verification（端到端）

```bash
make lint && make test
go test -v -tags=e2e ./internal/e2e_harness/production/ -run 'TestManifestReconcile|TestInit' -timeout 30m
go test -v -tags=e2e -short ./internal/e2e_harness/federated/... -timeout 15m
```

预期：manifest-reconcile e2e 中 initShaped 对象 past-grace 被 --gc 删除且 residual 清零（exit 语义自洽）；`TestInitUnderConcurrentMutation` 保持绿；flusher/init/cmd-tools 全部单测绿；lint 干净。
