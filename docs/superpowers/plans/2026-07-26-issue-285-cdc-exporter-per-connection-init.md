# Issue #285 — CDC DuckExporter per-connection init + 有界池 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 #245 的 per-connection init 模式应用到 `internal/cdc/duckdb_exporter.go` 的 `NewDuckExporter`，消除"session 级 DuckDB 初始化只落单条池连接 + 无界池"缺陷；同时按用户裁决把 init 机制抽成共享包 `internal/duckdbinit`，federated 同步迁移。

**Architecture:** 新建 `internal/duckdbinit` 承载 Step/Stmt 类型、`MakeConnInit` 钩子和 `ValidateS3Credential`（两包现有副本删除）。federated 的语句构建器保留在原包但改用共享类型（纯机械迁移，零行为变化）。cdc 侧把 `NewDuckExporter` 里 ~100 行的逐语句 ExecContext 改为：构建语句列表（fail-fast 凭据校验）→ `duckdb.NewConnector(dsn, connInit)` → `sql.OpenDB` → `SetMaxOpenConns(1)` → `PingContext`。

**Tech Stack:** Go, github.com/duckdb/duckdb-go/v2（`NewConnector` 每条新物理连接调用 connInitFn）, database/sql, zap。

## Context（为什么做）

`NewDuckExporter`（internal/cdc/duckdb_exporter.go:43-144）用 `sql.Open` + `db.ExecContext` 做 PRAGMA/INSTALL/LOAD/SET s3_* 初始化——这些是 **session（连接）级**语句，只落到池中一条任意连接；且从不调 `SetMaxOpenConns`，池默认**无界**。任何并发使用（`Runner.getOrCreateDuckExporter` 缓存 exporter 供并发 flush 复用，runner.go:234-271；`compaction.DuckMerger` 直接复用 `exporter.DB`）都可能懒开出**没有任何 S3 配置**的连接 → 与 #245 相同的 `in region ''` HTTP 404。`cmd/tools/manifest_reconcile.go:253` 已经手工 `SetMaxOpenConns(1)` workaround。#245 已在 federated 侧修完（PR #286, 437efc9），本 issue 是它 PR 里点名的 CDC 侧 follow-up。

**用户裁决（2026-07-26）：**
1. init 机制抽共享包（非 cdc 本地复制一份）。
2. exporter 池策略 = 硬编码 `SetMaxOpenConns(1)`，不加 CDCConfig 字段（YAGNI；6 个调用方全顺序使用；per-connection init 已让未来放开变安全）。

**已核实的关键事实：**
- federated 参考实现：`internal/federated/duckdb_conn.go` — `buildInitSteps`(:143) / `initStep`/`initStmt`(:123-137) / `makeConnInit`(:227) / `validateS3Credential`(:48)。step 语义：step 内一条失败跳过该 step 余下语句（INSTALL 失败不 LOAD），后续 step 照跑；所有执行失败 log+skip 不阻断连接。
- cdc 现状语句集（顺序）：PRAGMA memory_limit（若 DuckMemLimit≠""）→ PRAGMA threads（若 >0）→ INSTALL/LOAD postgres_scanner, httpfs, parquet → SET s3_access_key_id / s3_secret_access_key（非空时，先校验）→ SET s3_session_token（cfg.S3SessionToken 或 env AWS_SESSION_TOKEN 回落）→ SET s3_region → SET s3_endpoint（trim https:// 和 http:// 两种前缀）→ SET s3_use_ssl=true/false（**恒设**）→ SET s3_url_style='path'（仅 S3UsePath）。执行失败全部 Warnw+skip；凭据非法则 `db.Close()` + 返回错误。
- `validateS3Credential` 两包字面相同副本：`internal/cdc/helpers.go:196`、`internal/federated/duckdb_conn.go:48`（禁字符集 `'";\ ` 含空格）；repo 内无直接单测。
- cdc 包内 `validateS3Credential` 唯一调用方就是 duckdb_exporter.go（5 处）；federated 内调用在 buildS3Steps(:194,:201)。
- federated 测试引用点：`duckdb_conn_test.go:262-266`（`TestMakeConnInit_FailedInstallSkipsLoad`，用 `recordingExecer` fake）；#245 三个行为测试（First/All/ConcurrentConnectionsConfigured）以 `SELECT current_setting('s3_region')` 为断言锚点。
- `NewDuckExporter` 调用方 6 处，签名不变故均无需改动：flusher.go:105、init.go:106、runner.go:254（经 `newDuckExporterFn` seam）、cmd/tools/compactor.go:123、cmd/tools/manifest_reconcile.go:249（**:253 的手工 SetMaxOpenConns(1) 变冗余，须删**）、internal/e2e_harness/production/cdc.go:199。
- `sql.Open("duckdb", ...)` 生产代码仅此一处；修复后全仓生产代码无裸 sql.Open duckdb。
- 每次导出 SQL 内嵌的 `PRAGMA memory_limit; ATTACH IF NOT EXISTS ... AS pg_db` 与 COPY 在**同一次 ExecContext**（单连接内的多语句批）执行，连接本地性天然满足，不改。
- 现有 cdc 单测对 NewDuckExporter 初始化路径**零覆盖**（仅 SQL 构建测试）。

**刻意的行为收紧（PR 里要点名）：** 现状下 DuckDBPath 不可用时构造照样成功（Exec 失败只 Warnw），首次导出才炸；新实现加 `PingContext`（5s）后构造期 fail-fast。与 federated 行为对齐。

## Global Constraints

- 源文件 ≤500 行、函数 ≤100 行（coding-standard.md）。
- 错误必须带上下文包装 `fmt.Errorf("...: %w", err)`，禁止裸 `return err`。
- lint 用仓库钉住的 golangci-lint v1.64.8（`make lint`），不得升级。
- 单测命令模板：`GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test <pkg> -run <Name> -v`。
- 子代理禁用 Sonnet 5；不能用 Haiku 的任务用 Opus 4.8。
- PR 不自动合并；PR 引用 `Closes #285`。
- federated 迁移必须**零行为变化**——现有 federated 测试套件是回归门。

## File Structure

- Create: `internal/duckdbinit/duckdbinit.go`（~90 行：Stmt/Step、SingleStmtStep、ExtensionStep、ValidateS3Credential、MakeConnInit）
- Create: `internal/duckdbinit/duckdbinit_test.go`
- Modify: `internal/federated/duckdb_conn.go`（删 :44-56 validateS3Credential、:123-137 类型、:227-242 makeConnInit；构建器改用共享类型）
- Modify: `internal/federated/duckdb_conn_test.go`（:266 调用点 + import）
- Create: `internal/cdc/duckdb_exporter_init.go`（语句构建器，~110 行）
- Create: `internal/cdc/duckdb_exporter_init_test.go`
- Modify: `internal/cdc/duckdb_exporter.go`（NewDuckExporter :43-144 重写为 ~30 行；import 调整）
- Modify: `internal/cdc/helpers.go`（删 :192-204 validateS3Credential）
- Modify: `cmd/tools/manifest_reconcile.go`（删 :253）
- Create: `docs/superpowers/plans/2026-07-26-issue-285-cdc-exporter-per-connection-init.md`（本计划落库副本）

---

### Task 0: Worktree 与分支

- [ ] **Step 1:** 按仓库惯例清理已合并分支、fast-forward main，然后建 worktree/分支 `fix/issue-285-cdc-duckdb-per-connection-init`（用 superpowers:using-git-worktrees；后续所有命令在 worktree 内，子代理须显式路径 + pwd 守卫）。
- [ ] **Step 2:** 把本计划落一份到 `docs/superpowers/plans/2026-07-26-issue-285-cdc-exporter-per-connection-init.md`（随 Task 1 一起提交）。

### Task 1: 共享包 `internal/duckdbinit`（含单测）

**Files:**
- Create: `internal/duckdbinit/duckdbinit.go`
- Test: `internal/duckdbinit/duckdbinit_test.go`

**Interfaces（后续 Task 依赖）:**
- `type Stmt struct { SQL, Label string }`
- `type Step struct { Stmts []Stmt }`
- `func SingleStmtStep(sql, label string) Step`
- `func ExtensionStep(ext string) Step` — INSTALL+LOAD 成对一个 Step
- `func ValidateS3Credential(name, value string) error` — 禁字符集 `'";\ `（含空格），与两包现有副本逐字同语义
- `func MakeConnInit(steps []Step, log *zap.SugaredLogger) func(driver.ExecerContext) error` — 失败 Warnw+跳过该 Step 余下语句，永不返回 error

- [ ] **Step 1: 写包实现**

```go
// Package duckdbinit builds and applies the session-scoped initialization
// (INSTALL/LOAD/SET/PRAGMA) that every pooled DuckDB connection must run on
// open. Session-scoped statements issued through the pool reach only one
// arbitrary connection (issues #245, #285); the connector init hook returned
// by MakeConnInit runs them for each new physical connection instead.
package duckdbinit

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// Stmt is a single statement executed on every new physical DuckDB connection.
type Stmt struct {
	SQL   string
	Label string
}

// Step groups statements that depend on each other: when one fails, the
// step's remaining statements are skipped (an extension whose INSTALL fails
// must not be LOADed), while later steps still run.
type Step struct {
	Stmts []Stmt
}

// SingleStmtStep wraps one independent statement in its own step.
func SingleStmtStep(sql, label string) Step {
	return Step{Stmts: []Stmt{{SQL: sql, Label: label}}}
}

// ExtensionStep pairs an extension's INSTALL and LOAD into one step, so a
// failed INSTALL skips that extension's LOAD.
func ExtensionStep(ext string) Step {
	return Step{Stmts: []Stmt{
		{SQL: fmt.Sprintf("INSTALL %s;", ext), Label: "install " + ext},
		{SQL: fmt.Sprintf("LOAD %s;", ext), Label: "load " + ext},
	}}
}

// ValidateS3Credential checks that an S3 credential value is safe to embed in
// a DuckDB SET statement. DuckDB's PRAGMA/SET does not support parameterized
// queries, so the value is checked against a denylist of characters instead.
// Rejected characters: single-quote ('), double-quote ("), semicolon (;),
// backslash (\), and space.
func ValidateS3Credential(name, value string) error {
	const forbidden = `'";\ `
	for _, ch := range forbidden {
		if strings.ContainsRune(value, ch) {
			return fmt.Errorf("S3 credential %q contains forbidden character %q; DuckDB SET does not support parameterized queries", name, string(ch))
		}
	}
	return nil
}

// MakeConnInit returns the connector init hook the driver runs for every new
// physical connection. Failed statements are logged and skipped so a degraded
// init never blocks the connection; construction-time errors are limited to
// credential validation done by the statement builders.
func MakeConnInit(steps []Step, log *zap.SugaredLogger) func(driver.ExecerContext) error {
	return func(execer driver.ExecerContext) error {
		for _, step := range steps {
			for _, s := range step.Stmts {
				// The driver binds the Connect context to the connection while
				// this hook runs; statements are literal SQL, no NamedValue args.
				if _, err := execer.ExecContext(context.Background(), s.SQL, nil); err != nil {
					log.Warnw("duckdb: connection init step failed", "step", s.Label, "err", err)
					break
				}
			}
		}
		return nil
	}
}
```

（ValidateS3Credential 的错误消息与现有两副本**逐字一致**——federated 有测试断言 `InvalidS3CredentialFailsFast` 的错误文本路径。）

- [ ] **Step 2: 写单测**

```go
package duckdbinit_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// recordingExecer records every attempted statement and fails the one
// matching failOn.
type recordingExecer struct {
	executed []string
	failOn   string
}

func (r *recordingExecer) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	r.executed = append(r.executed, query)
	if query == r.failOn {
		return nil, errors.New("injected init failure")
	}
	return driver.RowsAffected(0), nil
}

func TestMakeConnInit_FailedStmtSkipsRestOfStepOnly(t *testing.T) {
	steps := []duckdbinit.Step{
		duckdbinit.ExtensionStep("bad_ext"),
		duckdbinit.SingleStmtStep("SET s3_region='us-test-1';", "set s3_region"),
	}
	execer := &recordingExecer{failOn: "INSTALL bad_ext;"}
	require.NoError(t, duckdbinit.MakeConnInit(steps, zap.NewNop().Sugar())(execer))

	require.Contains(t, execer.executed, "INSTALL bad_ext;")
	require.NotContains(t, execer.executed, "LOAD bad_ext;", "failed INSTALL must skip the LOAD in the same step")
	require.Contains(t, execer.executed, "SET s3_region='us-test-1';", "later steps must still run")
}

func TestValidateS3Credential(t *testing.T) {
	for _, bad := range []string{"a'b", `a"b`, "a;b", `a\b`, "a b"} {
		require.Error(t, duckdbinit.ValidateS3Credential("s3_region", bad), "value %q must be rejected", bad)
	}
	require.NoError(t, duckdbinit.ValidateS3Credential("s3_region", "us-east-1"))
}
```

- [ ] **Step 3: 跑测试确认全绿**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/duckdbinit -v`
Expected: PASS（新包，先写测试再实现的 compile-red 意义不大，直接同 commit）

- [ ] **Step 4: Commit**

```bash
git add internal/duckdbinit/ docs/superpowers/plans/2026-07-26-issue-285-cdc-exporter-per-connection-init.md
git commit -m "refactor: #285 extract shared DuckDB per-connection init into internal/duckdbinit"
```

### Task 2: federated 迁移到共享包（零行为变化）

**Files:**
- Modify: `internal/federated/duckdb_conn.go`
- Modify: `internal/federated/duckdb_conn_test.go`

**Interfaces:**
- Consumes: Task 1 的 `duckdbinit.Step/SingleStmtStep/ExtensionStep/ValidateS3Credential/MakeConnInit`
- Produces: 无新接口；`NewDuckDBClient(Context)` 签名与行为不变

- [ ] **Step 1: duckdb_conn.go 机械替换**

1. 删 `validateS3Credential`（:44-56）、`initStmt`/`initStep`/`makeSingleStmtStep`（:123-137）、`makeConnInit`（:224-242）。
2. 构建器签名与内部调用替换（逻辑逐字保留，只换类型/函数名）：
   - `buildInitSteps(cfg) ([]initStep, error)` → `([]duckdbinit.Step, error)`
   - `buildExtensionSteps`：`appendExt` 内改为 `steps = append(steps, duckdbinit.ExtensionStep(ext))`
   - `buildS3Steps`：`validateS3Credential(...)` → `duckdbinit.ValidateS3Credential(...)`；`makeSingleStmtStep(...)` → `duckdbinit.SingleStmtStep(...)`
   - `buildPragmaSteps` 同上
3. `NewDuckDBClientContext` :92：`duckdb.NewConnector(dsn, makeConnInit(steps))` → `duckdb.NewConnector(dsn, duckdbinit.MakeConnInit(steps, zap.S()))`（zap.S() 即原 makeConnInit 内的全局 logger，行为不变）。
4. import：删 `context`?（仍被其他函数用，保留）、删 `database/sql/driver`（若不再引用）；加 `github.com/lychee-technology/forma/internal/duckdbinit`。

- [ ] **Step 2: duckdb_conn_test.go 更新**

`TestMakeConnInit_FailedInstallSkipsLoad`（:258 附近）：`makeConnInit(steps)(execer)` → `duckdbinit.MakeConnInit(steps, zap.S())(execer)`；`recordingExecer` fake 保留在此文件（它测的是 federated 构建器 × 共享钩子的组合语义，与 Task 1 的纯包测试不重复——一个盯 builder 输出，一个盯钩子机制）。加 import。

- [ ] **Step 3: 回归验证**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/federated ./internal/duckdbinit ./factory && make lint`
Expected: 全 PASS（federated 全套含 #245 三个行为测试是本 Task 的回归门）

- [ ] **Step 4: Commit**

```bash
git add internal/federated/
git commit -m "refactor(federated): #285 migrate duckdb conn init to shared internal/duckdbinit"
```

### Task 3: cdc 红测试（先失败）

**Files:**
- Create: `internal/cdc/duckdb_exporter_init_test.go`（仅行为测试；builder 单测在 Task 4 加，避免 compile-red 污染全包）

**Interfaces:**
- Consumes: 现有 `NewDuckExporter(ctx, cfg, s3AccessKey, s3Secret, logger)` 签名（不变）

- [ ] **Step 1: 写行为测试**

```go
package cdc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newExporterInitTestConfig() CDCConfig {
	return CDCConfig{
		DuckDBPath:   "", // :memory:
		S3Region:     "us-test-1",
		S3Endpoint:   "http://127.0.0.1:9000",
		S3UseSSL:     false,
		S3UsePath:    true,
		QueryTimeout: 5 * time.Second,
	}
}

// Session-scoped S3 settings must be present on every physical connection,
// not only the one the constructor happened to initialize (#285, same class
// as #245). SetMaxIdleConns(0) discards released connections so each
// subsequent query runs on a brand-new physical connection.
func TestNewDuckExporter_FreshConnectionsConfigured(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	ctx := context.Background()
	exp, err := NewDuckExporter(ctx, newExporterInitTestConfig(), "AKIDEXAMPLE", "testsecretvalue", zap.NewNop())
	require.NoError(t, err)
	defer exp.DB.Close()

	requireS3Region := func() {
		var region string
		require.NoError(t, exp.DB.QueryRowContext(ctx, "SELECT current_setting('s3_region')").Scan(&region))
		require.Equal(t, "us-test-1", region)
	}
	requireS3Region() // the connection the constructor initialized

	exp.DB.SetMaxIdleConns(0)
	for i := 0; i < 3; i++ {
		requireS3Region() // red pre-fix: fresh connections carry no session SETs
	}
}

// The exporter pool must be bounded (#285: sql.Open default is unlimited).
func TestNewDuckExporter_PoolBoundedToSingleConnection(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	exp, err := NewDuckExporter(context.Background(), newExporterInitTestConfig(), "AKIDEXAMPLE", "testsecretvalue", zap.NewNop())
	require.NoError(t, err)
	defer exp.DB.Close()
	require.Equal(t, 1, exp.DB.Stats().MaxOpenConnections)
}

// Credential validation must fail construction (regression: this already
// holds pre-fix, post-fix it fails before any connection is opened).
func TestNewDuckExporter_InvalidCredentialFailsFast(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	_, err := NewDuckExporter(context.Background(), newExporterInitTestConfig(), "bad'key", "testsecretvalue", zap.NewNop())
	require.Error(t, err)
}
```

（红锚点必须是 session 级 SET（s3_region）——#245 计划的教训：扩展 LOAD 状态可能实例级共享，不能作红锚点。`t.Setenv("AWS_SESSION_TOKEN", "")` 隔离开发机/CI 环境里的真实 token。）

- [ ] **Step 2: 跑红确认**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run 'TestNewDuckExporter_' -v`
Expected: `FreshConnectionsConfigured` FAIL（新物理连接上 current_setting('s3_region') 报错或非 us-test-1）；`PoolBoundedToSingleConnection` FAIL（MaxOpenConnections==0）；`InvalidCredentialFailsFast` PASS（既有行为，作回归钉）。

- [ ] **Step 3: Commit**

```bash
git add internal/cdc/duckdb_exporter_init_test.go
git commit -m "test(cdc): #285 red tests for per-connection init and bounded exporter pool"
```

### Task 4: cdc 绿实现

**Files:**
- Create: `internal/cdc/duckdb_exporter_init.go`
- Modify: `internal/cdc/duckdb_exporter.go`（重写 NewDuckExporter :43-144）
- Modify: `internal/cdc/helpers.go`（删 :192-204 validateS3Credential）
- Test: `internal/cdc/duckdb_exporter_init_test.go`（追加 builder 单测）

**Interfaces:**
- Consumes: `duckdbinit.*`（Task 1）；`NewDuckExporter` 公开签名**不变**
- Produces: `buildExporterInitSteps(cfg CDCConfig, s3AccessKey, s3Secret string) ([]duckdbinit.Step, error)`（包内私有）

- [ ] **Step 1: 写 `internal/cdc/duckdb_exporter_init.go`**

```go
package cdc

import (
	"fmt"
	"os"
	"strings"

	"github.com/lychee-technology/forma/internal/duckdbinit"
)

// buildExporterInitSteps assembles the PRAGMA/INSTALL/LOAD/SET statements
// every pooled exporter connection must run on open, preserving the exact
// statement set and order NewDuckExporter previously issued through the pool
// (#285). Credential validation happens here, so construction fails fast
// before any connection is opened.
func buildExporterInitSteps(cfg CDCConfig, s3AccessKey, s3Secret string) ([]duckdbinit.Step, error) {
	var steps []duckdbinit.Step
	if cfg.DuckMemLimit != "" {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA memory_limit='%s';", cfg.DuckMemLimit), "set memory_limit"))
	}
	if cfg.DuckThreads > 0 {
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("PRAGMA threads=%d;", cfg.DuckThreads), "set threads"))
	}
	// postgres_scanner first for postgres_query
	for _, ext := range []string{"postgres_scanner", "httpfs", "parquet"} {
		steps = append(steps, duckdbinit.ExtensionStep(ext))
	}
	s3Steps, err := buildExporterS3Steps(cfg, s3AccessKey, s3Secret)
	if err != nil {
		return nil, fmt.Errorf("build cdc duckdb s3 statements: %w", err)
	}
	return append(steps, s3Steps...), nil
}

func buildExporterS3Steps(cfg CDCConfig, s3AccessKey, s3Secret string) ([]duckdbinit.Step, error) {
	var steps []duckdbinit.Step
	credentials := []struct{ name, value string }{
		{"s3_access_key_id", s3AccessKey},
		{"s3_secret_access_key", s3Secret},
		// Temporary credentials (STS/assumed roles) are a key+secret+token
		// triple; without the token httpfs signs requests the store rejects
		// even though the SDK client on the same credentials works.
		{"s3_session_token", resolveExporterSessionToken(cfg)},
		{"s3_region", cfg.S3Region},
	}
	for _, c := range credentials {
		if c.value == "" {
			continue
		}
		if err := duckdbinit.ValidateS3Credential(c.name, c.value); err != nil {
			return nil, fmt.Errorf("invalid cdc duckdb s3 config: %w", err)
		}
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET %s='%s';", c.name, c.value), "set "+c.name))
	}
	if cfg.S3Endpoint != "" {
		ep := strings.TrimPrefix(strings.TrimPrefix(cfg.S3Endpoint, "https://"), "http://")
		if err := duckdbinit.ValidateS3Credential("s3_endpoint", ep); err != nil {
			return nil, fmt.Errorf("invalid cdc duckdb s3 config: %w", err)
		}
		steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET s3_endpoint='%s';", ep), "set s3_endpoint"))
	}
	sslVal := "true"
	if !cfg.S3UseSSL {
		sslVal = "false"
	}
	steps = append(steps, duckdbinit.SingleStmtStep(fmt.Sprintf("SET s3_use_ssl=%s;", sslVal), "set s3_use_ssl"))
	if cfg.S3UsePath {
		steps = append(steps, duckdbinit.SingleStmtStep("SET s3_url_style='path';", "set s3_url_style"))
	}
	return steps, nil
}

func resolveExporterSessionToken(cfg CDCConfig) string {
	if cfg.S3SessionToken != "" {
		return cfg.S3SessionToken
	}
	return os.Getenv("AWS_SESSION_TOKEN")
}
```

注意与现状的两处刻意差异（其余逐字保序）：(a) 现状凭据校验错误**裸返回**，新代码按规范包装；federated 的 `InvalidS3CredentialFailsFast` 类断言若查错误文本子串仍能命中（消息保留在 `%w` 链里）。(b) 校验时机从"逐条 SET 前"提到"构造语句列表时"——语义相同（都在构造期 fail-fast）。

- [ ] **Step 2: 重写 `NewDuckExporter`（duckdb_exporter.go:42-144 整段替换）**

```go
// NewDuckExporter opens a DuckDB pool whose every physical connection
// self-configures (pragmas, extensions, S3 session settings) via a connector
// init hook — session-scoped statements issued through the pool reach only
// one arbitrary connection (#285, same class as #245). Init statement
// failures are logged and skipped, never blocking the connection;
// construction fails only on credential validation or ping.
func NewDuckExporter(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret string, logger *zap.Logger) (*DuckExporter, error) {
	steps, err := buildExporterInitSteps(cfg, s3AccessKey, s3Secret)
	if err != nil {
		return nil, fmt.Errorf("build duckdb exporter init statements: %w", err)
	}
	connector, err := duckdb.NewConnector(cfg.DuckDBPath, duckdbinit.MakeConnInit(steps, logger.Sugar()))
	if err != nil {
		return nil, fmt.Errorf("open duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)
	// Exports run sequentially and file-backed DuckDB is effectively
	// single-writer; per-connection init keeps a larger pool safe if ever
	// needed, but nothing needs one today (#285).
	db.SetMaxOpenConns(1)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb: %w", err)
	}
	return &DuckExporter{DB: db, Logger: logger}, nil
}
```

import 调整（duckdb_exporter.go 顶部）：`_ "github.com/duckdb/duckdb-go/v2"` → 具名 `duckdb "github.com/duckdb/duckdb-go/v2"`；删 `os`（移入 init 文件）；加 `"github.com/lychee-technology/forma/internal/duckdbinit"`。`cfg.DuckDBPath` 为空时 `NewConnector("")` 即 :memory:，与 `sql.Open("duckdb", "")` 等价（federated/#245 已验证该驱动路径）。具名 import 的包 init 照样注册 `"duckdb"` driver，cdc 各测试文件里的裸 `sql.Open("duckdb", ...)` 不受影响。

- [ ] **Step 3: 删 `internal/cdc/helpers.go:192-204` 的 `validateS3Credential`**（包内已无调用方；`strings` import 若仅它使用则一并清理——helpers.go 其他函数大概率还在用，以编译器为准）。

- [ ] **Step 4: 追加 builder 单测到 `duckdb_exporter_init_test.go`**

```go
func TestBuildExporterInitSteps_FullConfigStatementSet(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	cfg := CDCConfig{
		DuckMemLimit: "1GB", DuckThreads: 2,
		S3Region: "us-test-1", S3Endpoint: "https://s3.example.com",
		S3UseSSL: true, S3UsePath: true, S3SessionToken: "tok123",
	}
	steps, err := buildExporterInitSteps(cfg, "AKID", "secretvalue")
	require.NoError(t, err)
	var sqls []string
	for _, st := range steps {
		for _, s := range st.Stmts {
			sqls = append(sqls, s.SQL)
		}
	}
	require.Equal(t, []string{
		"PRAGMA memory_limit='1GB';",
		"PRAGMA threads=2;",
		"INSTALL postgres_scanner;", "LOAD postgres_scanner;",
		"INSTALL httpfs;", "LOAD httpfs;",
		"INSTALL parquet;", "LOAD parquet;",
		"SET s3_access_key_id='AKID';",
		"SET s3_secret_access_key='secretvalue';",
		"SET s3_session_token='tok123';",
		"SET s3_region='us-test-1';",
		"SET s3_endpoint='s3.example.com';",
		"SET s3_use_ssl=true;",
		"SET s3_url_style='path';",
	}, sqls)
}

func TestBuildExporterInitSteps_SessionTokenEnvFallback(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "envtok")
	steps, err := buildExporterInitSteps(newExporterInitTestConfig(), "AKID", "secretvalue")
	require.NoError(t, err)
	var sqls []string
	for _, st := range steps {
		for _, s := range st.Stmts {
			sqls = append(sqls, s.SQL)
		}
	}
	require.Contains(t, sqls, "SET s3_session_token='envtok';")
}

func TestBuildExporterInitSteps_MinimalConfigOmitsOptionalStatements(t *testing.T) {
	t.Setenv("AWS_SESSION_TOKEN", "")
	steps, err := buildExporterInitSteps(CDCConfig{}, "", "")
	require.NoError(t, err)
	var sqls []string
	for _, st := range steps {
		for _, s := range st.Stmts {
			sqls = append(sqls, s.SQL)
		}
	}
	// no pragmas, no SETs except the unconditional s3_use_ssl
	require.Equal(t, []string{
		"INSTALL postgres_scanner;", "LOAD postgres_scanner;",
		"INSTALL httpfs;", "LOAD httpfs;",
		"INSTALL parquet;", "LOAD parquet;",
		"SET s3_use_ssl=false;",
	}, sqls)
}
```

- [ ] **Step 5: 跑绿 + 全包回归**

Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc -run 'TestNewDuckExporter_|TestBuildExporterInitSteps_' -v`
Expected: 全 PASS
Run: `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/cdc ./internal/federated ./internal/duckdbinit ./internal/compaction ./factory && make lint`
Expected: 全 PASS（compaction 单测覆盖 DuckMerger 复用路径）

- [ ] **Step 6: Commit**

```bash
git add internal/cdc/
git commit -m "fix(cdc): #285 per-connection DuckDB init and bounded pool for DuckExporter"
```

### Task 5: 清理冗余 workaround + 全量单测

**Files:**
- Modify: `cmd/tools/manifest_reconcile.go:253`

- [ ] **Step 1:** 删 `exporter.DB.SetMaxOpenConns(1)`（:253）——构造器现在自设；该行是 #285 前的手工钉。
- [ ] **Step 2:** Run: `make test && make lint` — Expected: 全 PASS。
- [ ] **Step 3: Commit**

```bash
git add cmd/tools/manifest_reconcile.go
git commit -m "chore(tools): #285 drop redundant manual SetMaxOpenConns pin in manifest reconcile"
```

### Task 6: e2e 回归门（需 Docker）

- [ ] **Step 1（焦点）:** `go test -v ./internal/e2e_harness/production -run 'CDC|Flush|Compaction' -timeout=15m`（覆盖 flusher/init/compactor 真实走 exporter 的路径；具体测试名以 `grep -l "RunCompaction\|flush" internal/e2e_harness/production/*_test.go` 为准）。
- [ ] **Step 2（全量）:** `make test-e2e-production` — Expected: 0 失败。已知坑：并发跑 e2e 与单测会雪崩假失败（#187 教训），串行跑；`TestConcurrentFlushSnapshot`/`UpdateBeforeExport` 偶发同毫秒 changed_at flake 已归档 #276，非本改动回归。
- [ ] **Step 3:** 若 e2e 全绿，进入交付。

### Task 7: 交付

- [ ] **Step 1:** 推分支、开 PR：标题 `fix(cdc): #285 per-connection DuckDB init and bounded pool for DuckExporter`，body 含 `Closes #285`，并写明：
  - 共享包裁决与 federated 迁移零行为变化（federated 套件为证）；
  - 池策略裁决 = 硬编码 1（YAGNI，per-connection init 已让未来放开安全）；
  - **刻意收紧**：构造期 PingContext fail-fast（原先坏 DuckDBPath 只 Warnw、首次导出才炸）；
  - 每导出 SQL 内嵌 ATTACH/PRAGMA 与 COPY 同一 ExecContext 批、连接本地性不受影响（issue 里点名的 :270/:286 两处）；
  - compaction.DuckMerger 与 Runner 缓存 exporter 的并发复用自动受益。
- [ ] **Step 2:** 不自动合并；等评审。合并后：清 worktree/分支、FF main、#285 收尾评论并关闭（若 Closes 未自动关）。

## 范围外（PR 里说明）

- `Runner.duckExporters` 缓存 exporter 终身不关（memoize 终身）已有 follow-up #326，不在本 PR 处理。
- CDCConfig 不加池尺寸旋钮（用户裁决 YAGNI）；federated 侧 `MaxConnections` 旋钮不动。

## 风险清单

- INSTALL 每物理连接重复执行：幂等，扩展落盘后仅元数据检查；`SetMaxOpenConns(1)` + 默认 idle 池下每物理连接一生只跑一次。与 #245 相同结论。
- 单测里 `SetMaxIdleConns(0)` 使每次查询新开物理连接并重跑 init：INSTALL 失败也只 log+skip，红锚点 s3_region 不依赖网络。
- PingContext 收紧：坏 DuckDBPath 从"构造成功、导出才炸"变"构造即失败"——刻意，PR 点名。
- federated 迁移碰联邦查询路径：纯机械类型替换，federated 全套单测 + production e2e 兜底。
- `helpers.go` 删函数后的 import 悬空：以编译器/lint 为准清理。

## 验证摘要

1. Task 3 后：两红一绿（FreshConnectionsConfigured / PoolBounded 红，InvalidCredential 绿）。
2. Task 4 后：`go test ./internal/cdc ./internal/federated ./internal/duckdbinit ./internal/compaction ./factory` + `make lint` 全绿。
3. Task 5 后：`make test` 全绿。
4. Task 6：`make test-e2e-production` 0 失败（生产 harness 是 e2e 闸门——#301 教训）。
