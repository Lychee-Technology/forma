# Issue #204 — list 属性端到端支持 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> 批准后请将本计划另存为 `docs/superpowers/plans/2026-07-18-issue-204-list-attributes-e2e.md`（仓库惯例），并在独立 worktree 中执行。

**Goal:** 让 `list`（数组）属性完整跨三层往返：写路径按元素持久化（items 类型定型）、CDC 导出以 DuckDB LIST 列保留 `array_indices` 顺序、federated 读侧按位置重建 `array_indices`，最终 `TestListAttributeRoundTrip` 的 round-trip 规格测试去 skip 后全绿。

**Architecture:** 三个阻塞点按依赖顺序修：(1) 元数据下沉 `items_type` → 写路径 `populateTypedValue` 增加 list 分支按元素类型委托既有 typed 分支；(2) CDC 导出 `buildEAVQuery` 带上 `array_indices`，list 属性的 pivot 从 `MAX(CASE…)` 换成 `list(… ORDER BY …) FILTER (…)` 聚合 → parquet 存 `VARCHAR[]`（或 items 对应类型的 LIST）；(3) federated 热侧 pivot 同样出 LIST 保证 UNION ALL 两腿类型统一，`attributes_json` 用 `list_transform((x,i)->…)` 按位置重建每元素对象。下游 Go 侧（`model.ParseEAVAttribute`、`federated/merge.go` 按 `(AttrID, ArrayIndices)` 去重）已 index-aware，无需改动。

**Tech Stack:** Go, PostgreSQL (EAV), DuckDB 1.x (duckdb-go v2.5.6), Parquet/S3, testcontainers e2e。

## Global Constraints

- 源文件 ≤500 行、函数 ≤100 行（coding-standard.md）；`duckdb_schema_projection.go` 现约 464 行，新逻辑必须抽 helper。
- 所有错误带上下文包装；写路径校验错误必须包 `forma.ErrInvalidInput`，错误信息含属性名与 attrID（既有 `handleConversionError` 已满足）。
- 严格 TDD：每个 task 红测试先行。
- 单测命令统一用 `GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test …`。
- cdc `castEAVValue` 与 sqlgen `buildEAVPivotExpr` 必须保持镜像（两处已有互指注释，`duckdb_exporter.go:396-403` / `duckdb_schema_projection.go:227-233`），本次改动两侧同步。
- lint 钉 golangci-lint v1.64.8（`make lint`），勿动。
- PR 不自动合并；PR body 关联 `#204`。

## 用户已定的三个取舍（binding）

1. **items 类型本次下沉**：`AttributeMetadata` 增加 `ItemsType`，parser/generator 全链路；缺省 `text`。
2. **`tags` 加入共享 `e2e_wide` fixture**（attrID 18 已核实空闲，最大现有 ID=17），同步扩展 `wideParquetTypes`（`"tags": "VARCHAR[]"`）。
3. **benchmark 投影硬编码一并对齐**（`duckdb_benchmark_projection.go:283`）。

## 关键背景事实（已逐一核实）

- 写路径：`flattenToAttributes`（`internal/transform/transformer.go:255-323`）已把数组拆成逐元素记录，`ArrayIndices` 由 `joinIndices` 编码为逗号分隔十进制（平铺数组即 `"0","1","2"`；嵌套为 `"0,1"`），且在调用 `populateTypedValue` **之前**已赋好（:310→:313）。唯一拒绝点是 `internal/transform/typed_value.go:62-63` 的 default 分支。
- `eav_data` PK 含 `array_indices`（`cmd/tools/init_db.go:175-183`），逐元素多行天然支持；EAV 只有 `value_text`/`value_numeric` 两列（int64 sidecar 在 EAV 持久化前清空）。
- 热 OLTP 读已完整支持数组重建（`advanced_query_template.go:112-130` JSON_AGG 带真实 `array_indices`；`FromAttributes` 用 `parseIndices`+`setValueAtPath` 按整数位置重建，行序无关）。
- CDC 导出：`buildEAVQuery`（`internal/cdc/export_sql_builder.go:80-89`）丢弃 `array_indices`；pivot 在 `buildSchemaDrivenProjection`（:143-146）`MAX(CASE…)` 折叠；聚合发生在 DuckDB 侧（`buildEAVAggregationSQL` :158-169，`postgres_query` 外层 GROUP BY row_id），故 `list()` 聚合可用。
- Compaction（`internal/compaction/merge_sql.go:44-73`）列/类型无关，`SELECT * … union_by_name=true` + 按 row_id rn=1 —— **LIST 列整行透传；"每元素一 parquet 行"会被 compaction 折成单行，是不可选项**。Manifest 只统计 row_id/changed_at min-max，不受影响。
- Federated：热腿 pivot `buildEAVPivotExpr`（`internal/sqlgen/duckdb_schema_projection.go:234-252`）同样 MAX 折叠；`hot_vals` CTE（`advanced_query_template_duckdb.go:67-74`）postgres_scan 全列可见，模板本身无需改（pivot 表达式经 `{{.EAVPivotSelect}}` 注入）。`attributes_json` 硬编码在 `buildOuterSelect` :341-343（`'array_indices': ''`）。`duckDBAttrCast`（:411-430）default 会把 LIST `CAST(… AS VARCHAR)` 字符串化——必须加显式 list 分支。LWW rn=1 按 row_id 粒度，胜出行内的 list 元素不受去重影响。
- 下游 Go 已就绪：`model.ParseEAVAttribute`（`internal/model/record_parsing.go:54-56`）读 `array_indices`；`federated/merge.go:187` 按 `(AttrID|ArrayIndices)` 键去重（今日 `''` 导致全部元素折成一键，恢复真实 indices 后自然全存活）。
- 谓词：list 类型在 predicate normalizer 已被 default 分支干净拒绝（`predicate_normalizer.go` :230/:283），list-containment 不在本 issue 范围，**无需改动**。
- e2e 规格：`TestListAttributeRoundTrip`（`internal/e2e_harness/production/list_roundtrip_e2e_test.go:48-102`）目前**钉的是拒绝契约**，修复后必须重写为直接跑 `runListRoundTripSpec`（:110-196，断言 3 EAV 行 / 热读 3 元素 / base+delta 两 tier ≥3 值 / federated 每记录 3 元素）；其 `recoverTagElements` 已兼容 LIST 或逐行两种物理形态。
- 生成器现状：`cmd/tools/generate_attributes.go:293-299` 把原始类型数组扁平化为**元素类型**（从不产出 `list`）；`generateAttributesJSON` 再生成时会覆写 `valueType`（:125）。**已上线 schema（`cmd/server/schemas/*.json` 的 attendees/tags/phones 等）本 PR 一律不再生成**——翻转会把存量 parquet 标量列变 LIST，`union_by_name` 类型冲突（迁移风险，见 Risks）。
- oracle：`oracle.go normalizeValue`（:269-290）default 分支对 `list` 会报错，但 list 规格测试不走 oracle、其他 e2e_wide 测试不写 `tags`——Task 9 有验证点。

---

### Task 0: `ItemsType` 元数据管道

**Files:**
- Modify: `schema_registry.go:132-141`（`AttributeMetadata`）
- Modify: `internal/schemameta/parser.go:12-43`
- Test: `internal/schemameta/parser_test.go`（追加，勿覆写）

**Interfaces:**
- Produces: `AttributeMetadata.ItemsType forma.ValueType`；`func (m AttributeMetadata) EffectiveItemsType() ValueType`（后续所有 task 消费）。

- [ ] **Step 1: 红测试**（parser 测试文件追加）

```go
func TestParseAttributeMetadata_ItemsType(t *testing.T) {
	meta, err := parseAttributeMetadata("tags", map[string]any{
		"attributeID": float64(18), "valueType": "list", "items_type": "integer",
	}, "test.json")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if meta.ItemsType != forma.ValueTypeInteger {
		t.Errorf("ItemsType = %q, want integer", meta.ItemsType)
	}

	// 缺省：ItemsType 为空，EffectiveItemsType() 回落 text
	meta2, err := parseAttributeMetadata("tags", map[string]any{
		"attributeID": float64(18), "valueType": "list",
	}, "test.json")
	if err != nil {
		t.Fatalf("parse default: %v", err)
	}
	if meta2.ItemsType != "" || meta2.EffectiveItemsType() != forma.ValueTypeText {
		t.Errorf("default ItemsType = %q / effective %q, want ''/text", meta2.ItemsType, meta2.EffectiveItemsType())
	}

	// 拒绝嵌套 list 容器
	if _, err := parseAttributeMetadata("tags", map[string]any{
		"attributeID": float64(18), "valueType": "list", "items_type": "list",
	}, "test.json"); err == nil {
		t.Error("items_type 'list' accepted, want error")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**（`ItemsType`/`EffectiveItemsType` 未定义，编译错）

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/schemameta -run TestParseAttributeMetadata_ItemsType -v
```

- [ ] **Step 3: 实现**

`schema_registry.go`（`ValueType` 字段之后加字段与方法）：

```go
	// ItemsType is the element value type when ValueType is ValueTypeList.
	// Empty means text (EffectiveItemsType).
	ItemsType ValueType `json:"items_type,omitempty"`
```

```go
// EffectiveItemsType returns the element type of a list attribute,
// defaulting to text when items_type is not declared.
func (m AttributeMetadata) EffectiveItemsType() ValueType {
	if m.ItemsType != "" {
		return m.ItemsType
	}
	return ValueTypeText
}
```

`internal/schemameta/parser.go`（`meta.ValueType = …` 之后）：

```go
	if itemsType, ok := attrData["items_type"].(string); ok && itemsType != "" {
		if forma.ValueType(itemsType) == forma.ValueTypeList {
			return forma.AttributeMetadata{}, fmt.Errorf(
				"invalid items_type 'list' for attribute %s in %s: nested lists are not supported", attrName, source)
		}
		meta.ItemsType = forma.ValueType(itemsType)
	}
```

- [ ] **Step 4: 测试转绿** （同 Step 2 命令，PASS）
- [ ] **Step 5: Commit** `feat(schemameta): #204 thread list items_type into AttributeMetadata`

---

### Task 1: 写路径 list 分支（Blocker 1）

**Files:**
- Modify: `internal/transform/typed_value.go:21-64`（switch 增加 list 分支；需 import `strings`）
- Test: `internal/transform/typed_value_test.go`（新建或追加既有 transform 测试）

**Interfaces:**
- Consumes: `meta.EffectiveItemsType()`（Task 0）。
- Produces: list 属性写入产生逐元素 `EAVRecord`（`ArrayIndices` "0".."n-1"，值列由 items 类型决定）。

- [ ] **Step 1: 红测试**

```go
func TestPopulateTypedValue_List(t *testing.T) {
	listMeta := forma.AttributeMetadata{AttributeName: "tags", AttributeID: 18, ValueType: forma.ValueTypeList}

	// 文本元素（缺省 items_type）落 value_text
	attr := model.EAVRecord{ArrayIndices: "0"}
	set, err := populateTypedValue(&attr, "tags", "alpha", listMeta)
	if err != nil || !set {
		t.Fatalf("text element: set=%v err=%v", set, err)
	}
	if attr.ValueText == nil || *attr.ValueText != "alpha" {
		t.Errorf("ValueText = %v, want alpha", attr.ValueText)
	}

	// 数值元素（items_type integer）落 value_numeric
	intMeta := listMeta
	intMeta.ItemsType = forma.ValueTypeInteger
	attr2 := model.EAVRecord{ArrayIndices: "1"}
	if _, err := populateTypedValue(&attr2, "tags", float64(7), intMeta); err != nil {
		t.Fatalf("int element: %v", err)
	}
	if attr2.ValueNumeric == nil || *attr2.ValueNumeric != 7 {
		t.Errorf("ValueNumeric = %v, want 7", attr2.ValueNumeric)
	}

	// 多维下标拒绝
	attr3 := model.EAVRecord{ArrayIndices: "0,1"}
	if _, err := populateTypedValue(&attr3, "tags", "x", listMeta); !errors.Is(err, forma.ErrInvalidInput) {
		t.Errorf("nested indices: err=%v, want ErrInvalidInput", err)
	}

	// 标量载荷（indices 为空）拒绝
	attr4 := model.EAVRecord{ArrayIndices: ""}
	if _, err := populateTypedValue(&attr4, "tags", "scalar", listMeta); !errors.Is(err, forma.ErrInvalidInput) {
		t.Errorf("scalar payload: err=%v, want ErrInvalidInput", err)
	}
}
```

另加一条走 `Transformer.ToAttributes` 全流程的表驱动用例（stub registry 增加一个 `valueType: list` 属性）：`[]any{"alpha","beta","gamma"}` → 3 条记录、indices "0","1","2"、`value_text` 逐一对应。

- [ ] **Step 2: 跑测试确认失败**（报 `unsupported value type 'list'`）

```bash
GOCACHE=$PWD/.gocache GOFLAGS=-buildvcs=false go test ./internal/transform -run 'TestPopulateTypedValue_List|TestTransformer_ToAttributes' -v
```

- [ ] **Step 3: 实现**（`typed_value.go`，bool 分支之后、default 之前）

```go
	case forma.ValueTypeList:
		// flattenToAttributes 已把数组拆成逐元素调用并写好 ArrayIndices；
		// 这里只需按 items 类型给单个标量元素定型。
		if attr.ArrayIndices == "" {
			return handleConversionError(fmt.Errorf("list attribute requires an array value, got scalar %v", value))
		}
		if strings.Contains(attr.ArrayIndices, ",") {
			return handleConversionError(fmt.Errorf("multi-dimensional array not supported for list attribute (indices %q)", attr.ArrayIndices))
		}
		elemMeta := meta
		elemMeta.ValueType = meta.EffectiveItemsType()
		if elemMeta.ValueType == forma.ValueTypeList {
			return handleConversionError(fmt.Errorf("items_type 'list' is not supported"))
		}
		return populateTypedValue(attr, attrName, value, elemMeta)
```

- [ ] **Step 4: 测试转绿**；顺跑 `go test ./internal/transform ./internal/... -count=1`（局部回归）
- [ ] **Step 5: Commit** `feat(transform): #204 accept list attributes, type elements by items_type`

---

### Task 2: 读侧定型对称（热 OLTP 读）

**Files:**
- Modify: `internal/transform/attribute_converter.go:159`（`FromEAVRecords` 调用点）
- Test: `internal/transform/attribute_converter_test.go`（追加）

- [ ] **Step 1: 红测试**：构造 `valueType: list, items_type: integer` 的 cache 与 3 条 `value_numeric` EAVRecord（indices "0","1","2"），断言 `FromEAVRecords` 返回的 `EntityAttribute.Value` 为 int/float 数值而非 text-fallback；再经 `FromAttributes` 重建为有序数组。
- [ ] **Step 2: 确认失败**（default fallback 对 numeric-only 记录仍能返回值，若断言值类型/数值精度则可红；若确实无法构造红场景，以"显式化"名义直接实现并在 commit message 注明）。
- [ ] **Step 3: 实现**（:159 附近）

```go
		meta := cache[attrName]
		vt := meta.ValueType
		if vt == forma.ValueTypeList {
			vt = meta.EffectiveItemsType()
		}
		attr, err := c.FromEAVRecord(record, vt)
```

- [ ] **Step 4: 转绿**：`go test ./internal/transform -v -run FromEAVRecords`
- [ ] **Step 5: Commit** `feat(transform): #204 read list elements via items_type instead of text fallback`

---

### Task 3: CDC 导出——array_indices + LIST 聚合（Blocker 2）

**Files:**
- Modify: `internal/cdc/export_sql_builder.go:80-89`（`buildEAVQuery`）、`:128-147`（`buildSchemaDrivenProjection`）
- Test: `internal/cdc/export_sql_shared_test.go`（追加）；新增 `internal/cdc/duckdb_list_syntax_test.go`（语法保险）

**Interfaces:**
- Consumes: `meta.EffectiveItemsType()`、既有 `castEAVValue`。
- Produces: list 属性的 parquet 列为 DuckDB LIST（text items → `VARCHAR[]`）。

- [ ] **Step 1: DuckDB 语法 spike（红/绿一步到位）**——新建 `duckdb_list_syntax_test.go`，用 in-memory DuckDB 执行本计划依赖的三个构造，作为长期语法守卫：

```go
// TestDuckDBListSyntaxSupport pins the DuckDB constructs the list export/read
// path depends on (#204): aggregate ORDER BY + FILTER, two-arg lambda with
// index, flatten of mixed typed/empty sublists.
func TestDuckDBListSyntaxSupport(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil { t.Fatalf("open duckdb: %v", err) }
	defer db.Close()

	var s string
	row := db.QueryRow(`SELECT to_json(list(v ORDER BY TRY_CAST(i AS BIGINT)) FILTER (WHERE k = 1))
		FROM (VALUES ('b','1',1),('a','0',1),('x','0',2)) t(v,i,k)`)
	if err := row.Scan(&s); err != nil { t.Fatalf("list ORDER BY + FILTER: %v", err) }
	if s != `["a","b"]` { t.Errorf("got %s, want [\"a\",\"b\"]", s) }

	row = db.QueryRow(`SELECT to_json(list_transform(['a','b'], (x, i) -> CAST(i - 1 AS VARCHAR) || ':' || x))`)
	if err := row.Scan(&s); err != nil { t.Fatalf("two-arg lambda: %v", err) }
	if s != `["0:a","1:b"]` { t.Errorf("got %s", s) }

	row = db.QueryRow(`SELECT to_json(list_filter(flatten([['a', NULL], [], ['b']]), x -> x IS NOT NULL))`)
	if err := row.Scan(&s); err != nil { t.Fatalf("flatten mixed: %v", err) }
	if s != `["a","b"]` { t.Errorf("got %s", s) }
}
```

若 two-arg lambda `(x, i) ->` 不被 v2.5.6 支持：fallback 为 `list_transform(range(1, len(col)+1), i -> …col[i]…)`，Task 5 的表达式随之替换。

- [ ] **Step 2: 红测试**（export_sql_shared_test.go 追加）：构造含 `{ValueType: list, ItemsType: text, AttributeID: 18}` 的 attrCache，断言：
  - `buildEAVQuery(...)` 输出含 `array_indices`；
  - `projection.eavAgg` 对 list 属性 == `list(CAST(value_text AS VARCHAR) ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = 18) AS tags`；
  - 标量属性仍是 `MAX(CASE WHEN attr_id = …)`（既有断言不动）。
- [ ] **Step 3: 确认失败**
- [ ] **Step 4: 实现**

`buildEAVQuery`：

```go
	return fmt.Sprintf(
		"SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric FROM %s WHERE schema_id = %d AND %s%s",
		eavData, schemaID, eavFilter, attrFilter,
	)
```

`buildSchemaDrivenProjection`（:143 处分支）：

```go
		if meta.ValueType == forma.ValueTypeList {
			elemMeta := meta
			elemMeta.ValueType = meta.EffectiveItemsType()
			eavAgg = append(eavAgg, fmt.Sprintf(
				"list(%s ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = %d) AS %s",
				castEAVValue(elemMeta), meta.AttributeID, alias))
		} else {
			castExpr := castEAVValue(meta)
			eavAgg = append(eavAgg, fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN %s END) AS %s", meta.AttributeID, castExpr, alias))
		}
		eavSelect = append(eavSelect, fmt.Sprintf("e.%s", alias))
		eavAttrIDs = append(eavAttrIDs, meta.AttributeID)
```

同步更新 `castEAVValue` 上方镜像注释（`duckdb_exporter.go:396-403`）说明 list 属性的元素 cast 走 items 类型。

- [ ] **Step 5: 转绿** `go test ./internal/cdc -v -run 'ExportSQL|DuckDBListSyntax'`；跑全包回归 `go test ./cdc ./internal/cdc`
- [ ] **Step 6: Commit** `feat(cdc): #204 export list attrs as DuckDB LIST, preserve array_indices order`

---

### Task 4: Federated 热侧 pivot LIST + cast 透传

**Files:**
- Modify: `internal/sqlgen/duckdb_schema_projection.go:234-252`（`buildEAVPivotExpr`）、`:411-430`（`duckDBAttrCast`）、`eavValueColumn` 附近新增 `eavElementCastExpr`
- Test: `internal/sqlgen` 既有投影渲染测试文件（追加）

**Interfaces:**
- Produces: `eavElementCastExpr(vt forma.ValueType) string`（Task 5 复用）；list 属性的 `EAVPivotSelect` 为 LIST 聚合，两腿（pg_source / parquet）类型均为 LIST(T)。

- [ ] **Step 1: 红测试**：schema 含 list 属性（items text）时：
  - `EAVPivotSelect` 含 `list(value_text ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = 18)`；
  - `duckDBAttrCast("tags", forma.ValueTypeList)` 返回 `tags`（无 CAST 包裹）；
  - 纯标量 schema 的渲染输出与现状逐字节一致（回归守卫）。
- [ ] **Step 2: 确认失败**
- [ ] **Step 3: 实现**

```go
// eavElementCastExpr renders the raw-column cast for one list element of the
// given items type. Must mirror cdc.castEAVValue applied to the same type so
// hot LIST elements type-unify with the parquet LIST column (#204, cf. #205).
func eavElementCastExpr(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeBool:
		return "(value_numeric <> 0)"
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "TRY_CAST(value_numeric AS BIGINT)"
	case forma.ValueTypeInteger:
		return "TRY_CAST(value_numeric AS INTEGER)"
	case forma.ValueTypeSmallInt:
		return "TRY_CAST(value_numeric AS SMALLINT)"
	case forma.ValueTypeNumeric:
		return "TRY_CAST(value_numeric AS DOUBLE)"
	default: // text / uuid
		return "value_text"
	}
}
```

（实现时与 `cdc.castEAVValue` 实际输出逐分支比对，以 cdc 侧为准修正上表。）

`buildEAVPivotExpr` 开头加：

```go
	if a.meta.ValueType == forma.ValueTypeList {
		return fmt.Sprintf("list(%s ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = %d)",
			eavElementCastExpr(a.meta.EffectiveItemsType()), a.attrID)
	}
```

`duckDBAttrCast` 加：

```go
	case forma.ValueTypeList:
		// LIST 列只被 attributes_json 消费；CAST(… AS VARCHAR) 会把它字符串化，必须透传。
		return attr
```

`buildPGProjection` 的 `ANY_VALUE(hot_vals.<attr>)`（:199-202）对 LIST 合法，无需改。

- [ ] **Step 4: 转绿** `go test ./internal/sqlgen -v`
- [ ] **Step 5: Commit** `feat(sqlgen): #204 hot-tier LIST pivot for list attrs, passthrough cast`

---

### Task 5: Federated attributes_json 按位置重建 array_indices

**Files:**
- Modify: `internal/sqlgen/duckdb_schema_projection.go:278-354`（`buildOuterSelect`；抽 helper `eavJSONPart` 防超 100 行）；`SchemaProjection` 需新增按属性名可查的 meta（如 `AttrMetas map[string]forma.AttributeMetadata` 或至少 items 类型 map），在 `UnifiedColumnTypes` 同处填充。
- Test: `internal/sqlgen` 投影渲染测试（追加）

**Interfaces:**
- Consumes: `eavElementCastExpr`（Task 4）、`EffectiveItemsType`。

- [ ] **Step 1: 红测试**：
  - 纯标量 schema：`OuterSelect` 与现状**逐字节一致**（不含 `flatten(`）——既有 golden 断言不许 churn；
  - 含 list 属性 schema：含 `flatten([`、`list_transform(tags, (x, i) ->`、`CAST(i - 1 AS VARCHAR)`，且 list 属性的对象不含 `'array_indices': ''`；标量属性对象仍为 `'array_indices': ''`。
- [ ] **Step 2: 确认失败**
- [ ] **Step 3: 实现**——`buildOuterSelect` 的 jsonParts 循环改为（有 list 属性时启用 flatten 形态，否则保持原样）：

标量属性 part（原对象包一层单元素 list）：

```
[CASE WHEN <unified> IS NOT NULL THEN {'schema_id': <S>, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': <A>, 'array_indices': '', 'value_text': <vt>, 'value_numeric': <vn>} END]
```

list 属性 part（text items 示例；numeric-family items 时 `value_text` 为 NULL、`value_numeric` 为 `CAST(x AS DOUBLE)`，bool items 为 `CAST(CAST(x AS INTEGER) AS DOUBLE)`）：

```
CASE WHEN <unified> IS NOT NULL THEN list_transform(<unified>, (x, i) -> {'schema_id': <S>, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': <A>, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': CAST(x AS VARCHAR), 'value_numeric': NULL}) ELSE [] END
```

合成：

```
to_json(list_filter(flatten([<part1>, <part2>, …]), x -> x IS NOT NULL))::TEXT AS attributes_json
```

Go 形态（抽 helper，示意）：

```go
func (sp *SchemaProjection) eavJSONPart(schemaID int16, attr string) string {
	unified := ParquetAttrColumn(attr)
	vt := sp.UnifiedColumnTypes[attr]
	if vt == forma.ValueTypeList {
		itemsVT := sp.itemsTypeFor(attr) // 新增的 meta 查询
		valueText, valueNumeric := "NULL", "NULL"
		if eavValueColumn(itemsVT) == "value_numeric" {
			if itemsVT == forma.ValueTypeBool {
				valueNumeric = "CAST(CAST(x AS INTEGER) AS DOUBLE)"
			} else {
				valueNumeric = "CAST(x AS DOUBLE)"
			}
		} else {
			valueText = "CAST(x AS VARCHAR)"
		}
		return fmt.Sprintf(
			"CASE WHEN %s IS NOT NULL THEN list_transform(%s, (x, i) -> {'schema_id': %d, 'row_id': CAST(row_id AS VARCHAR), 'attr_id': %d, 'array_indices': CAST(i - 1 AS VARCHAR), 'value_text': %s, 'value_numeric': %s}) ELSE [] END",
			unified, unified, schemaID, sp.attrIDForName(attr), valueText, valueNumeric)
	}
	// 标量：现有对象字符串原样，包 [] —— 见上方 SQL 形态
	…
}
```

同步更新 :317-324 的契约注释（说明 list 属性逐元素对象、indices 来自位置）。

- [ ] **Step 4: 转绿** `go test ./internal/sqlgen -v`；再跑 `go test ./internal/federated/...`（单测层回归）
- [ ] **Step 5: Commit** `feat(sqlgen): #204 reconstruct per-element array_indices in attributes_json`

---

### Task 6: benchmark 投影对齐

**Files:**
- Modify: `internal/sqlgen/duckdb_benchmark_projection.go:283` 附近
- Test: 该文件对应的渲染测试（追加）

- [ ] **Step 1: 红测试**：benchmark 投影对标量属性输出保持现状（`'array_indices', ''`）；若声明 list 属性则输出 `list_transform` 逐元素形态（与 Task 5 同构，json_object 风格）。
- [ ] **Step 2: 实现**：在 benchmark 的 EAV JSON 构造处加同样的 list 分支（复用/镜像 Task 5 的表达式；benchmark 数据集今日无 list 属性，行为不变，属防御性对齐），并留注释指向 #204。
- [ ] **Step 3: 转绿 + Commit** `fix(sqlgen): #204 benchmark projection emits positional array_indices for list attrs`

---

### Task 7: 生成器产出 `list` + `items_type`

**Files:**
- Modify: `cmd/tools/generate_attributes.go:15-19`（`attributeSpec`）、`:293-299`（原始数组分支）、`:120-145`（merge/new 两处 map 落盘）
- Test: `cmd/tools/generate_attributes_test.go:340-380` 附近追加

**注意：本 PR 不再生成 `cmd/server/schemas/*_attributes.json`（迁移风险见 Risks），只改生成器行为并补测试。**

- [ ] **Step 1: 红测试**：schema 含 `"tags": {"type":"array","items":{"type":"string"}}` → 生成 `"valueType": "list", "items_type": "text"`；再生成（存量文件已有该属性）时 `valueType`/`items_type` 同步更新且 attributeID 保留。
- [ ] **Step 2: 实现**

```go
type attributeSpec struct {
	ValueType      string
	ItemsType      string
	RequiredPolicy string
}
```

数组分支（`:293-299`）：

```go
		case "string", "integer", "number", "boolean":
			attributes[path] = attributeSpec{
				ValueType:      "list",
				ItemsType:      getValueType(items),
				RequiredPolicy: policy,
			}
```

merge 既有属性（`:125` 附近）：

```go
			existingData["valueType"] = spec.ValueType
			if spec.ItemsType != "" {
				existingData["items_type"] = spec.ItemsType
			} else {
				delete(existingData, "items_type")
			}
```

新属性（`:139-142`）map 增加 `"items_type": spec.ItemsType`（仅当非空）。`writeAttributesMap` 的 otherKeys 通道（:210-214）已能序列化，无需改。

- [ ] **Step 3: 转绿** `go test ./cmd/tools -v -run GenerateAttributes`
- [ ] **Step 4: Commit** `feat(tools): #204 generator emits valueType list + items_type for primitive arrays`

---

### Task 8: validate-schema-consistency 感知 items 类型

**Files:**
- Modify: `cmd/tools/validate_schema_consistency.go`（值类型→存储列归类处，约 :217-229 与 :299-312；实施时先定位实际归类函数）
- Test: 对应测试文件追加

- [ ] **Step 1: 红测试**：`valueType: list, items_type: integer` 的属性归类为 numeric 列支撑（不误报 mismatch）；`items_type` 缺省的 list 归类为 text 列支撑。
- [ ] **Step 2: 实现**：归类 switch 中 list 分支改为按 `meta.EffectiveItemsType()` 递归/复用既有归类（形态：`case forma.ValueTypeList: return usesNumericColumn(meta.EffectiveItemsType())`，以实际函数签名为准）。
- [ ] **Step 3: 转绿 + Commit** `fix(tools): #204 schema consistency classifies list attrs by items_type`

---

### Task 9: 共享 fixture 增加 `tags` + wideParquetTypes

**Files:**
- Modify: `internal/e2e_harness/production/schemas/e2e_wide.json`（properties 增加 `"tags": {"type": "array", "items": {"type": "string"}}`）
- Modify: `internal/e2e_harness/production/schemas/e2e_wide_attributes.json`（追加 `"tags": { "attributeID": 18, "valueType": "list", "items_type": "text" }`；18 已核实空闲）
- Modify: `internal/e2e_harness/production/full_type_parquet_e2e_test.go:20-28`（`wideParquetTypes` 增加 `"tags": "VARCHAR[]"`）

- [ ] **Step 1:** 三处修改如上。
- [ ] **Step 2: 验证点**：确认无既有 e2e_wide 用例事件写 `tags`（`grep -rn '"tags"' internal/e2e_harness/production --include='*_test.go'` 仅 list 测试命中）；oracle `normalizeValue` 不会收到 list 值，无需加分支——若 grep 出新命中则补 oracle list case。
- [ ] **Step 3: 回归**：`go test -v ./internal/e2e_harness/production/... -tags=e2e -run TestFullTypeRoundTripAcrossTiers -timeout 30m`（穷尽列断言吃下新 `tags VARCHAR[]` 列即绿；需 Docker）。
- [ ] **Step 4: Commit** `test(e2e): #204 add tags list attr to shared e2e_wide fixture + wideParquetTypes`

---

### Task 10: e2e 规格改写 + 去 skip（最终验收门）

**Files:**
- Modify: `internal/e2e_harness/production/list_roundtrip_e2e_test.go`

- [ ] **Step 1: 改写**：
  - 删除 :48-102 的拒绝钉测（`TestListAttributeRoundTrip` 主体）与 :93-101 的 skip 包装，测试主体直接 `runListRoundTripSpec(ctx, t, cluster)`；
  - `runListRoundTripSpec` 内 `NewEnv(t, cluster, WithSchemaDir(writeListSchemaDir(t)))` 改为 `NewEnv(t, cluster)`（`tags` 已在共享 fixture）；
  - 删除 `writeListSchemaDir`/`injectJSON` 及相关 import；更新文件头 B2 注释（私有 schema 注入已退役，`tags` 常驻共享 fixture 且 `wideParquetTypes` 已扩展）。
- [ ] **Step 2: 验收（绿门）**

```bash
go test -v ./internal/e2e_harness/production/... -tags=e2e -run TestListAttributeRoundTrip -timeout 30m
```

预期：3 EAV 行（indices "0","1","2"）→ 热读 3 元素 → base+delta parquet 各 ≥3 值 → federated 2 记录各 3 元素、按 index 有序。
- [ ] **Step 3: Commit** `test(e2e): #204 un-skip list round-trip spec, retire private schema injection`

---

## 最终验证

```bash
make lint
make test                                                     # 全量单测
go test -v ./internal/e2e_harness/... -timeout=5m             # infra smoke
go test -v ./internal/e2e_harness/production/... -tags=e2e -run 'TestListAttributeRoundTrip|TestFullTypeRoundTripAcrossTiers' -timeout 30m
go test -v ./internal/e2e_harness/federated/... -tags=e2e -short -timeout 30m   # CI 同款联邦回归
```

## Risks

1. **生成器翻转的迁移风险（最高）**：再生成存量 schema 会把数组属性 `text→list`，parquet 列 `VARCHAR→VARCHAR[]`，与旧 parquet 文件 `union_by_name` 类型冲突。**本 PR 不再生成 `cmd/server/schemas/*_attributes.json`**；PR body 写明迁移注意（存量部署需重导出/换 S3 前缀后方可再生成），并开 follow-up issue 记录。
2. **DuckDB 语法可用性**：`list(x ORDER BY y) FILTER`、`(x,i)->` 双参 lambda、`flatten` 混合子列表——Task 3 Step 1 的 `TestDuckDBListSyntaxSupport` 前置钉死；lambda 不支持则统一换 `list_transform(range(1, len(col)+1), i -> …col[i]…)` 形态。
3. **struct 类型统一**：`flatten([...])` 内所有对象字段名/顺序/类型必须一致（value_text VARCHAR|NULL、value_numeric DOUBLE|NULL）；纯标量 schema 保持原路径（逐字节一致）限制暴露面，golden 测试防 churn。
4. **文件/函数规模红线**：`duckdb_schema_projection.go` 已 464 行，Task 4/5 新逻辑必须抽 helper（`eavElementCastExpr`、`eavJSONPart`）；若逼近 500 行，沿 #220 既有缝隙拆文件。
5. **行号漂移**：issue 里 `duckdb_schema_projection.go:328/:691`、`transformer.go:337-372` 均为陈旧锚点；本计划中的行号以 2026-07-18 main（4cf532e）为准，实施时以符号名定位。

## 范围外（明确不做）

- list 谓词/包含过滤（normalizer 已干净拒绝，维持现状）。
- 嵌套/多维数组的 parquet 往返（写路径显式拒绝，Task 1 守卫）。
- 存量 `cmd/server/schemas` 的再生成与数据迁移（follow-up issue）。
- `#274`（deleted_ts 不对称）等无关 follow-up。
