# Federated Query Benchmark 高层设计

更新时间：2026-04-17  
适用仓库：`forma`

## 1. 背景

Forma 当前已经具备联邦查询、冷热分层、CDC flush、compaction、基础分页与性能测试能力，但还缺少一个专门面向混合查询的系统性 benchmark。现有性能测试更偏向通用回归，尚未覆盖以下关键问题：

- 基于真实业务模型的冷热联合查询负载
- 多种数据分布对谓词下推和路由策略的影响
- 深分页与大页码跳转的性能退化
- 冷热重叠、软删除、去重和最近版本覆盖的正确性与成本

本设计提出一个参考 TPC-E 数据特征、但适配 Forma 存储模型的 Federated Query Benchmark，用于评估 cold + hot 联邦查询路径的正确性、性能和可观测性。

## 2. 目标

- 为 Forma 提供可重复、可扩展、可对比的混合查询 benchmark。
- 使用 TPC-E 风格的金融交易域数据，覆盖交易、客户、证券三类核心实体。
- 同时覆盖热字段列绑定和长尾 EAV 字段查询。
- 系统测试不同数据分布下的分页、过滤、排序、深分页和冷热 merge 行为。
- 为后续优化提供统一指标，包括延迟分位数、吞吐、冷热命中比例、去重比例和谓词下推效率。

## 3. 非目标

- 不追求 TPC-E 官方规范兼容，不实现完整的 33 张表和 12 类事务。
- 不作为官方 TPC-E 成绩发布工具，不涉及审计规则。
- 不替代现有 `internal/e2e_harness/federated` 功能测试，只在其基础上扩展 benchmark 能力。
- 第一阶段不实现分布式执行、不实现跨机器 driver，不以峰值吞吐为首要目标。

## 4. 设计原则

- 与现有 e2e harness 对齐，优先复用已有容器化测试基础设施。
- 数据模型参考 TPC-E 业务语义，但要贴合 Forma 的 `entity_main + eav_data + change_log + parquet` 结构。
- Benchmark 结果必须可复现，同一随机种子下应生成同一批数据。
- 工作负载要同时覆盖 correctness 与 performance，而不是只做压测。
- 深分页单独建模，明确区分传统 `LIMIT/OFFSET` 与未来可能的 keyset/two-phase 方案。

## 5. 逻辑架构

Benchmark 系统由六个逻辑层组成：

1. Schema Mapping Layer
2. Data Generation Layer
3. Tier Preparation Layer
4. Workload Definition Layer
5. Benchmark Runner Layer
6. Metrics and Report Layer

### 5.1 Schema Mapping Layer

该层负责把 TPC-E 风格实体映射到 Forma schema。

首批引入三个 benchmark schema：

- `trade`
- `customer`
- `security`

其中：

- 高频过滤和排序字段映射到 hot columns
- 长尾属性保留在 EAV
- 每个 schema 都要包含可过滤字段、可排序字段、以及只存在于 EAV 的字段

建议的 `trade` schema：

- Hot: `symbol`, `trade_type`, `quantity`, `price`, `trade_time`, `customer_id`
- EAV: `exchange`, `commission`, `is_cash`, `broker_id`, `order_channel`

这样可以覆盖：

- 纯 hot pushdown
- 纯 EAV filter
- hot + EAV 混合条件
- 基于时间的排序与分页

### 5.2 Data Generation Layer

该层生成 benchmark 数据，要求同时支持规模、分布和冷热重叠配置。

支持的分布类型：

- `uniform`：均匀分布，作为基线
- `zipf`：热点客户、热点 symbol、热点 region
- `temporal`：越新的数据越密集，模拟真实写入热度
- `partition-skew`：不同 region 或 sector 严重不均衡
- `hotspot-overlap`：固定比例 row_id 在 cold 和 hot 同时存在

生成器输出的逻辑数据需满足：

- 同一 `row_id` 可跨 tier 出现多个版本
- 更新时间单调递增，满足 last-write-wins 检验
- 部分记录带软删除标记，用于测试 anti-join 与删除覆盖
- 可精确控制 selectivity，以支持高、中、低选择性 workload

### 5.3 Tier Preparation Layer

该层负责把生成好的记录分配到三个 tier：

- Cold/Base: 历史 parquet，大文件，低更新
- Warm/Delta: 最近 flush 的 parquet，小文件，较新
- Hot: Postgres `entity_main + eav_data + change_log` 中的未 flush 数据

默认 tier 配比：

- Base: 60%
- Delta: 30%
- Hot: 10%

但 benchmark 需要允许覆盖以下变化：

- 高热度模式：40/20/40
- 长尾历史模式：85/10/5
- 高重叠模式：Base 与 Hot 有 5%-10% 主键重叠

Tier Preparation 还需要负责：

- 把 cold/warm 记录写入 base/delta parquet
- 把 hot 记录写入 Postgres 三张表
- 标记 overlap、delete、update、restore 等特殊样本

### 5.4 Workload Definition Layer

该层定义 benchmark 查询模板。首期工作负载分为五类。

#### A. 基线分页

- 无过滤，按 `trade_time DESC`
- 高选择性过滤 + 分页
- 低选择性过滤 + 分页
- EAV 过滤 + 分页
- hot + EAV 组合条件 + 分页

#### B. 深分页

- `page = 1`
- `page = 100`
- `page = 1,000`
- `page = 100,000`

要求固定 page size，例如 `20` 或 `50`，并明确记录 offset 规模。

#### C. 冷热命中模式

- 只命中 hot
- 只命中 cold
- 同时命中 cold + hot
- 同时命中 cold + warm + hot

#### D. 去重与覆盖

- 冷热同 key，不同版本
- 热 tier 删除冷 tier 中已有记录
- 冷记录被热记录更新部分 EAV 属性

#### E. 路由与策略对比

- `PreferHot = true`
- `RoutingStrategyHybrid`
- 小结果集走 Postgres
- 大结果集走 DuckDB

### 5.5 Benchmark Runner Layer

Runner 负责 benchmark 生命周期管理：

1. 注册 schema
2. 生成数据
3. 写入 tier
4. 预热
5. 重复执行 workload
6. 汇总 latency 与 correctness 指标
7. 输出结构化报告

Runner 需要支持：

- 串行运行
- 固定并发运行
- 指定 workload 子集运行
- 指定 scale factor 和 distribution 运行
- 指定随机种子运行

推荐 scale：

- `small`: 100K
- `medium`: 1M
- `large`: 10M

### 5.6 Metrics and Report Layer

需要输出以下指标：

- `p50`, `p95`, `p99`, `max`, `avg`
- `qps`
- `result_count`
- `hot_hit_ratio`
- `cold_hit_ratio`
- `dedup_count`
- `delete_filtered_count`
- `pushdown_efficiency`
- `memory_peak_mb`

报告格式：

- 控制台摘要
- JSON 结果文件
- Markdown 报告

这样既可用于本地分析，也可作为 CI artifact 或回归基线。

## 6. 数据模型设计

### 6.1 TPC-E 风格实体

虽然不完全复刻 TPC-E，但借鉴其金融交易域特征：

- `trade`: 高频写入，高时间局部性
- `customer`: 中频更新，适合做过滤维度
- `security`: 低频变更，适合作为冷数据维表

### 6.2 Hot/EAV 设计要求

每个 schema 至少满足：

- 2 个可排序 hot 字段
- 2 个高选择性 hot 过滤字段
- 1 个低选择性 hot 过滤字段
- 2 个 EAV 字段
- 1 个会参与冷热覆盖冲突的字段

## 7. 分页设计

深分页是本 benchmark 的重点。

### 7.1 当前基线

使用现有分页语义：

- `LIMIT page_size OFFSET offset`

这可以真实测出当前系统在 merged result 上的性能退化。

### 7.2 深分页关注点

需要重点观察：

- offset 增大后，DuckDB 读取和排序成本是否线性或超线性上升
- 热数据和冷数据 merge 后再分页的成本
- 小结果集策略是否会误走 DuckDB
- 大页码跳转时总数统计是否变成瓶颈

### 7.3 大页码测试点

建议至少固定以下 case：

- `page_size=20, page=1`
- `page_size=20, page=100`
- `page_size=20, page=1,000`
- `page_size=20, page=100,000`

同时保留等价 keyset case，作为未来优化对照组，但第一阶段可以只输出设计接口，不强制实现替代算法。

## 8. 正确性要求

Benchmark 不仅测性能，还要校验语义正确。

必须校验：

- cold/warm/hot 合并后记录数是否正确
- 同一 `row_id` 的最新版本是否胜出
- 软删除是否能遮蔽冷层旧版本
- EAV 属性在跨 tier 合并时是否按更新时间选中正确版本
- 分页结果是否稳定、有序、无重复

## 9. 可观测性要求

Runner 应记录每次 workload 的关键上下文：

- query name
- distribution
- scale
- page size
- offset/page number
- returned rows
- source tier mix
- duration
- error

如果执行计划可获取，还应记录：

- 是否走 DuckDB
- 是否 PreferHot
- 每个 source 的返回行数
- merge 耗时

## 10. 与现有系统的集成

优先复用现有以下能力：

- `internal/e2e_harness/federated/fixtures.go`
- `internal/e2e_harness/federated/seeding.go`
- `internal/e2e_harness/federated/query.go`
- `internal/e2e_harness/federated/performance_test.go`
- `internal/federated_pagination.go`
- `internal/federated_merge.go`
- `internal/federated_routing.go`

新增 benchmark 不应复制一套完全独立的测试系统，而应在现有 harness 上增量扩展。

## 11. 风险与取舍

### 11.1 风险

- 现有测试查询模型较简化，可能不足以表达复杂 hybrid 过滤
- 大规模数据写入会拉长 benchmark 初始化时间
- 深分页在当前实现下可能需要全量 merge，large scale 成本很高
- 不同分布下的 selectivity 难以精确命中，需要生成器提供可校准参数

### 11.2 取舍

- 第一阶段优先做可复现、可落地的 benchmark，不先做复杂优化器仿真
- 第一阶段使用已有 federated query 能力，不强推新的查询 DSL
- 第一阶段接受 `medium` 作为主回归规模，`large` 作为单独性能实验规模

## 12. 里程碑

建议按四个里程碑推进：

1. 定义 schema、分布模型和 workload
2. 实现数据生成、tier 装载和 benchmark runner
3. 补齐深分页、冷热重叠、正确性校验和报告输出
4. 建立回归基线并接入 CI 或人工性能回归流程

## 13. 交付物

第一阶段交付物包括：

- benchmark HLD 文档
- benchmark implementation plan
- benchmark issue backlog
- benchmark scaffolding 代码
- 至少一组 `small` 和 `medium` 数据集结果样例
- 深分页专项报告
