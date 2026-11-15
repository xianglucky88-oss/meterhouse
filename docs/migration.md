# 查询执行迁移：DuckDB → ClickHouse

## 结论速览

| 维度 | 迁移前 | 迁移后 |
|---|---|---|
| 取数入口 | 3 个接口各写一段 SQL + 语义层只覆盖 2 个指标 | 7 个指标全部由 `metrics/metrics.yaml` 编译 |
| 口径 | 同一指标 3 个数字（A=80 / B=61.75 / C=3767） | 一个指标一个定义；`verify` 全绿 |
| 高基数 | 运行期对大表做窗口去重 + 全量扫描 | 明细按 `biz_day` 分区/排序，查询期去重，无预聚合 |
| 引擎故障 | 视为"查询返回空" | 显式 503 或降级回退，绝不产生占位指标 |
| 切流 | 全局开关，无法逐接口灰度 | 逐接口 `off/shadow/primary`，带审计与差异记录 |

迁移的**目标引擎是 ClickHouse**（仓库原有设计文档即如此），DuckDB 保留为
本地开发引擎与迁移期的 `control`。

## 1. 为什么不是"把 SQL 改成 ClickHouse 语法"

原实现的问题不在方言，而在**没有唯一定义**。直接把三段手写 SQL 翻成
ClickHouse 语法，只会把"三个数字"平移过去，并且把去重口径错误固化到新库。

因此迁移的第一步是把口径收敛到语义层：

- 指标用**结构化原语**声明（`agg` + `column` + `filter` + `dedup_by`），
  而不是 `expr` 字符串；
- 同一份定义编译到两种方言（`meterhouse/sql/dialect.py`）；
- 口径一致性由构造保证，而不是靠人肉比对两段 SQL。

`verify` 会把每个指标分别放到两种引擎上跑，并与 `ground_truth` 对照。

## 2. 存储布局

见 `meterhouse/storage.py`，两种引擎共用同一份 `FactTableLayout`。

关键决定：

1. **`biz_day` 物化列**：UTC 存储时间 → 业务时区日期，作为
   ClickHouse 的 `PARTITION BY toYYYYMM(biz_day)` 与 `ORDER BY` 首列。
   这样"按本地日期过滤"能直接裁分区，而不是先全表扫再过滤。
   DuckDB 侧对应一个生成列 + 索引。
2. **排序键含主键与版本**：`ORDER BY (biz_day, order_id, row_version)`，
   查询期的 `LIMIT 1 BY order_id` 能按排序顺序流式去重，不必全量排序。
3. **skip index**：高基数维度（region/status/supplier_id）加 bloom filter。
4. **投影（projection）**：为 `region` / `status` / `supplier_id` 各建一份
   排序副本，换维度下钻时避免运行期大排序。投影**只改物理顺序，不做聚合**。

### 明确不做的事

- **不建物化聚合表**。高基数维度的组合不可穷举；预聚合所有组合会在维度
  增长后崩掉，且掩盖基数问题。查询始终从去重后的明细算起。
- **不把 `non_additive` 指标相加**。`active_suppliers` / `orders_total` /
  `paid_rate` 在目标粒度**重新去重**；跨月相加是错的。

### 实测（3,000,000 行明细，700 天，50,000 个供应商）

| 查询 | 结果行数 | 耗时 |
|---|---|---|
| 单月窗口 GMV（分区裁剪） | 1 | **9 ms** |
| 单月窗口 GMV 按 region | 80 | **10 ms** |
| 700 天全窗口 GMV | 1 | 151 ms |
| 700 天按月粒度订单数 | 24 | 245 ms |
| 50,000 供应商维度 GMV（全窗口） | 50,000 | 366 ms |

分区裁剪可从 `EXPLAIN indexes=1` 验证：24 个分区中只读 **3 个 part**，
372 个 granule 中只读 **17 个**。

## 3. 事实可追溯性

原始事实**逐行保留**，不因迁移被折叠：

- 每行带 `ingest_batch_id`（落在哪一批）与 `row_version`（同主键内序号）；
- `ingest_batches` 台账记录每批的 `table / rows_loaded / min_version /
  max_version / loaded_at`；
- 两引擎灌的是同一份 `generate_rows()` 输出，`check-parity` 逐行校验和
  行数一致（`sum(row_version)` / `sum(amount_cents)` 等）。

查询响应里带 `lineage`：来源表、去重键、版本列、时间窗口、定义版本。

## 4. 逐接口切流

配置见 `metrics/routes.yaml`，三个角色：

- `control`：当前可信引擎（默认 DuckDB），迁移期始终可回退；
- `target`：迁移目标（默认 ClickHouse）；
- `mode`：
  - `off`：只走 control；
  - `shadow`：仍由 control 服务，同时把同一查询发到 target 并**比对结果**，
    记录差异（`match` / `mismatch` / `shadow_error`），不改变响应数字；
  - `primary`：由 target 服务；故障时按 `fallback` 回退 control，并标记
    `degraded=true` + `fallback_from`。

变更接口：`POST /api/v1/routes/{interface}`（需 `X-Admin-Token`，未配置时
仅允许本机演示且响应标 `local-unauthenticated`）或
`python -m meterhouse cutover`。每次变更写 `metrics/routes.audit.jsonl`。

**推荐顺序**：`off` → `shadow`（观察差异）→ `primary`（保留 fallback）→
`primary`（关闭 fallback）。

### 差异识别

`shadow` 模式下路由层逐行对齐两侧结果：维度/时间键必须完全一致，
数值列按相对误差比较（`tolerance_pct` 可配）。差异写入
`/api/v1/status` 的 `recent_comparisons` 与 `comparison_counts`。

## 5. 故障隔离与容量

| 场景 | 行为 |
|---|---|
| target 不可用 + `fallback=true` | 200，`degraded=true`，`fallback_from` |
| target 不可用 + `fallback=false` | **503**，响应体不含任何指标值 |
| control 不可用 | **503**（不会被 target 的结果掩盖） |
| 结果行数超上限 | `CapacityExceeded` → 503，不返回截断结果 |
| 并发超上限 | `CapacityExceeded`（有界排队，不无限阻塞） |
| 连续失败 | 熔断打开，后续请求快速失败，不再逐请求等连接超时 |

异常分类见 `meterhouse/errors.py`；引擎翻译见 `meterhouse/engine`。
**没有任何路径会把引擎故障折算成 0 / null 指标。**

## 6. 容量回退（capacity fallback）

除故障回退，接口还有**容量回退**：`shadow`/`primary` 下 target 返回
`CapacityExceeded`（结果太大 / 并发打满）时同样按 `fallback` 回退 control。
这与"引擎宕机"共用一条路径，区别只在 `failure_reason`。

## 7. 仍未解决 / 后续

1. **对比期缺失**：语义层已有时间窗口与粒度，但同环比（`compare=prev_period`）
   尚未实现；`non_additive` 指标的对比期必须独立编译，加一个即可。
2. **指标定义版本化的历史重算**：响应带 `definition_version`，但历史报表
   按旧版本重算的策略未落地。
3. **行级摄取工具**：现在同步是"整表重建 / 追加"。生产需要按 `ingest_batch_id`
   幂等重放，以及 upsert（ReplacingMergeTree）在大表上的 `FINAL` 代价评估。
4. **DuckDB 只读并发**：本地引擎与写连接共享同一文件锁，API 进程内不能同时
   写；生产应改用只读副本或独立进程。
