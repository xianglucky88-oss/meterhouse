# 指标语义层设计说明

## 目标

让"同一个指标"在概览、趋势、区域报表、导出、API 中**只有一个数字**。

## 为什么需要语义层

当前状态是：

```
概览页 ──┐
趋势页 ──┼── 三份手写 SQL，各自算一遍 ──> 三个数字
区域报表 ┘
```

问题不是"某一份写错了"，而是**没有唯一的定义**。
只要新增一个页面，就多一份 SQL，口径就多一个漂移点。

语义层要解决的是：

```
指标定义（YAML）
      │  编译
      ▼
   一条 SQL
      │  被所有页面复用
      ▼
   一个数字
```

## 指标定义的组成

一个指标需要显式声明五件事：

1. **名称**：唯一标识。同一个业务概念只能有一个名字。
2. **类型**：
   - `simple`：可加聚合（sum、count）
   - `distinct`：去重计数（count distinct）
   - `ratio`：分子/分母
3. **来源与表达式**：从哪张表、算什么。
4. **过滤器**：例如 `status = 'paid'`、`is_active`。
5. **可加性**：决定上卷时能不能直接相加。

### 可加性（这是最容易写错的地方）

| 类型 | 含义 | 时间上卷 | 维度上卷 |
|---|---|---|---|
| `additive` | 可直接相加 | 可以 | 可以 |
| `semi_additive` | 时间上不可加，维度上可加 | 不可以 | 可以 |
| `non_additive` | 都不可加 | 不可以 | 不可以 |

`active_suppliers` 是 `non_additive`：1 月的 80 个和 2 月的 75 个，
季度不是 155 个。要算季度必须重新去重。

`gmv_cents` 是 `additive`（在去重之后）：可以跨天、跨区域相加。

`semantic.compile_grouped("gmv_cents", ["region"])` 目前能跑通，
分组求和的总额与 `ground_truth` 之间存在差额，原因待查。

## 时间粒度

语义层需要支持 `day / week / month / quarter / year`，且必须显式指定时区：

```yaml
time:
  column: created_at
  storage_tz: UTC
  biz_tz: Asia/Shanghai
```

否则就会出现"UTC 口径 12 单 / 本地口径 50 单"这种事。

## 同环比

同环比不能简单地"用两个 SQL 的结果相减"，需要考虑：

- 对比期是否有数据（缺失时是 0 还是 N/A）；
- `non_additive` 指标的对比期必须独立计算，不能由更细粒度相加得到；
- 时区与夏令时（如果业务跨时区）。

## 语义层要暴露的能力

```
GET /api/v1/metrics/{name}?dimensions=region&granularity=month&compare=prev_period
```

响应里应包含：

- `value`：当前期
- `prev`：对比期
- `delta_pct`：变化率
- `sql`：实际执行的 SQL（便于审计）
- `definition_version`：指标定义版本（口径变了要能追溯）

## 与 ClickHouse 的关系

生产用 ClickHouse。语义层编译出的 SQL 需要通过方言适配层：

| 语义 | DuckDB | ClickHouse |
|---|---|---|
| 日期截断 | `date_trunc('month', ts)` | `toStartOfMonth(ts)` |
| 时区转换 | `ts + INTERVAL 8 HOUR` | `toTimeZone(ts, 'Asia/Shanghai')` |
| 条件聚合 | `count(*) FILTER (WHERE ...)` | `countIf(...)` |
| 去重计数 | `count(DISTINCT x)` | `uniqExact(x)` |

`sql/dialect.py` 是适配层的位置，当前只有 DuckDB 实现。

## 待决策

1. "活跃供应商数"到底定义成"窗口内去重数"还是"日均值"？两者需要两个指标名。
2. 缺失对比期数据时返回 0 还是 null。
3. 指标定义版本化：口径变更时，历史报表要不要按旧版本重算。
4. 语义层是否允许业务方自定义维度组合（灵活 vs 口径可控的权衡）。
