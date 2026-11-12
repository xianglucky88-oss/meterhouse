# meterhouse

内部经营看板。6 个指标、2 张明细表、3 个页面、1 个每日导出脚本。

**核心问题：同一个「活跃供应商数」，三个接口给出三个数字。**

## 快速开始

```bash
pip install -r requirements.txt

python -m meterhouse seed          # 生成示例数据（80 供应商 / 6000 订单 / 60 天）
python -m meterhouse verify        # 口径一致性断言
uvicorn meterhouse.api:app --port 9400
open http://localhost:9400
make demo                          # 固定演示序列
```

本地用 DuckDB（秒级），生产是 ClickHouse。两者 SQL 由 `sql/dialect.py` 适配
（当前只有一套 DuckDB 方言，ClickHouse 适配是待办）。

## 结构

```
meterhouse/
    data.py       建表、数据生成、权威口径（ground_truth）
    queries.py    遗留 SQL —— 三个接口各自的手写查询
    semantic.py   指标语义层骨架（未完成）
    api.py        FastAPI：3 个页面接口 + 语义层查询 + 口径对比
metrics/
    metrics.yaml  指标定义（草稿，只用来 2 个指标）
data/
    示例数据（DuckDB）
tests/
scripts/
    verify_metrics.py  口径断言
    demo.sh            固定演示
```

## 口径对比

```bash
curl -s localhost:9400/api/v1/parity | python3 -m json.tool
```

实测（seed 20261112）：

| 口径 | 实现 | 数值 |
|---|---|---|
| A（概览页） | 按 `supplier_id` 跨期去重 | **80** |
| B（趋势页） | 按 UTC 日期去重后求平均 | **61.75** |
| C（区域报表） | 按 `supplier_id\|date` 拼键去重 | **3767** |
| 权威 | — | **80** |

极差 **98.4%**。A 和 B 根本不是同一个指标（"跨期去重"vs"日均值"），
C 则把供应商在四个区域各算一遍。

其他指标与权威口径的对比：

| 指标 | 接口值 | 权威值 | 倍数 |
|---|---|---|---|
| GMV | 468,199,201 | 458,468,062 | 1.0212 |
| 订单数 | 6,125 | 6,000 | 1.0208 |
| 客单价 | 102,070.90 | 76,411.34 | 1.3358 |

三个指标的偏差比例各不相同：分子与分母受重复行影响的方式并不一致。

## 时区

`supplier_stats.stat_time` 是 UTC。同一天 10-31：
UTC 口径 **12 单**，本地口径 **50 单**。差 4 倍多。

## 接口

```
GET  /api/v1/dashboard/summary    概览（口径 A）
GET  /api/v1/dashboard/trend      趋势（口径 B）
GET  /api/v1/reports/region       区域报表（口径 C）
GET  /api/v1/metrics              语义层指标列表
GET  /api/v1/metrics/{name}       通过语义层查询
GET  /api/v1/parity               三份口径并排 + 极差
GET  /api/v1/ground-truth         权威口径
GET  /api/v1/slow                 慢查询现场
POST /api/v1/admin/seed           重建数据
```

## 测试

```bash
python -m pytest -q
```

19 个用例覆盖：生成器确定性、重复推送、深夜订单、三份口径互不相同、
A 与权威一致、B 是日均值、C 跨区域重复计入、GMV/订单数/客单价超算、
时区归属、语义层加载/编译/拒绝非法维度/可加性声明。
