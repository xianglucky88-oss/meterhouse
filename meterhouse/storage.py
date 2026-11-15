"""事实表存储布局。

两张明细表在**两个引擎上使用同一份逻辑布局**：时间列、去重键、业务日
物化列、排序键、skip index、投影全部集中声明，避免"本地能跑、生产布局
不一致"。

为什么是 `biz_day` 而不是原始 UTC 时间做分区/排序键首列：

    ClickHouse 的 `toYYYYMM(toDateTime)` 分区依赖服务端时区，而"按本地日
    归属"是业务口径。把业务日作为 MATERIALIZED 列，它既参与分区裁剪，
    又参与排序键与 min-max 索引，查询端的日期过滤能直接命中。

为什么不做物化聚合：
    高基数维度组合不可穷举。任何"预聚合所有维度组合"的做法都会在维度
    增长后崩掉，并且把基数问题藏起来。这里只优化**明细扫描**的物理布局。
"""

from __future__ import annotations

from .sql.dialect import FactTableLayout


SUPPLIER_LAYOUT = FactTableLayout(
    name="supplier_stats",
    columns=[
        ("supplier_id", "string"),
        ("stat_time", "datetime"),
        ("is_active", "bool"),
        ("region", "string"),
        ("category", "string"),
        ("ingest_batch_id", "string"),
        ("row_version", "int64"),
    ],
    time_column="stat_time",
    biz_tz="Asia/Shanghai",
    key_columns=["supplier_id", "stat_time"],
    order_by=["biz_day", "supplier_id", "stat_time"],
    skip_index_columns=["region", "category"],
    projection_dimensions=[["region"], ["category"], ["region", "category"]],
)

ORDER_LAYOUT = FactTableLayout(
    name="order_facts",
    columns=[
        ("order_id", "string"),
        ("supplier_id", "string"),
        ("created_at", "datetime"),
        ("amount_cents", "int64"),
        ("status", "string"),
        ("region", "string"),
        ("ingest_batch_id", "string"),
        ("row_version", "int64"),
    ],
    time_column="created_at",
    biz_tz="Asia/Shanghai",
    key_columns=["order_id"],
    order_by=["biz_day", "order_id", "row_version"],
    skip_index_columns=["region", "status", "supplier_id"],
    projection_dimensions=[["region"], ["status"], ["supplier_id"]],
)

LAYOUTS: dict[str, FactTableLayout] = {
    SUPPLIER_LAYOUT.name: SUPPLIER_LAYOUT,
    ORDER_LAYOUT.name: ORDER_LAYOUT,
}

# 摄入台账：每个批次落在哪张表、哪段版本号区间、多少行。
# 这是"原始事实可追溯"的实现：任何一次指标计算都能反查到它读过的批次。
INGEST_LEDGER = "ingest_batches"
