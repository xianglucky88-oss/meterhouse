"""已废弃的手写 SQL。

历史上三个接口各写一段 SQL，同一个指标有三份互相矛盾的实现。
迁移后所有取数都走 `metrics/metrics.yaml` 语义层，本模块不再承载口径。

保留这个文件只有一个目的：让仍在 `from meterhouse.queries import QUERIES`
的外部脚本不至于 ImportError。它现在的每条 SQL 都由语义层编译而来，
因此**不会再产生新口径**——如果它和语义层不一致，说明编译器坏了，
而不是"第二个真相"。

新代码请直接用 `MetricStore.compile_query()`。
"""

from __future__ import annotations

from pathlib import Path

from .semantic import MetricStore, TimeWindow

_STORE = MetricStore.load(Path("metrics/metrics.yaml"))


def _sql(name: str, **kwargs) -> str:
    return _STORE.compile_query(name, engine="duckdb", **kwargs).sql


# 旧名字 → 语义层指标（均无维度、无时间窗口）
QUERIES = {
    "active_suppliers_a": _sql("active_suppliers"),
    "active_suppliers_b": _sql("active_suppliers_daily_avg"),
    "active_suppliers_c": _sql("active_suppliers", dimensions=["region"]),
    "gmv_cents": _sql("gmv_cents"),
    "orders_total": _sql("orders_total"),
    "orders_by_date": _sql("orders_total", time_window=TimeWindow(granularity="day")),
    "supplier_count_by_region": _sql("suppliers_total", dimensions=["region"]),
    "conversion_rate": _sql("paid_rate"),
    "avg_order_cents": _sql("avg_order_cents"),
}
